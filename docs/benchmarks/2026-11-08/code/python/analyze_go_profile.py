"""Classify sampled Go CPU stacks without counting a sample more than once."""
from collections import defaultdict
import re
import subprocess


def analyze(tool, path):
    raw = subprocess.check_output([tool, '-raw', str(path)], text=True)
    samples, locations = [], {}
    section = None
    current = None
    for line in raw.splitlines():
        if line == 'Samples:': section = 'samples'; continue
        if line == 'Locations': section = 'locations'; continue
        if line == 'Mappings': break
        if section == 'samples':
            match = re.match(r'^\s*(\d+)\s+(\d+):\s+([\d ]+)\s*$', line)
            if match: samples.append((int(match[2]), match[3].split()))
        if section == 'locations':
            match = re.match(r'^\s*(\d+):\s+(.*)', line)
            if match:
                current = match[1]; locations[current] = match[2]
            elif current:
                locations[current] += '\n' + line
    assert samples and locations, 'Unrecognized pprof raw format'
    def owner(stack):
        if 'main.(*reportOutput).' in stack or 'main.(*rowWriter).' in stack or 'main.newReportOutput' in stack:
            return 'output'
        if 'main.loadMaster' in stack: return 'setup'
        if ('/parquet/' in stack or 'encoding/csv.(*Reader)' in stack
                or 'strconv.ParseFloat' in stack or 'strconv.atof' in stack or 'strconv.readFloat' in stack):
            return 'decode'
        if 'main.(*stream).next' in stack: return 'row_handling'
        if 'main.(*period).' in stack: return 'calculate'
        return 'other'
    exclusive, inclusive, allocation_owner = defaultdict(int), defaultdict(int), defaultdict(int)
    for nanos, ids in samples:
        stack = '\n'.join(locations[i] for i in ids)
        phase = owner(stack)
        inclusive[phase] += nanos
        if any(name in stack for name in ['runtime.gc', 'runtime.scanObject', 'runtime.scanobject', 'runtime.(*gcWork)', 'runtime.sweepone']):
            category = 'gc'
        elif any(name in stack for name in ['runtime.mallocgc', 'runtime.newobject', 'runtime.growslice', 'runtime.makeslice', 'runtime.profilealloc']):
            category = 'allocation'; allocation_owner[phase] += nanos
        elif any(name in stack for name in ['syscall.Read', 'syscall.read', 'syscall.Pread', 'syscall.pread', 'os.(*File).Read', 'internal/poll.(*FD).Read']):
            category = 'file_read'
        elif any(name in stack for name in ['runtime.madvise', 'runtime.bgscavenge', 'runtime.sysUnused']):
            category = 'heap_maintenance'
        elif any(name in stack for name in ['runtime.pthread_cond_', 'runtime.pthread_kill', 'runtime.usleep', 'runtime.findRunnable', 'runtime.schedule', 'runtime.semasleep']):
            category = 'runtime_support'
        else:
            category = phase
        exclusive[category] += nanos
    assert sum(exclusive.values()) == sum(n for n, _ in samples)
    return {'sampled_cpu_seconds': sum(exclusive.values()) / 1e9,
            'exclusive_cpu_seconds': {k: v/1e9 for k, v in sorted(exclusive.items())},
            'phase_cpu_including_allocation_and_gc_assist': {k: v/1e9 for k, v in sorted(inclusive.items())},
            'allocation_cpu_by_calling_phase': {k: v/1e9 for k, v in sorted(allocation_owner.items())},
            'method': '10ms CPU samples; GC first, allocation second, file reads third, then heap/runtime support or caller phase; each sample counted once. These are CPU estimates, not elapsed stage durations.'}
