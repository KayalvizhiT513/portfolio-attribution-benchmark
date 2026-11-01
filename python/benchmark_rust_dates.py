"""Isolate the Rust date-cache change; preserve original benchmark evidence."""
import argparse
import csv
import hashlib
import json
import math
import statistics
from pathlib import Path

from benchmark_native import measured
from test_attribution import compare_files

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'results/rust-date-cache'
BASELINE = ROOT / 'results/csv-parquet-native-output'
REPORT = ROOT / 'RUST_DATE_READ_REPORT.md'
SCENARIOS = [('country', 2190, 3), ('security', 318000, 1), ('weekly', 316, 1),
             ('monthly', 72, 1), ('subset', 602, 1)]


def save(runs, checks, environment):
    for name, value in [('runs', runs), ('checks', checks), ('environment', environment)]:
        (OUT / f'{name}.json').write_text(json.dumps(value, indent=2) + '\n')
    if runs:
        with (OUT / 'runs.csv').open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(runs[0])); writer.writeheader(); writer.writerows(runs)
    def samples(scenario, fmt):
        return [r for r in runs if r['scenario'] == scenario and r['format'] == fmt]
    def median(scenario, fmt, field):
        values = [float(r[field]) for r in samples(scenario, fmt)]
        return statistics.median(values) if values else None
    def number(value): return '—' if value is None else f'{value:.3f}'
    lines = ['# Rust: remove repeated Parquet date formatting', '',
             f'Completed {len(runs)}/14 full-history runs. Only the Rust input date adapter changed; calculation, native output, batch size and worker settings are unchanged.', '',
             '## Fresh CSV vs Parquet', '',
             'Times below are medians. Country has three runs per format; other scenarios have one. Each run processes all 1,095 dates and 175,200,000 return records.', '',
             '| Scenario | Runs per format | CSV read s¹ | Parquet read s¹ | CSV whole run s | Parquet whole run s |',
             '|---|---:|---:|---:|---:|---:|']
    for scenario, _, repetitions in [('security',318000,1), ('country',2190,3), ('weekly',316,1), ('monthly',72,1), ('subset',602,1)]:
        values = [number(median(scenario,fmt,field)) for field in ['read_parse_seconds','process_wall_seconds'] for fmt in ['csv','parquet']]
        lines.append(f'| {scenario.capitalize()} | {repetitions} | ' + ' | '.join(values) + ' |')
    lines += ['', '¹ Read time adds time from the index and portfolio workers. The workers can overlap, so read time can exceed whole-run time.', '',
              '## Country: check the remaining slowdown', '',
              'Country pairs alternate CSV→Parquet, Parquet→CSV, CSV→Parquet. All runs use four configured threads and normal filesystem caching. The trigger for adding deeper timers is Parquet median read time or whole-run time at least 10% above CSV after three pairs. This is a practical threshold, not a statistical significance test.', '']
    if len(samples('country','csv')) == len(samples('country','parquet')) == 3:
        read_ratio=median('country','parquet','read_parse_seconds')/median('country','csv','read_parse_seconds')
        wall_ratio=median('country','parquet','process_wall_seconds')/median('country','csv','process_wall_seconds')
        followup = 'The slowdown remains above the trigger; separate decoding and adapter measurements are shown below.' if (OUT/'profile/runs.json').exists() else 'The slowdown remains above the trigger; add separate decoding and adapter timers.'
        lines += [f'Parquet/CSV ratio: read **{read_ratio:.3f}×**; whole run **{wall_ratio:.3f}×**. Below 1 means Parquet is faster.', '',
                  '**Follow-up:** ' + (followup if max(read_ratio,wall_ratio)>=1.1 else 'The slowdown is below the trigger; separate decoding/adapter timers are not required for this result.'), '']
        csv_wall=[r['process_wall_seconds'] for r in samples('country','csv')]
        pq_wall=[r['process_wall_seconds'] for r in samples('country','parquet')]
        lines += [f'Whole-run ranges: CSV {min(csv_wall):.3f}–{max(csv_wall):.3f}s; Parquet {min(pq_wall):.3f}–{max(pq_wall):.3f}s.', '']
    lines += ['## What changed', '',
              'Before: every Parquet return record converted its date32 value into a new date string. Now: compare the integer date and keep the formatted string until the date changes. The cache survives batch boundaries. CSV and Parquet string-date handling are unchanged.', '',
              'For these date-sorted files, date formatting falls from roughly 175.2 million calls to 2,190 per run: 1,095 dates in each of two return streams. This count follows from the input layout and cache logic; the unit test verifies cache reuse.', '',
              f'Output checks passed: **{len(checks)}**. Every output field is checked against the original Rust result and the unchanged Go result; fresh CSV and Parquet outputs are also compared.', '',
              '[Raw measurements](results/rust-date-cache/runs.csv) · [Checks](results/rust-date-cache/checks.json) · [Settings and binary hash](results/rust-date-cache/environment.json) · [Original baseline](CSV_PARQUET_REPORT.md)', '']
    profile_path = OUT / 'profile/runs.json'
    if profile_path.exists():
        profiled = json.loads(profile_path.read_text())
        lines += ['## Where the remaining Parquet read time goes', '',
                  f'Profiling runs completed: {len(profiled)}/7. Profiling output comparisons passed: {len(json.loads((OUT / "profile/checks.json").read_text()))}/14.', '',
                  'Optional timers are enabled only with `ATTRIBUTION_PROFILE_READ=1`. One clock surrounds each batch fetch; the rest of the read stage covers typed Arrow access, date caching, ID lookup, validation, and building daily buffers. No clocks are added to individual rows.', '',
                  '**Batch read/decode includes file I/O, decompression, decoding and Arrow batch creation. It does not isolate decompression from disk I/O.** The daily-buffer figure is the read-stage time minus batch time: it includes loop overhead, typed access, lookup/validation and copying values. Both columns are combined worker time, not process wall time.', '',
                  '| Scenario | Batch read + decode s | Arrow → daily buffers s | Total read s |', '|---|---:|---:|---:|']
        for scenario in ['security','country','weekly','monthly','subset']:
            candidates = sorted([r for r in profiled if r['scenario']==scenario], key=lambda r:float(r['read_parse_seconds']))
            if not candidates: continue
            r = candidates[len(candidates)//2]
            lines.append(f"| {scenario.capitalize()} | {float(r['parquet_batch_read_decode_seconds']):.3f} | {float(r['parquet_to_daily_buffer_seconds']):.3f} | {float(r['read_parse_seconds']):.3f} |")
        lines += ['', 'Country uses the profiling run with the middle read time out of three; the other scenarios use one run. Each row’s components come from the same run and sum to its read time. Profiled times are kept separate from the uninstrumented comparison above.', '',
                  '[Profile measurements](results/rust-date-cache/profile/runs.csv) · [Profile checks](results/rust-date-cache/profile/checks.json)', '']
        country_profiles = sorted([r for r in profiled if r['scenario']=='country'], key=lambda r:float(r['read_parse_seconds']))
        if len(country_profiles)==3:
            r=country_profiles[1]
            share=float(r['parquet_to_daily_buffer_seconds'])/float(r['read_parse_seconds'])*100
            lines += [f'In the representative country run, about **{share:.0f}%** of reader time is in row handling and daily-buffer preparation, including lookup/validation. The remaining time is batch reading/decoding. This identifies where to investigate next; it does not separately measure hashing, validation or buffer copies.', '']
    REPORT.write_text('\n'.join(lines))


def profile():
    directory=OUT/'profile'
    if directory.exists(): raise FileExistsError(directory)
    directory.mkdir()
    runs=json.loads((OUT/'runs.json').read_text())
    checks=json.loads((OUT/'checks.json').read_text())
    environment=json.loads((OUT/'environment.json').read_text())
    binary=ROOT/'rust/target/release/portfolio-attribution'
    environment['profiling_binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
    environment['profiling_env']='ATTRIBUTION_PROFILE_READ=1'
    profiled,profile_checks=[],[]
    for scenario,expected,repetitions in SCENARIOS:
        for repetition in range(1,repetitions+1):
            out=directory/scenario/f'run-{repetition}';out.mkdir(parents=True)
            metrics=out/'metrics.csv'
            command=['env','ATTRIBUTION_PROFILE_READ=1',str(binary),'--data',str(ROOT/'attribution_data_parquet'),
                     '--scenario',scenario,'--threads','4','--max-days','0','--output-dir',str(out),
                     '--metrics',str(metrics),'--instruments-file',str(BASELINE/'subset.csv')]
            print(f'PROFILE {scenario} {repetition}',flush=True)
            usage=measured(command,out/'run.log')
            with metrics.open() as f: row=next(csv.DictReader(f))
            assert int(row['days'])==1095 and int(row['rows_read'])==175200000 and int(row['output_rows'])==expected
            decoding=float(row['parquet_batch_read_decode_seconds'])
            adapter=float(row['parquet_to_daily_buffer_seconds'])
            assert decoding>0 and adapter>0 and int(row['parquet_batches'])>0
            assert math.isclose(decoding+adapter,float(row['read_parse_seconds']),rel_tol=1e-9,abs_tol=1e-6)
            row.update({'repetition':repetition,**usage});profiled.append(row)
            path=out/f'{scenario}-portfolio_PF.parquet'
            for fmt in ['csv','parquet']:
                original=OUT/scenario/'run-1'/fmt/f'{scenario}-portfolio_PF.{fmt}'
                count,delta=compare_files(original,path);assert count==expected
                profile_checks.append({'scenario':scenario,'repetition':repetition,'comparison':f'cache-only {fmt}',
                                       'rows':count,'maximum_absolute_difference':delta,'status':'PASS'})
            (directory/'runs.json').write_text(json.dumps(profiled,indent=2)+'\n')
            (directory/'checks.json').write_text(json.dumps(profile_checks,indent=2)+'\n')
            with (directory/'runs.csv').open('w',newline='') as f:
                writer=csv.DictWriter(f,fieldnames=list(profiled[0]));writer.writeheader();writer.writerows(profiled)
            save(runs,checks,environment)
            print(f'  read/decode {decoding:.3f}s; adapter {adapter:.3f}s; whole run {usage["process_wall_seconds"]:.3f}s',flush=True)
    assert len(profiled)==7 and len(profile_checks)==14
    assert hashlib.sha256(binary.read_bytes()).hexdigest()==environment['profiling_binary_sha256']
    print(f'PROFILE COMPLETE: {REPORT}',flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile',action='store_true')
    if parser.parse_args().profile:
        profile();return
    if OUT.exists(): raise FileExistsError(OUT)
    OUT.mkdir(parents=True)
    binary=ROOT/'rust/target/release/portfolio-attribution'
    subset=BASELINE/'subset.csv'
    environment={'change':'cache date32 text until the integer day changes', 'threads':4,
                 'input':'unchanged attribution_data and attribution_data_parquet',
                 'repetitions':{'country':3,'other_scenarios':1}, 'cache':'OS-managed; paired format order alternates',
                 'slowdown_trigger_ratio':1.1,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest()}
    runs, checks=[],[]
    save(runs,checks,environment)
    for scenario,expected,repetitions in SCENARIOS:
        for repetition in range(1,repetitions+1):
            order=['csv','parquet'] if repetition%2 else ['parquet','csv']
            paths={}
            for fmt in order:
                out=OUT/scenario/f'run-{repetition}'/fmt;out.mkdir(parents=True)
                data=ROOT/('attribution_data' if fmt=='csv' else 'attribution_data_parquet')
                metrics=out/'metrics.csv'
                command=[str(binary),'--data',str(data),'--output-dir',str(out),'--metrics',str(metrics),
                         '--scenario',scenario,'--threads','4','--max-days','0','--instruments-file',str(subset)]
                print(f'START {scenario} pair {repetition} {fmt}',flush=True)
                usage=measured(command,out/'run.log')
                with metrics.open() as f: row=next(csv.DictReader(f))
                assert int(row['days'])==1095 and int(row['rows_read'])==175200000 and int(row['output_rows'])==expected
                row.update({'format':fmt,'repetition':repetition,**usage});runs.append(row)
                path=out/f'{scenario}-portfolio_PF.{fmt}';paths[fmt]=path
                for lang in ['rust','go']:
                    old=BASELINE/fmt/scenario/lang/path.name
                    count,delta=compare_files(old,path);assert count==expected
                    checks.append({'scenario':scenario,'repetition':repetition,'format':fmt,'comparison':f'original {lang}',
                                   'rows':count,'maximum_absolute_difference':delta,'status':'PASS'})
                save(runs,checks,environment)
                print(f"DONE {scenario} {fmt}: wall {usage['process_wall_seconds']:.3f}s; read {float(row['read_parse_seconds']):.3f}s",flush=True)
            count,delta=compare_files(paths['csv'],paths['parquet']);assert count==expected
            checks.append({'scenario':scenario,'repetition':repetition,'comparison':'fresh CSV/Parquet',
                           'rows':count,'maximum_absolute_difference':delta,'status':'PASS'})
            save(runs,checks,environment)
    assert len(runs)==14 and len(checks)==35
    assert hashlib.sha256(binary.read_bytes()).hexdigest()==environment['binary_sha256']
    print(f'COMPLETE: {REPORT}',flush=True)


if __name__=='__main__': main()
