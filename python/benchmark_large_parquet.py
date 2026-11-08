"""Generate the 365M-row fixture and measure bounded-memory Parquet reads."""
import argparse
import datetime as dt
import gc
import json
import resource
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['generate', 'scan', 'load'])
    parser.add_argument('--path', default='results/large_parquet/securities.parquet')
    args = parser.parse_args()
    path = Path(args.path)
    started = time.perf_counter()
    if args.mode == 'generate':
        schema = pa.schema([('SecId', pa.int64()), ('Effective Date', pa.date32())]
                           + [(f'Float{i:02}', pa.float64()) for i in range(1, 16)])
        rng = np.random.default_rng(42)
        secids = pa.array(np.arange(1, 100001, dtype=np.int64))
        epoch = (dt.date(2016, 1, 1) - dt.date(1970, 1, 1)).days
        path.parent.mkdir(parents=True, exist_ok=True)
        # Random continuous data avoids misleading compression from repeated values.
        with pq.ParquetWriter(path, schema, compression='snappy', use_dictionary=False,
                              write_statistics=True) as writer:
            for day in range(3650):
                arrays = [secids, pa.array(np.full(100000, epoch + day, dtype=np.int32),
                                           type=pa.date32())]
                arrays += [pa.array(rng.random(100000)) for _ in range(15)]
                writer.write_table(pa.Table.from_arrays(arrays, schema=schema))
                if (day + 1) % 100 == 0:
                    print(json.dumps({'days_written': day + 1,
                                      'seconds': time.perf_counter() - started}), flush=True)
        rows = 365000000
        logical_bytes = rows * (8 + 4 + 15 * 8)
    elif args.mode == 'scan':
        rows = logical_bytes = 0
        parquet = pq.ParquetFile(path)
        max_batch = 0
        for batch in parquet.iter_batches(batch_size=100000, use_threads=True):
            rows += batch.num_rows
            logical_bytes += batch.nbytes
            max_batch = max(max_batch, batch.nbytes)
            del batch
        gc.collect()
    else:
        # Explicit mode for a host with enough RAM. No pandas conversion or copying.
        table = pq.read_table(path, memory_map=False, use_threads=True)
        rows, logical_bytes = table.num_rows, table.nbytes
    result = dict(mode=args.mode, rows=rows, logical_bytes=logical_bytes,
                  parquet_bytes=path.stat().st_size, seconds=time.perf_counter()-started,
                  peak_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                  arrow_peak_allocated_bytes=pa.default_memory_pool().max_memory(),
                  schema=str(pq.read_schema(path)))
    if args.mode == 'scan':
        result['max_batch_bytes'] = max_batch
    output = path.parent / (args.mode + '.json')
    output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2), flush=True)


if __name__ == '__main__':
    main()
