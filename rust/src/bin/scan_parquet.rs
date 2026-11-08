use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use std::{env, fs::File, thread, time::Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let path = args
        .next()
        .unwrap_or_else(|| "results/large_parquet/securities.parquet".into());
    let requested_workers = match args.next() {
        Some(value) => value.parse::<usize>()?,
        None => thread::available_parallelism()?.get(),
    };
    if requested_workers == 0 || args.next().is_some() {
        return Err("usage: scan_parquet [path] [positive worker count]".into());
    }
    let start = Instant::now();
    let metadata = ArrowReaderMetadata::load(&File::open(&path)?, Default::default())?;
    let group_count = metadata.metadata().num_row_groups();
    let workers = requested_workers.min(group_count.max(1));
    // Each worker owns a disjoint contiguous range of row groups. Only one
    // decoded batch per reader is retained; the file footer is shared.
    let (rows, batches, bytes) = thread::scope(|scope| {
        let mut handles = Vec::new();
        for worker in 0..workers {
            let metadata = metadata.clone();
            let path = &path;
            handles.push(
                scope.spawn(move || -> Result<(usize, usize, usize), String> {
                    let run = || -> Result<_, Box<dyn std::error::Error>> {
                        let groups = (group_count * worker / workers
                            ..group_count * (worker + 1) / workers)
                            .collect();
                        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
                            File::open(path)?,
                            metadata,
                        )
                        .with_row_groups(groups)
                        .with_batch_size(65_536)
                        .build()?;
                        let (mut rows, mut batches, mut bytes) = (0, 0, 0);
                        for batch in reader {
                            let batch = batch?;
                            if batch.num_columns() != 17 {
                                return Err("expected 17 columns".into());
                            }
                            rows += batch.num_rows();
                            batches += 1;
                            bytes += batch.get_array_memory_size();
                            std::hint::black_box(&batch);
                        }
                        Ok((rows, batches, bytes))
                    };
                    run().map_err(|e| e.to_string())
                }),
            );
        }
        let mut totals = (0, 0, 0);
        for handle in handles {
            let (rows, batches, bytes) = handle
                .join()
                .map_err(|_| "reader worker panicked".to_string())??;
            totals.0 += rows;
            totals.1 += batches;
            totals.2 += bytes;
        }
        Ok::<_, String>(totals)
    })?;
    if rows != 365_000_000 {
        return Err(format!("unexpected row count: {rows}").into());
    }
    println!("{{\"language\":\"rust\",\"parallelism\":\"row_groups\",\"workers\":{workers},\"rows\":{rows},\"columns\":17,\"batch_size\":65536,\"batches\":{batches},\"decoded_array_memory_bytes_sum\":{bytes},\"parquet_bytes\":{},\"seconds\":{}}}", std::fs::metadata(path)?.len(), start.elapsed().as_secs_f64());
    Ok(())
}
