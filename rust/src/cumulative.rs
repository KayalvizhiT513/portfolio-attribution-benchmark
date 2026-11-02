//! US Total statistics: shared attribution daily reader and parallel month ends.
use crate::{input::Input, Config, Master, Metrics, Result, HEADER};
use std::{
    fs,
    path::{Path, PathBuf},
    thread,
    time::Instant,
};
fn cumulative_path(c: &Config, input: &Path) -> PathBuf {
    c.output.join(format!(
        "cumulative-{}.csv",
        input.file_stem().unwrap().to_string_lossy()
    ))
}
struct TotalObservation {
    date: String,
    p: [f64; 2],
    b: [f64; 2],
}
fn cumulative_values(obs: &[TotalObservation], t: usize) -> Vec<String> {
    let (mut n, mut mp, mut mb, mut ma, mut vp, mut vb, mut va, mut cov) =
        (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0);
    for x in obs {
        n += 1.0;
        let (p, b) = (x.p[t], x.b[t]);
        let a = p - b;
        let (dp, db, da) = (p - mp, b - mb, a - ma);
        mp += dp / n;
        mb += db / n;
        ma += da / n;
        vp += dp * (p - mp);
        vb += db * (b - mb);
        va += da * (a - ma);
        cov += dp * (b - mb);
    }
    if n < 2.0 {
        return vec![String::new(); 5];
    }
    let te = (va.max(0.0) / (n - 1.0)).sqrt();
    vec![
        (vp.max(0.0) / (n - 1.0)).sqrt().to_string(),
        (vb.max(0.0) / (n - 1.0)).sqrt().to_string(),
        te.to_string(),
        if te > 0.0 {
            (ma / te).to_string()
        } else {
            String::new()
        },
        if vb > 0.0 {
            (cov / vb).to_string()
        } else {
            String::new()
        },
    ]
}
// Pure calculation API, independent of files and the CLI dispatcher.
fn cumulative_rows(
    obs: &[TotalObservation],
    entity: &str,
    effective_workers: usize,
) -> Result<Vec<Vec<String>>> {
    if effective_workers == 0 {
        return Err("cumulative requires positive workers".into());
    }
    let next = std::sync::atomic::AtomicUsize::new(0);
    let rows = thread::scope(|scope| -> Result<Vec<Vec<String>>> {
        let mut workers = vec![];
        for _ in 0..effective_workers.min(36) {
            let next = &next;
            workers.push(scope.spawn(move || {
                let mut rows = vec![];
                loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= 36 {
                        break;
                    }
                    let year = 2023 + i / 12;
                    let month = i % 12 + 1;
                    let days = match month {
                        4 | 6 | 9 | 11 => 30,
                        2 => {
                            if year % 4 == 0 {
                                29
                            } else {
                                28
                            }
                        }
                        _ => 31,
                    };
                    let end = format!("{year:04}-{month:02}-{days:02}");
                    let n = obs.partition_point(|x| x.date <= end);
                    let mut row = vec![
                        entity.to_string(),
                        "Total".into(),
                        "2023-01-01".into(),
                        end,
                        n.to_string(),
                        if n > 0 {
                            obs[n - 1].date.clone()
                        } else {
                            String::new()
                        },
                    ];
                    for t in 0..2 {
                        row.extend(cumulative_values(&obs[..n], t))
                    }
                    rows.push((i, row));
                }
                rows
            }));
        }
        let mut rows = vec![vec![]; 36];
        for worker in workers {
            for (i, row) in worker
                .join()
                .map_err(|_| "effective-date worker panicked")?
            {
                rows[i] = row
            }
        }
        Ok(rows)
    })?;
    Ok(rows)
}
fn write_cumulative(
    path: &Path,
    c: &Config,
    obs: &[TotalObservation],
    entity: &str,
    metrics: &mut Metrics,
) -> Result<()> {
    let start = Instant::now();
    let rows = cumulative_rows(obs, entity, c.effective_workers)?;
    metrics.compute += start.elapsed().as_secs_f64();
    let start = Instant::now();
    let target = cumulative_path(c, path);
    let mut writer = csv::Writer::from_path(crate::output::partial(&target))?;
    let mut header = vec![
        "Entity".to_string(),
        "Level".into(),
        "PeriodStart".into(),
        "EffectiveDate".into(),
        "Observations".into(),
        "LastObservationDate".into(),
    ];
    for t in ["Base", "Local"] {
        for name in [
            "PortfolioStdDev",
            "BenchmarkStdDev",
            "TrackingError",
            "InformationRatio",
            "Beta",
        ] {
            header.push(format!("{t}{name}"))
        }
    }
    writer.write_record(header)?;
    for row in rows {
        writer.write_record(row)?
    }
    writer.flush()?;
    metrics.written = 36;
    metrics.report_rows = 36;
    metrics.write += start.elapsed().as_secs_f64();
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn empty_us_windows() {
        let rows = cumulative_rows(&[], "PF", 4).unwrap();
        assert_eq!(rows.len(), 36);
        for row in rows {
            assert_eq!(row[4], "0");
            assert_eq!(row[5], "");
            assert!(row[6..].iter().all(|v| v.is_empty()));
        }
    }
    #[test]
    fn zero_denominators() {
        let obs: Vec<_> = (0..3)
            .map(|_| TotalObservation {
                date: String::new(),
                p: [0.01; 2],
                b: [0.01; 2],
            })
            .collect();
        assert_eq!(cumulative_values(&obs, 0), vec!["0", "0", "0", "", ""]);
        assert_eq!(cumulative_values(&obs[..1], 0), vec![""; 5]);
    }
    #[test]
    fn identical_series() {
        let obs: Vec<_> = [-0.01, 0.0, 0.01]
            .into_iter()
            .map(|v| TotalObservation {
                date: String::new(),
                p: [v; 2],
                b: [v; 2],
            })
            .collect();
        let v = cumulative_values(&obs, 0);
        assert!((v[0].parse::<f64>().unwrap() - 0.01).abs() < 1e-15);
        assert_eq!(&v[2..], &["0", "", "1"]);
    }
}

struct DailyTotal {
    present: bool,
    date: String,
    entity: String,
    returns: [f64; 2],
}
struct ReadJob {
    paired: Vec<Vec<crate::country_parallel::DateRange>>,
}

pub(crate) fn run(c: &Config) -> Result<()> {
    let wall = Instant::now();
    fs::create_dir_all(&c.output)?;
    if fs::canonicalize(&c.output)? == fs::canonicalize(&c.data)? {
        return Err("output directory must differ from input".into());
    }
    let master_start = Instant::now();
    let m = crate::master(c)?;
    let master_seconds = master_start.elapsed().as_secs_f64();
    let master_path = crate::input::resolve(&c.data, "security_master")?;
    let portfolios = crate::input::portfolios(&c.data)?;
    let mut paths = vec![crate::input::resolve(&c.data, "index")?];
    paths.extend(portfolios.iter().cloned());
    let boundary_start = Instant::now();
    let mut input_bytes = fs::metadata(master_path)?.len();
    let mut jobs = vec![];
    let mut planned = vec![];
    let mut all_ranges = true;
    let mut range_tasks = 0;
    for path in &paths {
        input_bytes += fs::metadata(path)?.len();
        let ranges = if c.threads > 1 {
            if path.extension().is_some_and(|e| e == "parquet") {
                crate::country_parallel::parquet_ranges(path, c.max_days)?
            } else {
                crate::country_parallel::ranges(path, c.max_days)?
            }
        } else {
            None
        };
        planned.push(ranges.clone().unwrap_or_default());
        if ranges.as_ref().is_none_or(|r| r.is_empty()) {
            all_ranges = false;
        }
    }
    {
        if all_ranges {
            for r in &planned[1..] {
                if r.len() != planned[0].len()
                    || r.iter().zip(&planned[0]).any(|(a, b)| a.date != b.date)
                {
                    return Err("portfolio/index date mismatch".into());
                }
            }
            let size = planned[0].len().div_ceil(c.threads * 2);
            for first in (0..planned[0].len()).step_by(size) {
                let last = (first + size).min(planned[0].len());
                jobs.push(ReadJob {
                    paired: planned.iter().map(|r| r[first..last].to_vec()).collect(),
                });
                range_tasks += 1;
            }
        } else {
            jobs.push(ReadJob {
                paired: (0..paths.len()).map(|_| vec![]).collect(),
            });
        }
    }
    let boundary_seconds = boundary_start.elapsed().as_secs_f64();
    let pool_start = Instant::now();
    let workers = c.threads.min(jobs.len());
    let next = std::sync::atomic::AtomicUsize::new(0);
    let results = thread::scope(|scope| -> Result<Vec<_>> {
        let mut handles = vec![];
        for _ in 0..workers {
            let (jobs, paths, next, m) = (&jobs, &paths, &next, &m);
            handles.push(scope.spawn(move || -> Result<Vec<_>> {
                let mut results = vec![];
                loop {
                    let j = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if j >= jobs.len() {
                        break;
                    }
                    let job = &jobs[j];
                    let start = Instant::now();
                    let (totals, mt) = read_attribution_daily(paths, &job.paired, c, m)?;
                    let elapsed = start.elapsed().as_secs_f64();
                    for (file, totals) in totals.into_iter().enumerate() {
                        results.push((
                            file,
                            totals,
                            if file == 0 { mt.rows } else { 0 },
                            if file == 0 { elapsed } else { 0.0 },
                        ));
                    }
                }
                Ok(results)
            }));
        }
        let mut results = vec![];
        let mut errors = vec![];
        for h in handles {
            match h.join() {
                Ok(Ok(r)) => results.extend(r),
                Ok(Err(e)) => errors.push(e.to_string()),
                Err(_) => errors.push("daily totals reader panicked".into()),
            }
        }
        if !errors.is_empty() {
            return Err(errors.join("; ").into());
        }
        Ok(results)
    })?;
    let pool_seconds = pool_start.elapsed().as_secs_f64();
    let mut totals: Vec<Vec<DailyTotal>> = (0..paths.len()).map(|_| vec![]).collect();
    let mut metrics = Metrics::default();
    for (file, rows, count, elapsed) in results {
        totals[file].extend(rows);
        metrics.rows += count;
        metrics.read += elapsed;
    }
    for rows in &mut totals {
        rows.sort_by(|a, b| a.date.cmp(&b.date));
        if rows.is_empty() {
            return Err("no input dates".into());
        }
        if rows
            .windows(2)
            .any(|w| w[0].date >= w[1].date || w[0].entity != w[1].entity)
        {
            return Err("duplicate dates or mixed entities across ranges".into());
        }
    }
    for (i, path) in portfolios.iter().enumerate() {
        let (p, b) = (&totals[i + 1], &totals[0]);
        if p.len() != b.len() || p.iter().zip(b).any(|(p, b)| p.date != b.date) {
            return Err("portfolio/index date mismatch".into());
        }
        if p[0].date != "2023-01-01" || p.last().unwrap().date.as_str() < "2025-12-01" {
            return Err("36 month ends require history starting 2023-01-01 and extending into December 2025".into());
        }
        let mut obs = vec![];
        for (p, b) in p.iter().zip(b) {
            if p.present != b.present {
                return Err(format!("US holdings missing on only one side: {}", p.date).into());
            }
            if p.present {
                obs.push(TotalObservation {
                    date: p.date.clone(),
                    p: p.returns,
                    b: b.returns,
                })
            }
        }
        let mut side = Metrics::default();
        write_cumulative(path, c, &obs, &p[0].entity, &mut side)?;
        metrics.add(&side);
    }
    for path in &portfolios {
        let target = cumulative_path(c, path);
        fs::rename(crate::output::partial(&target), target)?
    }
    if let Some(parent) = c.metrics.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?
    }
    let mut w = csv::Writer::from_path(&c.metrics)?;
    w.write_record([
        "language",
        "scenario",
        "threads",
        "days",
        "input_file_bytes",
        "rows_read",
        "output_rows",
        "read_tasks",
        "read_workers",
        "date_range_tasks",
        "date_range_workers",
        "effective_date_workers",
        "effective_date_tasks",
        "cumulative_reader",
        "read_pipeline_wall_seconds",
        "master_load_seconds",
        "boundary_scan_seconds",
        "read_filter_aggregate_seconds",
        "compute_seconds",
        "write_seconds",
        "wall_seconds",
    ])?;
    w.write_record(vec![
        "rust".into(),
        "cumulative".into(),
        c.threads.to_string(),
        totals[0].len().to_string(),
        input_bytes.to_string(),
        metrics.rows.to_string(),
        metrics.written.to_string(),
        jobs.len().to_string(),
        workers.to_string(),
        range_tasks.to_string(),
        if range_tasks > 0 { workers } else { 0 }.to_string(),
        c.effective_workers.min(36).to_string(),
        (36 * portfolios.len()).to_string(),
        "attribution".into(),
        pool_seconds.to_string(),
        master_seconds.to_string(),
        boundary_seconds.to_string(),
        metrics.read.to_string(),
        metrics.compute.to_string(),
        metrics.write.to_string(),
        wall.elapsed().as_secs_f64().to_string(),
    ])?;
    w.flush()?;
    println!(
        "{} rows written; metrics {}",
        metrics.written,
        c.metrics.display()
    );
    Ok(())
}

// Exact attribution Stream::next validation/filter/normalization, without BF effects.
fn read_attribution_daily(
    paths: &[PathBuf],
    ranges: &[Vec<crate::country_parallel::DateRange>],
    c: &Config,
    m: &Master,
) -> Result<(Vec<Vec<DailyTotal>>, Metrics)> {
    let mut config = c.clone();
    config.scenario = "country".into();
    config.period = "day".into();
    let mut streams = vec![];
    for (i, path) in paths.iter().enumerate() {
        let input = if let (Some(first), Some(last)) = (ranges[i].first(), ranges[i].last()) {
            if path.extension().is_some_and(|e| e == "parquet") {
                Input::open_groups(
                    path,
                    &HEADER,
                    Some((first.start as usize..last.end as usize).collect()),
                )?
            } else {
                Input::csv_range(path, first.start, last.end)?
            }
        } else {
            Input::open(path, &HEADER)?
        };
        streams.push(crate::Stream {
            r: input,
            pending: false,
            previous: String::new(),
            entity: String::new(),
            raw: vec![],
            seen: vec![],
        });
    }
    let mut totals: Vec<Vec<DailyTotal>> = (0..paths.len()).map(|_| vec![]).collect();
    let mut metrics = Metrics::default();
    let mut count = 0;
    while c.max_days == 0 || count < c.max_days {
        let mut date = String::new();
        let mut ended = false;
        for (i, s) in streams.iter_mut().enumerate() {
            let d = s.next(m, &config, &mut metrics)?;
            if i == 0 && d.is_none() {
                ended = true;
            }
            if ended {
                if d.is_some() {
                    return Err("extra portfolio dates".into());
                }
                continue;
            }
            let d = d.ok_or("portfolio ended before index")?;
            if i == 0 {
                date = d.date.clone()
            } else if d.date != date {
                return Err("portfolio/index date mismatch".into());
            }
            if !ranges[i].is_empty()
                && (count >= ranges[i].len() || ranges[i][count].date != d.date)
            {
                return Err("date range mismatch".into());
            }
            let mut total = DailyTotal {
                date: d.date,
                entity: d.entity,
                present: false,
                returns: [0.0; 2],
            };
            for v in d.stats {
                total.present |= v.w > 0.0;
                for t in 0..2 {
                    total.returns[t] += v.r[t]
                }
            }
            totals[i].push(total);
        }
        if ended {
            if !ranges[0].is_empty() && count != ranges[0].len() {
                return Err("missing dates in range".into());
            }
            break;
        }
        count += 1;
    }
    Ok((totals, metrics))
}
