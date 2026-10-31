use std::{
    collections::HashMap,
    error::Error,
    fs::{self, File},
    io::BufWriter,
    path::{Path, PathBuf},
    sync::{mpsc, Arc},
    thread,
    time::Instant,
};
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
const HEADER: [&str; 6] = [
    "Entity",
    "EffectiveDate",
    "InstrumentId",
    "Weight",
    "BaseReturn",
    "LocalReturn",
];
const OUT: [&str; 17] = [
    "Scenario",
    "Entity",
    "PeriodStart",
    "PeriodEnd",
    "Days",
    "RowType",
    "Segment",
    "ReturnType",
    "PortfolioWeight",
    "BenchmarkWeight",
    "Allocation",
    "Selection",
    "Interaction",
    "Total",
    "PortfolioReturn",
    "BenchmarkReturn",
    "ActiveReturn",
];
#[derive(Clone)]
struct Config {
    data: PathBuf,
    output: PathBuf,
    metrics: PathBuf,
    scenario: String,
    period: String,
    subset: Option<PathBuf>,
    threads: usize,
    max_days: usize,
}
fn config() -> Result<Config> {
    let mut c = Config {
        data: "attribution_data".into(),
        output: "results/rust".into(),
        metrics: "results/rust-metrics.csv".into(),
        scenario: "country".into(),
        period: String::new(),
        subset: None,
        threads: thread::available_parallelism()?.get(),
        max_days: 0,
    };
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        if a == "--help" {
            println!("--scenario security|subset|country|weekly|monthly --data DIR --output-dir DIR --metrics FILE --threads N --max-days N --instruments-file CSV --period all|day|week|month|quarter|year");
            std::process::exit(0);
        }
        let v = args.next().ok_or("missing option value")?;
        match a.as_str() {
            "--data" => c.data = v.into(),
            "--output-dir" => c.output = v.into(),
            "--metrics" => c.metrics = v.into(),
            "--scenario" => c.scenario = v,
            "--period" => c.period = v,
            "--instruments-file" => c.subset = Some(v.into()),
            "--threads" => c.threads = v.parse()?,
            "--max-days" => c.max_days = v.parse()?,
            _ => return Err(format!("unknown option {a}").into()),
        }
    }
    if !["security", "subset", "country", "weekly", "monthly"].contains(&c.scenario.as_str())
        || c.threads == 0
    {
        return Err("invalid scenario or threads".into());
    }
    if c.period.is_empty() {
        c.period = match c.scenario.as_str() {
            "weekly" => "week",
            "monthly" => "month",
            _ => "all",
        }
        .into()
    }
    if !["all", "day", "week", "month", "quarter", "year"].contains(&c.period.as_str()) {
        return Err("invalid period".into());
    }
    if c.scenario == "subset" && c.subset.is_none() {
        return Err("subset requires --instruments-file".into());
    }
    Ok(c)
}
#[derive(Clone, Copy, Default)]
struct Stat {
    w: f64,
    r: [f64; 2],
    present: bool,
}
struct Master {
    ids: HashMap<String, usize>,
    names: Vec<String>,
    countries: Vec<usize>,
    country_names: Vec<String>,
    selected: Vec<bool>,
    subset_slots: Vec<usize>,
    subset_names: Vec<String>,
}
fn reader(path: &Path, header: &[&str]) -> Result<csv::Reader<File>> {
    let mut r = csv::ReaderBuilder::new()
        .buffer_capacity(1024 * 1024)
        .from_path(path)?;
    if !r.headers()?.iter().eq(header.iter().copied()) {
        return Err(format!("bad header {}", path.display()).into());
    }
    Ok(r)
}
fn master(c: &Config) -> Result<Master> {
    let mut m = Master {
        ids: HashMap::new(),
        names: vec![],
        countries: vec![],
        country_names: vec![],
        selected: vec![],
        subset_slots: vec![],
        subset_names: vec![],
    };
    let mut countries = HashMap::new();
    for row in reader(
        &c.data.join("security_master.csv"),
        &["InstrumentId", "Currency", "Country"],
    )?
    .records()
    {
        let r = row?;
        if r.iter().any(str::is_empty) {
            return Err("blank master field".into());
        }
        let n = m.names.len();
        if m.ids.insert(r[0].into(), n).is_some() {
            return Err("duplicate master ID".into());
        }
        m.names.push(r[0].into());
        let next = countries.len();
        let k = *countries.entry(r[2].to_string()).or_insert(next);
        if k == m.country_names.len() {
            m.country_names.push(r[2].into())
        }
        m.countries.push(k);
    }
    m.selected = vec![c.scenario != "subset"; m.names.len()];
    if let Some(p) = &c.subset {
        for r in reader(p, &["InstrumentId"])?.records() {
            let r = r?;
            let i = *m.ids.get(&r[0]).ok_or("unknown subset instrument")?;
            m.selected[i] = true;
        }
    }
    if c.scenario == "subset" {
        m.subset_slots = vec![usize::MAX; m.names.len()];
        for (id, selected) in m.selected.iter().enumerate() {
            if *selected {
                m.subset_slots[id] = m.subset_names.len();
                m.subset_names.push(m.names[id].clone());
            }
        }
    }
    if m.names.is_empty() {
        return Err("empty master".into());
    }
    Ok(m)
}
#[derive(Default, Clone)]
struct Metrics {
    read: f64,
    filter: f64,
    aggregate: f64,
    compute: f64,
    write: f64,
    rows: u64,
    retained: u64,
    groups: u64,
    effects: u64,
    written: u64,
    bytes: u64,
    parsed: u64,
    report_rows: u64,
    checksum: [f64; 3],
}
impl Metrics {
    fn add(&mut self, b: &Self) {
        self.read += b.read;
        self.filter += b.filter;
        self.aggregate += b.aggregate;
        self.compute += b.compute;
        self.write += b.write;
        self.rows += b.rows;
        self.retained += b.retained;
        self.groups += b.groups;
        self.effects += b.effects;
        self.written += b.written;
        self.bytes += b.bytes;
        self.parsed += b.parsed;
        self.report_rows += b.report_rows;
        for j in 0..3 {
            self.checksum[j] += b.checksum[j];
        }
    }
}
struct Day {
    date: String,
    entity: String,
    stats: Vec<Stat>,
}
struct Stream {
    r: csv::Reader<File>,
    pending: Option<csv::StringRecord>,
    record: csv::StringRecord,
    previous: String,
    entity: String,
    raw: Vec<(usize, Stat)>,
    seen: Vec<bool>,
}
impl Stream {
    fn new(path: &Path) -> Result<Self> {
        Ok(Self {
            r: reader(path, &HEADER)?,
            pending: None,
            record: csv::StringRecord::new(),
            previous: String::new(),
            entity: String::new(),
            raw: Vec::new(),
            seen: Vec::new(),
        })
    }
    fn next(&mut self, m: &Master, c: &Config, metrics: &mut Metrics) -> Result<Option<Day>> {
        let start = Instant::now();
        let offset = self.r.position().byte();
        self.raw.clear();
        self.seen.resize(m.names.len(), false);
        self.seen.fill(false);
        let raw = &mut self.raw;
        let seen = &mut self.seen;
        let mut date = String::new();
        let mut total = 0.0;
        loop {
            if let Some(p) = self.pending.take() {
                self.record = p;
            } else if !self.r.read_record(&mut self.record)? {
                break;
            }
            let r = &self.record;
            if date.is_empty() {
                date = r[1].into();
                if !self.previous.is_empty() && date <= self.previous {
                    return Err("dates must strictly increase".into());
                }
                civil(&date)?;
            }
            if r[1] != date {
                self.pending = Some(r.clone());
                break;
            }
            if r.iter().any(str::is_empty) {
                return Err("blank return field".into());
            }
            if self.entity.is_empty() {
                self.entity = r[0].into()
            }
            if self.entity != r[0] {
                return Err("mixed entities".into());
            }
            let id = *m.ids.get(&r[2]).ok_or("unmapped instrument")?;
            if seen[id] {
                return Err(format!("duplicate instrument on {date}").into());
            }
            seen[id] = true;
            let w: f64 = r[3].parse()?;
            let b: f64 = r[4].parse()?;
            let l: f64 = r[5].parse()?;
            if !w.is_finite()
                || w < 0.0
                || !b.is_finite()
                || !l.is_finite()
                || b <= -1.0
                || l <= -1.0
            {
                return Err("invalid weight/return (returns must exceed -1 for linking)".into());
            }
            raw.push((
                id,
                Stat {
                    w,
                    r: [w * b, w * l],
                    present: true,
                },
            ));
            total += w;
        }
        metrics.read += start.elapsed().as_secs_f64();
        metrics.rows += raw.len() as u64;
        metrics.parsed += self.r.position().byte() - offset;
        if date.is_empty() {
            return Ok(None);
        }
        if (total - 1.0).abs() > 1e-6 {
            return Err(format!("weights sum to {total} on {date}").into());
        }
        self.previous = date.clone();
        let start = Instant::now();
        if c.scenario == "subset" {
            raw.retain(|(id, _)| m.selected[*id]);
        }
        let selected: f64 = raw.iter().map(|(_, s)| s.w).sum();
        if selected <= 0.0 {
            return Err(format!("zero selected weight: {}/{date}", self.entity).into());
        }
        metrics.retained += raw.len() as u64;
        metrics.filter += start.elapsed().as_secs_f64();
        let start = Instant::now();
        let n = if c.scenario == "country" {
            m.country_names.len()
        } else if c.scenario == "subset" {
            m.subset_names.len()
        } else {
            m.names.len()
        };
        let mut stats = vec![Stat::default(); n];
        // Normalize even full weights to remove source rounding and make daily reconciliation exact.
        for &(id, s) in raw.iter() {
            let k = if c.scenario == "country" {
                m.countries[id]
            } else if c.scenario == "subset" {
                m.subset_slots[id]
            } else {
                id
            };
            let a = &mut stats[k];
            a.present = true;
            a.w += s.w / selected;
            for t in 0..2 {
                a.r[t] += s.r[t] / selected;
            }
        }
        metrics.groups += stats.iter().filter(|s| s.present).count() as u64;
        metrics.aggregate += start.elapsed().as_secs_f64();
        Ok(Some(Day {
            date,
            entity: self.entity.clone(),
            stats,
        }))
    }
}
// Gregorian civil date to epoch-day, with strict round-trip validation.
fn civil(s: &str) -> Result<(i32, u32, u32, i64)> {
    if s.len() != 10
        || s.as_bytes().get(4) != Some(&b'-')
        || s.as_bytes().get(7) != Some(&b'-')
        || !s
            .bytes()
            .enumerate()
            .all(|(i, b)| i == 4 || i == 7 || b.is_ascii_digit())
    {
        return Err("date must be YYYY-MM-DD".into());
    }
    let y: i32 = s[..4].parse()?;
    let m: u32 = s[5..7].parse()?;
    let d: u32 = s[8..].parse()?;
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let lengths = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if y < 1 || !(1..=12).contains(&m) || d < 1 || d > lengths[(m - 1) as usize] {
        return Err("invalid date".into());
    }
    let yy = y - i32::from(m <= 2);
    let era = yy.div_euclid(400);
    let yoe = yy - era * 400;
    let mp = m as i32 + if m > 2 { -3 } else { 9 };
    let doy = (153 * mp + 2) / 5 + d as i32 - 1;
    let days = era as i64 * 146097 + (yoe * 365 + yoe / 4 - yoe / 100 + doy) as i64 - 719468;
    Ok((y, m, d, days))
}
fn bucket(date: &str, period: &str) -> Result<String> {
    if period == "all" {
        return Ok("ALL".into());
    }
    let (y, m, _, days) = civil(date)?;
    Ok(match period {
        "week" => format!("W{}", (days + 3).div_euclid(7)),
        "month" => format!("{y}-{m}"),
        "quarter" => format!("{y}-Q{}", (m - 1) / 3 + 1),
        "year" => y.to_string(),
        _ => date.into(),
    })
}
fn effect(p: Stat, b: Stat, t: usize, total_b: f64) -> [f64; 3] {
    let mut rp = if p.w > 0.0 { p.r[t] / p.w } else { 0.0 };
    let mut rb = if b.w > 0.0 { b.r[t] / b.w } else { rp };
    if p.w == 0.0 {
        rp = rb
    }
    if b.w == 0.0 {
        rb = rp
    }
    [
        (p.w - b.w) * (rb - total_b),
        b.w * (rp - rb),
        (p.w - b.w) * (rp - rb),
    ]
}
#[derive(Clone, Default)]
struct Acc {
    w: [f64; 2],
    e: [[f64; 3]; 2],
    present: bool,
    held: bool,
}
struct Period {
    key: String,
    start: String,
    end: String,
    days: usize,
    growth: [[f64; 2]; 2],
    acc: Vec<Acc>,
    portfolio_only: bool,
}
impl Period {
    fn new(day: &Day, key: String, c: &Config) -> Self {
        let portfolio_only = ["weekly", "monthly"].contains(&c.scenario.as_str());
        Self {
            key,
            start: day.date.clone(),
            end: day.date.clone(),
            days: 0,
            growth: [[1.0; 2]; 2],
            acc: vec![Acc::default(); if portfolio_only { 1 } else { day.stats.len() }],
            portfolio_only,
        }
    }
    fn add(&mut self, p: &Day, b: &Day, metrics: &mut Metrics) -> Result<()> {
        let start = Instant::now();
        let mut totals = [[0.0; 2]; 2];
        for s in &p.stats {
            for t in 0..2 {
                totals[0][t] += s.r[t];
            }
        }
        for s in &b.stats {
            for t in 0..2 {
                totals[1][t] += s.r[t];
            }
        }
        let mut daily = [[0.0; 3]; 2];
        for k in 0..p.stats.len() {
            let ps = p.stats[k];
            let bs = b.stats[k];
            if ps.present || bs.present {
                metrics.effects += 6;
            }
            for t in 0..2 {
                let e = effect(ps, bs, t, totals[1][t]);
                for j in 0..3 {
                    daily[t][j] += e[j];
                    if !self.portfolio_only {
                        self.acc[k].e[t][j] =
                            self.acc[k].e[t][j] * (1.0 + totals[1][t]) + e[j] * self.growth[0][t];
                    }
                }
            }
            if !self.portfolio_only {
                let a = &mut self.acc[k];
                a.present |= ps.present || bs.present;
                a.held |= ps.present;
                a.w[0] += ps.w;
                a.w[1] += bs.w;
            }
        }
        for t in 0..2 {
            if (daily[t].iter().sum::<f64>() - (totals[0][t] - totals[1][t])).abs() > 1e-9 {
                return Err("daily reconciliation failed".into());
            }
            if self.portfolio_only {
                for j in 0..3 {
                    self.acc[0].e[t][j] = self.acc[0].e[t][j] * (1.0 + totals[1][t])
                        + daily[t][j] * self.growth[0][t];
                }
            }
            for side in 0..2 {
                self.growth[side][t] *= 1.0 + totals[side][t];
                if !self.growth[side][t].is_finite() {
                    return Err("nonfinite compounded return".into());
                }
            }
        }
        if self.portfolio_only {
            let a = &mut self.acc[0];
            a.present = true;
            a.held = true;
            a.w[0] += 1.0;
            a.w[1] += 1.0;
        }
        self.days += 1;
        self.end = p.date.clone();
        metrics.compute += start.elapsed().as_secs_f64();
        Ok(())
    }
    fn write(
        &self,
        c: &Config,
        entity: &str,
        names: &[String],
        w: &mut csv::Writer<BufWriter<File>>,
        metrics: &mut Metrics,
    ) -> Result<()> {
        let start = Instant::now();
        let write_before = metrics.write;
        let mut totals = [[0.0; 3]; 2];
        let mut residual = Acc::default();
        let security = ["security", "subset"].contains(&c.scenario.as_str());
        for a in &self.acc {
            for t in 0..2 {
                for j in 0..3 {
                    totals[t][j] += a.e[t][j];
                }
            }
            if security && a.present && !a.held {
                residual.present = true;
                for side in 0..2 {
                    residual.w[side] += a.w[side];
                }
                for t in 0..2 {
                    for j in 0..3 {
                        residual.e[t][j] += a.e[t][j];
                    }
                }
            }
        }
        for t in 0..2 {
            if (totals[t].iter().sum::<f64>() - (self.growth[0][t] - self.growth[1][t])).abs()
                > 1e-8
            {
                return Err("linked reconciliation failed".into());
            }
        }
        let mut record = csv::ByteRecord::new();
        let days = self.days.to_string();
        if self.portfolio_only {
            for t in 0..2 {
                self.row(
                    c,
                    entity,
                    "total",
                    "ALL",
                    t,
                    self.acc[0].w,
                    totals[t],
                    &days,
                    &mut record,
                    w,
                    metrics,
                )?;
            }
        } else {
            for (k, a) in self.acc.iter().enumerate() {
                if a.present && (!security || a.held) {
                    for t in 0..2 {
                        self.row(
                            c,
                            entity,
                            "detail",
                            &names[k],
                            t,
                            a.w,
                            a.e[t],
                            &days,
                            &mut record,
                            w,
                            metrics,
                        )?;
                    }
                }
            }
            if residual.present {
                for t in 0..2 {
                    self.row(
                        c,
                        entity,
                        "benchmark_only",
                        "BENCHMARK_ONLY",
                        t,
                        residual.w,
                        residual.e[t],
                        &days,
                        &mut record,
                        w,
                        metrics,
                    )?;
                }
            }
        }
        metrics.compute +=
            (start.elapsed().as_secs_f64() - (metrics.write - write_before)).max(0.0);
        Ok(())
    }
    fn row(
        &self,
        c: &Config,
        entity: &str,
        kind: &str,
        name: &str,
        t: usize,
        weights: [f64; 2],
        e: [f64; 3],
        days: &str,
        record: &mut csv::ByteRecord,
        w: &mut csv::Writer<BufWriter<File>>,
        metrics: &mut Metrics,
    ) -> Result<()> {
        metrics.report_rows += 1;
        for j in 0..3 {
            metrics.checksum[j] += e[j];
        }
        let start = Instant::now();
        let rp = self.growth[0][t] - 1.0;
        let rb = self.growth[1][t] - 1.0;
        record.clear();
        for field in [
            c.scenario.as_str(),
            entity,
            &self.start,
            &self.end,
            days,
            kind,
            name,
            if t == 0 { "base" } else { "local" },
        ] {
            record.push_field(field.as_bytes());
        }
        let mut buffer = ryu::Buffer::new();
        for value in [
            weights[0] / self.days as f64,
            weights[1] / self.days as f64,
            e[0],
            e[1],
            e[2],
            e.iter().sum::<f64>(),
            rp,
            rb,
            rp - rb,
        ] {
            record.push_field(buffer.format(value).as_bytes());
        }
        w.write_byte_record(record)?;
        metrics.written += 1;
        metrics.write += start.elapsed().as_secs_f64();
        Ok(())
    }
}

fn worker(path: &Path, c: &Config, m: &Master, rx: mpsc::Receiver<Arc<Day>>) -> Result<Metrics> {
    let mut metrics = Metrics::default();
    let mut stream = Stream::new(path)?;
    let stem = path.file_stem().unwrap().to_string_lossy();
    let partial = c.output.join(format!("{}-{stem}.csv.partial", c.scenario));
    let start = Instant::now();
    let mut writer = csv::WriterBuilder::new()
        .buffer_capacity(1024 * 1024)
        .from_writer(BufWriter::with_capacity(
            1024 * 1024,
            File::create(partial)?,
        ));
    writer.write_record(OUT)?;
    metrics.write += start.elapsed().as_secs_f64();
    let mut period: Option<Period> = None;
    let mut entity = String::new();
    let names = if c.scenario == "country" {
        &m.country_names
    } else if c.scenario == "subset" {
        &m.subset_names
    } else {
        &m.names
    };
    let mut count = 0;
    for b in rx {
        let p = stream
            .next(m, c, &mut metrics)?
            .ok_or("portfolio ended before index")?;
        if p.date != b.date {
            return Err("portfolio/index date mismatch".into());
        }
        entity = p.entity.clone();
        let key = bucket(&p.date, &c.period)?;
        if period.as_ref().is_some_and(|s| s.key != key) {
            period
                .take()
                .unwrap()
                .write(c, &entity, names, &mut writer, &mut metrics)?;
        }
        if period.is_none() {
            period = Some(Period::new(&p, key, c))
        }
        period.as_mut().unwrap().add(&p, &b, &mut metrics)?;
        count += 1;
    }
    if count == 0 {
        return Err("no input dates".into());
    }
    if (c.max_days == 0 || count < c.max_days) && stream.next(m, c, &mut metrics)?.is_some() {
        return Err("extra portfolio dates".into());
    }
    period
        .unwrap()
        .write(c, &entity, names, &mut writer, &mut metrics)?;
    let start = Instant::now();

    writer.flush()?;
    writer.get_ref().get_ref().sync_all()?;
    metrics.write += start.elapsed().as_secs_f64();

    Ok(metrics)
}
fn run() -> Result<()> {
    let wall = Instant::now();
    let c = config()?;
    let m = master(&c)?;
    fs::create_dir_all(&c.output)?;
    if fs::canonicalize(&c.output)? == fs::canonicalize(&c.data)? {
        return Err("output directory must differ from input".into());
    }
    let mut paths: Vec<_> = fs::read_dir(&c.data)?
        .map(|e| e.map(|e| e.path()))
        .collect::<std::io::Result<_>>()?;
    paths.retain(|p| {
        p.file_name()
            .and_then(|s| s.to_str())
            .is_some_and(|n| n.starts_with("portfolio_") && n.ends_with(".csv"))
    });
    paths.sort();
    if paths.is_empty() {
        return Err("no portfolios".into());
    }
    let mut metrics = Metrics::default();
    let mut observed = 0;
    let mut input_bytes = fs::metadata(c.data.join("security_master.csv"))?.len();
    // One index producer broadcasts bounded one-day buffers. Batching limits native workers.
    for batch in paths.chunks(c.threads) {
        let result: Result<()> = thread::scope(|scope| {
            let mut senders = vec![];
            let mut workers = vec![];
            for p in batch {
                let (tx, rx) = mpsc::sync_channel(1);
                senders.push(tx);
                let (c, m) = (&c, &m);
                workers.push(scope.spawn(move || worker(p, c, m, rx)));
            }
            let mut index = Stream::new(&c.data.join("index.csv"))?;
            let mut days = 0;
            let production: Result<()> = (|| {
                while c.max_days == 0 || days < c.max_days {
                    let Some(day) = index.next(&m, &c, &mut metrics)? else {
                        break;
                    };
                    let day = Arc::new(day);
                    for tx in &senders {
                        let _ = tx.send(Arc::clone(&day));
                    }
                    days += 1;
                }
                Ok(())
            })();
            drop(senders);
            let mut errors = vec![];
            for worker in workers {
                match worker.join() {
                    Ok(Ok(x)) => metrics.add(&x),
                    Ok(Err(e)) => errors.push(e.to_string()),
                    Err(_) => errors.push("worker panicked".into()),
                }
            }
            production?;
            if !errors.is_empty() {
                return Err(errors.join("; ").into());
            }
            observed = days;
            Ok(())
        });
        result?;
        input_bytes += fs::metadata(c.data.join("index.csv"))?.len();
    }
    for path in &paths {
        input_bytes += fs::metadata(path)?.len();
        let stem = path.file_stem().unwrap().to_string_lossy();
        let target = c.output.join(format!("{}-{stem}.csv", c.scenario));

        fs::rename(target.with_extension("csv.partial"), &target)?;
        metrics.bytes += fs::metadata(target)?.len();
    }
    if let Some(p) = c.metrics.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(p)?;
    }
    let mut w = csv::Writer::from_path(&c.metrics)?;
    w.write_record([
        "language",
        "scenario",
        "period",
        "threads",
        "days",
        "input_file_bytes",
        "rows_read",
        "rows_retained",
        "groups",
        "effect_values",
        "output_rows",
        "output_bytes",
        "parsed_input_bytes",
        "read_parse_seconds",
        "filter_seconds",
        "aggregate_seconds",
        "compute_seconds",
        "write_seconds",
        "wall_seconds",
        "report_rows",
        "checksum_allocation",
        "checksum_selection",
        "checksum_interaction",
    ])?;
    w.write_record(vec![
        "rust".into(),
        c.scenario,
        c.period,
        c.threads.to_string(),
        observed.to_string(),
        input_bytes.to_string(),
        metrics.rows.to_string(),
        metrics.retained.to_string(),
        metrics.groups.to_string(),
        metrics.effects.to_string(),
        metrics.written.to_string(),
        metrics.bytes.to_string(),
        metrics.parsed.to_string(),
        metrics.read.to_string(),
        metrics.filter.to_string(),
        metrics.aggregate.to_string(),
        metrics.compute.to_string(),
        metrics.write.to_string(),
        wall.elapsed().as_secs_f64().to_string(),
        metrics.report_rows.to_string(),
        metrics.checksum[0].to_string(),
        metrics.checksum[1].to_string(),
        metrics.checksum[2].to_string(),
    ])?;
    w.flush()?;
    println!(
        "{} rows written; metrics {}",
        metrics.written,
        c.metrics.display()
    );
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn dates() {
        assert_eq!(
            bucket("2023-01-02", "week").unwrap(),
            bucket("2023-01-08", "week").unwrap()
        );
        assert_ne!(
            bucket("2023-01-01", "week").unwrap(),
            bucket("2023-01-02", "week").unwrap()
        );
        assert!(civil("2023-02-29").is_err());
        assert!(civil("2024-02-29").is_ok());
        assert_eq!(bucket("2023-03-31", "quarter").unwrap(), "2023-Q1");
    }
    #[test]
    fn formula() {
        let e = effect(
            Stat {
                w: 0.6,
                r: [0.06; 2],
                present: true,
            },
            Stat {
                w: 0.4,
                r: [0.02; 2],
                present: true,
            },
            0,
            0.03,
        );
        for (a, b) in e.into_iter().zip([0.004, 0.02, 0.01]) {
            assert!((a - b).abs() < 1e-12)
        }
    }
    #[test]
    fn linking() {
        let mut a: f64 = 0.0;
        let mut pg = 1.0;
        let mut bg = 1.0;
        for (p, b) in [(0.1, 0.05), (-0.1, -0.03), (0.0, 0.0)] {
            a = a * (1.0 + b) + (p - b) * pg;
            pg *= 1.0 + p;
            bg *= 1.0 + b;
        }
        assert!((a - (pg - bg)).abs() < 1e-12);
    }
}
