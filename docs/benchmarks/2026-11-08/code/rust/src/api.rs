//! Thin HTTP adapter: independent calls into the unchanged six-input engine.
use crate::worker_pool::WorkerPool;
use crate::{civil, six_input, Config, Result};
use serde_json::{json, Value};
use std::{
    fs,
    io::Read,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tiny_http::{Header, Method, Request, Response, Server, StatusCode};

#[derive(Default)]
struct Counters {
    active: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    sequence: AtomicU64,
}
fn respond(request: Request, status: u16, body: Value) {
    let response = Response::from_string(body.to_string())
        .with_status_code(StatusCode(status))
        .with_header(Header::from_bytes("Content-Type", "application/json").unwrap());
    let _ = request.respond(response);
}
fn dates(body: &[u8]) -> Result<(String, String)> {
    let value: Value = serde_json::from_slice(body)?;
    let object = value.as_object().ok_or("expected JSON object")?;
    if object.len() != 2 || object.keys().any(|k| k != "start_date" && k != "end_date") {
        return Err("provide only start_date and end_date".into());
    }
    let start = object["start_date"].as_str().ok_or("missing start_date")?;
    let end = object["end_date"].as_str().ok_or("missing end_date")?;
    // civil validates actual calendar dates; enforce the API's canonical format.
    for date in [start, end] {
        if date.len() != 10
            || date.as_bytes()[4] != b'-'
            || date.as_bytes()[7] != b'-'
            || date
                .bytes()
                .enumerate()
                .any(|(i, b)| i != 4 && i != 7 && !b.is_ascii_digit())
        {
            return Err("dates must be YYYY-MM-DD".into());
        }
        civil(date)?;
    }
    if end < start {
        return Err("end_date precedes start_date".into());
    }
    Ok((start.to_string(), end.to_string()))
}
fn calculate(
    base: &Config,
    start: String,
    end: String,
    id: u64,
    session: u128,
    pool: &WorkerPool,
    shared_smf: Option<&six_input::SharedSmf>,
) -> Result<Value> {
    let mut c = base.clone();
    c.listen = None;
    c.request_id = Some(id + 1);
    c.scenario = "six-input".into();
    c.period = "day".into();
    c.subset = None;
    c.max_days = 0;
    c.start_date = Some(start);
    c.end_date = Some(end);
    c.output = base
        .output
        .join(format!("request-{}-{session}-{id}", std::process::id()));
    fs::create_dir(&c.output)?;
    c.metrics = c.output.join("metrics.csv");
    let began = Instant::now();
    six_input::run_with_shared_smf(&c, Some(pool), shared_smf)?;
    let mut reader = csv::Reader::from_path(&c.metrics)?;
    let headers = reader.headers()?.clone();
    let row = reader.records().next().ok_or("missing metrics")??;
    let metrics: serde_json::Map<String, Value> = headers
        .iter()
        .zip(row.iter())
        .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
        .collect();
    Ok(
        json!({"request_id":id+1,"elapsed_seconds": began.elapsed().as_secs_f64(), "output_dir": c.output,
              "report": c.output.join("country-portfolio_PF.parquet"), "metrics": metrics}),
    )
}
fn handle(
    mut request: Request,
    base: &Config,
    counters: &Counters,
    session: u128,
    pool: &WorkerPool,
    requests: &WorkerPool,
    shared_smf: Option<&six_input::SharedSmf>,
) {
    if request.method() == &Method::Get && request.url() == "/health" {
        let (limit, running, waiting, peak) = pool.snapshot();
        let (request_limit, admitted_requests, queued_requests, peak_admitted_requests) =
            requests.snapshot();
        respond(
            request,
            200,
            json!({"pid":std::process::id(), "active":counters.active.load(Ordering::SeqCst),
            "completed":counters.completed.load(Ordering::SeqCst), "failed":counters.failed.load(Ordering::SeqCst),
            "workers_per_request":base.threads, "engine":"rust",
            "request_limit":request_limit, "running_requests":admitted_requests,
            "waiting_requests":queued_requests, "peak_running_requests":peak_admitted_requests,
            "worker_limit":limit, "running_date_workers":running,
            "waiting_date_workers":waiting, "peak_running_date_workers":peak,
            "read_activity":pool.activity.snapshot(), "shared_smf":shared_smf.map(|s|s.snapshot()).unwrap_or_else(||json!({"enabled":false,"state_scope":"worker","source_scope":"worker"}))}),
        );
        return;
    }
    if request.method() != &Method::Post || request.url() != "/attribution" {
        respond(request, 404, json!({"error":"unknown endpoint"}));
        return;
    }
    let mut body = Vec::new();
    let read = request.as_reader().take(4097).read_to_end(&mut body);
    let window = if read.is_err() || body.len() > 4096 {
        Err("invalid or oversized request body".into())
    } else {
        dates(&body)
    };
    let (start, end) = match window {
        Ok(dates) => dates,
        Err(e) => {
            respond(request, 400, json!({"error":e.to_string()}));
            return;
        }
    };
    let request_started = Instant::now();
    counters.active.fetch_add(1, Ordering::SeqCst);
    let id = counters.sequence.fetch_add(1, Ordering::SeqCst);
    // A worker panic is reported as a calculation failure. Allocator OOM aborts
    // remain native process failures; no synthetic allocation or memory cap.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        // Keep admission through calculation/cleanup. Queued requests have not
        // created outputs, loaded file metadata, or opened/mapped an SMF index.
        // This gate is separate from the global date-worker slots.
        let _admission = requests.acquire_worker(None);
        let queue_seconds = request_started.elapsed().as_secs_f64();
        calculate(base, start, end, id, session, pool, shared_smf).map(|mut body| {
            body["admitted_calculation_seconds"] = body["elapsed_seconds"].clone();
            body["request_admission_wait_seconds"] = queue_seconds.into();
            // Include admission wait in API latency; never hide it in the queue.
            body["elapsed_seconds"] = request_started.elapsed().as_secs_f64().into();
            body
        })
    }));
    counters.active.fetch_sub(1, Ordering::SeqCst);
    match result {
        Ok(Ok(body)) => {
            counters.completed.fetch_add(1, Ordering::SeqCst);
            respond(request, 200, body);
        }
        Ok(Err(e)) => {
            counters.failed.fetch_add(1, Ordering::SeqCst);
            respond(request, 422, json!({"error":e.to_string()}));
        }
        Err(_) => {
            counters.failed.fetch_add(1, Ordering::SeqCst);
            respond(
                request,
                500,
                json!({"error":"calculation panicked; see server log"}),
            );
        }
    }
}
pub(crate) fn serve(address: &str, base: &Config) -> Result<()> {
    fs::create_dir_all(&base.output)?;
    let server = Server::http(address)?;
    let base = Arc::new(base.clone());
    let counters = Arc::new(Counters::default());
    let pool = Arc::new(WorkerPool::with_trace(
        base.worker_limit,
        base.worker_trace.as_deref(),
    )?);
    let requests = Arc::new(WorkerPool::new(base.request_limit));
    let shared_smf = if base.private_smf {
        None
    } else {
        Some(Arc::new(six_input::SharedSmf::open(&base.data)?))
    };
    let session = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    println!(
        "Rust six-input API listening on {address}; {} admitted requests; {} workers/request; {} shared date-worker slots",
        base.request_limit, base.threads, base.worker_limit
    );
    for request in server.incoming_requests() {
        let base = Arc::clone(&base);
        let counters = Arc::clone(&counters);
        let pool = Arc::clone(&pool);
        let requests = Arc::clone(&requests);
        let shared_smf = shared_smf.clone();
        thread::spawn(move || {
            handle(
                request,
                &base,
                &counters,
                session,
                &pool,
                &requests,
                shared_smf.as_deref(),
            )
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validate_windows() {
        assert!(dates(br#"{"start_date":"2023-01-01","end_date":"2023-01-02"}"#).is_ok());
        for body in [
            r#"null"#,
            r#"{}"#,
            r#"{"start_date":"2023-02-30","end_date":"2023-03-01"}"#,
            r#"{"start_date":"2023-01-02","end_date":"2023-01-01"}"#,
            r#"{"start_date":"2023-01-01","end_date":"2023-01-02","threads":20}"#,
            r#"{"start_date":"2023-01-01","end_date":"2023-01-02"} {}"#,
        ] {
            assert!(dates(body.as_bytes()).is_err(), "{body}");
        }
    }
}
