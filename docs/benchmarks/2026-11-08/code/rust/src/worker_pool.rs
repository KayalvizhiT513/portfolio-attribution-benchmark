//! FIFO admission for date-range workers. Permits live for the full range,
//! including I/O waits, and are released on success, error or panic unwinding.
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Condvar, Mutex,
    },
    time::Instant,
};

#[derive(Clone)]
pub(crate) struct WorkerLabel {
    pub request_id: u64,
    pub worker_id: usize,
    pub first_date: String,
    pub last_date: String,
}
struct Trace {
    started: Instant,
    file: Mutex<File>,
}
impl Trace {
    fn emit(&self, event: &str, label: &WorkerLabel, ticket: u64, slot: Option<usize>) {
        let row = serde_json::json!({"event":event,"seconds":self.started.elapsed().as_secs_f64(),
            "request_id":label.request_id,"worker_id":label.worker_id,"ticket":ticket,
            "slot_id":slot,"first_date":label.first_date,"last_date":label.last_date,
            "rust_thread_id":format!("{:?}",std::thread::current().id())});
        if let Err(error) = writeln!(self.file.lock().unwrap(), "{row}") {
            eprintln!("worker trace write failed: {error}");
        }
    }
}

#[derive(Default)]
struct State {
    active: usize,
    waiting: usize,
    peak: usize,
    next_ticket: u64,
    admitted_ticket: u64,
    slots: Vec<bool>,
}
pub(crate) struct WorkerPool {
    limit: usize,
    state: Mutex<State>,
    changed: Condvar,
    trace: Option<Trace>,
    pub(crate) activity: Activity,
}
#[derive(Default)]
pub(crate) struct Count {
    current: AtomicUsize,
    peak: AtomicUsize,
}
pub(crate) struct ActivityGuard<'a>(&'a Count);
impl Count {
    pub(crate) fn enter(&self) -> ActivityGuard<'_> {
        let current = self.current.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(current, Ordering::SeqCst);
        ActivityGuard(self)
    }
    fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"current":self.current.load(Ordering::SeqCst),"peak":self.peak.load(Ordering::SeqCst)})
    }
}
impl Drop for ActivityGuard<'_> {
    fn drop(&mut self) {
        self.0.current.fetch_sub(1, Ordering::SeqCst);
    }
}
#[derive(Default)]
pub(crate) struct Activity {
    pub reading_workers: Count,
    pub read_decode_calls: Count,
    pub retained_daily_inputs: Count,
    pub workers_with_smf_cache: Count,
}
impl Activity {
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"reading_workers":self.reading_workers.snapshot(),
            "read_decode_calls":self.read_decode_calls.snapshot(),
            "retained_daily_inputs":self.retained_daily_inputs.snapshot(),
            "workers_with_smf_cache":self.workers_with_smf_cache.snapshot()})
    }
}
pub(crate) struct Permit<'a> {
    pool: &'a WorkerPool,
    label: Option<WorkerLabel>,
    ticket: u64,
    slot: usize,
}
impl WorkerPool {
    pub(crate) fn new(limit: usize) -> Self {
        assert!(limit > 0);
        Self {
            limit,
            state: Mutex::new(State {
                slots: vec![false; limit],
                ..State::default()
            }),
            changed: Condvar::new(),
            trace: None,
            activity: Activity::default(),
        }
    }
    pub(crate) fn with_trace(limit: usize, path: Option<&Path>) -> std::io::Result<Self> {
        let mut pool = Self::new(limit);
        if let Some(path) = path {
            pool.trace = Some(Trace {
                started: Instant::now(),
                file: Mutex::new(OpenOptions::new().write(true).create_new(true).open(path)?),
            });
        }
        Ok(pool)
    }
    #[cfg(test)]
    pub(crate) fn acquire(&self) -> Permit<'_> {
        self.acquire_worker(None)
    }
    pub(crate) fn acquire_worker(&self, label: Option<WorkerLabel>) -> Permit<'_> {
        let mut state = self.state.lock().unwrap();
        let ticket = state.next_ticket;
        state.next_ticket += 1;
        state.waiting += 1;
        if let (Some(trace), Some(label)) = (&self.trace, &label) {
            trace.emit("queued", label, ticket, None);
        }
        while state.active == self.limit || ticket != state.admitted_ticket {
            state = self.changed.wait(state).unwrap();
        }
        state.waiting -= 1;
        state.active += 1;
        state.peak = state.peak.max(state.active);
        state.admitted_ticket += 1;
        let slot = state.slots.iter().position(|occupied| !occupied).unwrap();
        state.slots[slot] = true;
        if let (Some(trace), Some(label)) = (&self.trace, &label) {
            trace.emit("admitted", label, ticket, Some(slot + 1));
        }
        self.changed.notify_all();
        Permit {
            pool: self,
            label,
            ticket,
            slot,
        }
    }
    // All fields come from one lock acquisition, so health snapshots are consistent.
    pub(crate) fn snapshot(&self) -> (usize, usize, usize, usize) {
        let s = self.state.lock().unwrap();
        (self.limit, s.active, s.waiting, s.peak)
    }
}
impl Drop for Permit<'_> {
    fn drop(&mut self) {
        let mut state = self.pool.state.lock().unwrap();
        if let (Some(trace), Some(label)) = (&self.pool.trace, &self.label) {
            trace.emit("released", label, self.ticket, Some(self.slot + 1));
        }
        state.slots[self.slot] = false;
        state.active -= 1;
        self.pool.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::{mpsc, Arc},
        thread,
        time::{Duration, Instant},
    };
    fn await_waiters(pool: &WorkerPool, count: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while pool.snapshot().2 != count {
            assert!(Instant::now() < deadline, "workers did not queue");
            thread::yield_now();
        }
    }
    #[test]
    fn holds_capacity_and_admits_in_fifo_order() {
        let pool = Arc::new(WorkerPool::new(2));
        let first = pool.acquire();
        let second = pool.acquire();
        let (started, received) = mpsc::channel();
        let mut joins = Vec::new();
        let mut releases = Vec::new();
        for id in 0..3 {
            let p = Arc::clone(&pool);
            let tx = started.clone();
            let (release, wait) = mpsc::channel();
            releases.push(release);
            joins.push(thread::spawn(move || {
                let _permit = p.acquire();
                tx.send(id).unwrap();
                wait.recv().unwrap();
            }));
            await_waiters(&pool, id + 1);
        }
        assert!(received.try_recv().is_err());
        drop(first);
        assert_eq!(received.recv_timeout(Duration::from_secs(5)).unwrap(), 0);
        assert_eq!(pool.snapshot(), (2, 2, 2, 2));
        assert!(received.try_recv().is_err()); // A worker keeps its slot while waiting on I/O.
        drop(second);
        assert_eq!(received.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
        releases[0].send(()).unwrap();
        assert_eq!(received.recv_timeout(Duration::from_secs(5)).unwrap(), 2);
        releases[1].send(()).unwrap();
        releases[2].send(()).unwrap();
        for join in joins {
            join.join().unwrap();
        }
        assert_eq!(pool.snapshot(), (2, 0, 0, 2));
    }
    #[test]
    fn releases_slot_on_error_and_panic() {
        let pool = WorkerPool::new(1);
        let fails = || -> std::result::Result<(), ()> {
            let _permit = pool.acquire();
            Err(())
        };
        assert!(fails().is_err());
        assert_eq!(pool.snapshot().1, 0);
        assert!(std::panic::catch_unwind(|| {
            let _permit = pool.acquire();
            panic!("test");
        })
        .is_err());
        let permit = pool.acquire();
        assert_eq!(pool.snapshot().1, 1);
        drop(permit);
        assert_eq!(pool.snapshot(), (1, 0, 0, 1));
    }
}
