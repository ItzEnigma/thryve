//! Concurrent push/pop throughput for `PQChild<Task>`.
//!
//! Isolates the priority task queue from the rest of the engine so it can be
//! measured directly: a fixed number of pusher threads and popper threads
//! move a fixed total number of tasks through the queue, and criterion
//! reports elapsed time / tasks-per-second for that batch.
//!
//! Run with: `cargo bench -p thryve_utils --bench priority_queue`

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

use thryve_utils::bases::notif::Notif;
use thryve_utils::cfg::config::Config;
use thryve_utils::extras::jobs::TaskJob;
use thryve_utils::extras::thread_local;
use thryve_utils::packets::packet_handle::PacketHandle;
use thryve_utils::packets::task::Task;
use thryve_utils::queues::priority_queue::PQChild;
use thryve_utils::res::TaskResult;
use thryve_utils::traits::queue::Queue;

/// A fresh, empty queue backed by a real `Config` and a real packet channel,
/// matching how `run_interactive::summon_requirements` wires one up.
///
/// `worker_count` must equal the number of popper threads this queue will
/// actually be used with: it sizes the `WorkerRegistry`, and each popper
/// thread must separately call `tl_set_worker_park_idx` with a distinct
/// index in `0..worker_count` before calling `pop()` -- see `run_scenario`.
/// A mismatch (or every thread defaulting to index 0) means multiple
/// genuinely-parked threads share one registry bit and one stored `Thread`
/// handle, so `notify_one()` wakes the wrong thread while the real waiters
/// never get a corresponding wakeup.
///
/// Returns the receivers too, and the caller must keep them alive for as
/// long as the queue is in use: `finalize()` calls `send_notification`,
/// which panics if the receiver has already been dropped (dropping them
/// immediately here was the bug that made the watchdog's rescue call panic
/// instead of rescuing anything).
fn make_queue(
    worker_count: usize,
) -> (
    PQChild<Task>,
    crossbeam::channel::Receiver<Task>,
    crossbeam::channel::Receiver<Notif>,
) {
    let (task_tx, task_rx) = crossbeam::channel::bounded(1);
    let (notif_tx, notif_rx) = crossbeam::channel::bounded(1);
    let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
    let mut config = Config::default();
    config.threads.worker_count = worker_count;
    (PQChild::new(&config, packet_handle), task_rx, notif_rx)
}

fn no_op_task() -> Task {
    Task::new("Bench", 1, TaskJob::new(TaskResult::default)).expect("valid task name")
}

/// Pushes `total_tasks` (split across `pushers` threads) and pops the same
/// count (split across `poppers` threads), all concurrently against one queue.
///
/// A real Thryve engine always eventually calls `finalize()`, whose
/// `notify_all()` guarantees any worker that missed a wakeup still gets
/// released. This benchmark's pushers stop dead once their share is sent, so
/// it has to provide that same guarantee itself, or a rare missed wakeup on
/// the very last task hangs the whole run instead of costing one thread a
/// few idle milliseconds. The watchdog below does exactly what a real
/// shutdown would: call `finalize()`, but only once every task has already
/// been handed to some thread's in-flight `pop()` call, so there's nothing
/// left for it to legitimately discard.
fn run_scenario(pushers: usize, poppers: usize, total_tasks: usize) {
    let (queue, _task_rx, _notif_rx) = make_queue(poppers);
    let push_claimed = AtomicUsize::new(0);
    let pop_claimed = AtomicUsize::new(0);
    // Incremented only after a pop() call has actually returned. `pop_claimed`
    // alone isn't enough to know when we're done: a thread bumps it and then
    // may sit blocked inside queue.pop() for a while, but other threads keep
    // racing ahead and can drive `pop_claimed` past total_tasks long before
    // that call returns -- "every slot has been picked up by someone" is not
    // the same as "everyone actually finished," and the watchdog below needs
    // the latter. `pop_completed` can only reach total_tasks once every
    // pop() call has genuinely returned.
    let pop_completed = AtomicUsize::new(0);

    thread::scope(|s| {
        for _ in 0..pushers {
            let queue = &queue;
            let push_claimed = &push_claimed;
            s.spawn(move || {
                while push_claimed.fetch_add(1, Ordering::Relaxed) < total_tasks {
                    queue.push(no_op_task());
                }
            });
        }

        for idx in 0..poppers {
            let queue = &queue;
            let pop_claimed = &pop_claimed;
            let pop_completed = &pop_completed;
            s.spawn(move || {
                thread_local::tl_set_worker_park_idx(idx);
                while pop_claimed.fetch_add(1, Ordering::Relaxed) < total_tasks {
                    black_box(queue.pop());
                    pop_completed.fetch_add(1, Ordering::Relaxed);
                }
            });
        }

        // Safety net, not the measured path: in the normal case `pop_completed`
        // reaches `total_tasks` well within this budget and the watchdog
        // returns immediately having done nothing.
        s.spawn(|| {
            for _ in 0..200 {
                if pop_completed.load(Ordering::Relaxed) >= total_tasks {
                    return;
                }
                thread::sleep(Duration::from_millis(5));
            }
            queue.finalize(None);
        });
    });
}

fn bench_priority_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("priority_queue_throughput");
    let total_tasks = 20_000usize;
    let pushers = 2usize;

    // 4 matches the framework's default worker_count; 8/12/16/20/24 trace the
    // scaling curve on this 12-core/24-thread machine; 32 pushes past full
    // oversubscription.
    //
    // Criterion's default 5s measurement window isn't enough to collect a
    // full 100 samples once popper count rises -- each individual iteration
    // gets slower (more threads contending, occasional missed-wakeup
    // stalls), so criterion was silently falling back to fewer samples and
    // printing "Unable to complete 100 samples in 5.0s" for every count
    // above 4. These per-count budgets are set from the actual "you may
    // wish to increase target time to Xs" values observed across multiple
    // runs (including the noisier unfixed variant, which needs the most
    // headroom), plus a safety margin so a full 100-sample run completes
    // cleanly every time rather than only when the machine gets lucky.
    let measurement_secs: &[(usize, u64)] = &[
        (4, 15),
        (8, 40),
        (12, 55),
        (16, 70),
        (20, 115),
        (24, 120),
        (32, 135),
    ];

    for &(poppers, secs) in measurement_secs {
        group.measurement_time(Duration::from_secs(secs));
        group.throughput(Throughput::Elements(total_tasks as u64));
        group.bench_with_input(
            BenchmarkId::new("poppers", poppers),
            &poppers,
            |b, &poppers| {
                b.iter(|| run_scenario(pushers, poppers, total_tasks));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_priority_queue);
criterion_main!(benches);
