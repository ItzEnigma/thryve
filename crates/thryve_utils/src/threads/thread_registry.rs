//! Lock-free registry of idle worker threads, used to park and wake them
//! without a mutex or condvar.
//!
//! Each worker that may call a queue's blocking `pop()` is assigned a fixed
//! index for that queue's lifetime (`0..capacity`). [`WorkerRegistry`] tracks
//! which of those indices are currently idle as a bitset -- one bit per
//! worker, packed into `AtomicU64` words -- so a waker can find (and claim)
//! an idle worker with a handful of atomic operations instead of a lock.
//! Capacity has no fixed ceiling: it grows by one more `AtomicU64` word per
//! additional 64 workers.
//!
//! # Usage contract
//!
//! This registry only provides correct, hang-free wakeups if callers follow
//! two rules:
//!
//! 1. **Register before spinning, not just before parking.** Call
//!    [`set_idle`](WorkerRegistry::set_idle) *before* entering any
//!    spin/backoff retry window, not only immediately before
//!    [`std::thread::park`]. A [`notify_one`](WorkerRegistry::notify_one) or
//!    [`notify_all`](WorkerRegistry::notify_all) that lands while a thread is
//!    still spinning (not yet parked) is not lost: it pre-arms that thread's
//!    park token, so the eventual `park()` call returns immediately instead
//!    of blocking. Registering only right before `park()` would reopen a
//!    window where a wakeup arriving during the spin is missed entirely.
//! 2. **Clear your bit unconditionally after every `park()` call returns,**
//!    regardless of why it returned. `park()` may return spuriously with no
//!    corresponding `unpark()` at all (this is allowed by
//!    [`std::thread::park`]'s own contract); if that happens, nothing else
//!    will clear the bit. A thread that goes on to do other work while its
//!    bit is still set becomes a "stale" entry: some future `notify_one()`
//!    can waste its wakeup unparking a thread that isn't actually waiting for
//!    anything, instead of reaching one that is. Clearing is idempotent, so
//!    doing it unconditionally is always safe even when a waker already
//!    cleared the bit itself.
//!
//! [`PQChild`](crate::queues::priority_queue::PQChild) and
//! [`QChild`](crate::queues::seg_queue::QChild) both follow this contract in
//! their `pop()` implementations.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread::{self, Thread};

/// Lock-free bitset of idle worker indices, plus each index's `Thread` handle
/// for waking it. See the [module docs](self) for the invariants callers must
/// follow.
pub struct WorkerRegistry {
    /// One bit per worker index, packed 64 to a word: bit `i % 64` of
    /// `words[i / 64]` is set while worker `i` is idle.
    pub words: Box<[AtomicU64]>,
    /// Each worker's `Thread` handle, populated lazily the first time that
    /// index calls [`set_idle`](WorkerRegistry::set_idle). A worker's
    /// identity never changes across a queue's lifetime, so this is written
    /// at most once per index.
    pub threads: Box<[OnceLock<Thread>]>,
}

impl WorkerRegistry {
    /// Creates a registry sized for exactly `capacity` worker indices
    /// (`0..capacity`) -- exactly the number of distinct threads that will
    /// ever call [`set_idle`]/[`clear_idle`](WorkerRegistry::clear_idle) on
    /// it, not an upper bound to size generously.
    ///
    /// [`set_idle`]: WorkerRegistry::set_idle
    pub fn new(capacity: usize) -> Self {
        let n_words = capacity.div_ceil(64).max(1);
        Self {
            words: (0..n_words).map(|_| AtomicU64::new(0)).collect(),
            threads: (0..capacity).map(|_| OnceLock::new()).collect(),
        }
    }

    /// Splits a global worker index into `(word index, bit mask)`.
    fn locate(idx: usize) -> (usize, u64) {
        (idx / 64, 1u64 << (idx % 64))
    }

    /// Marks worker `idx` idle and records its `Thread` handle on first use.
    ///
    /// Must be called before entering a spin/backoff retry window, not only
    /// right before parking -- see the [module docs](self).
    pub fn set_idle(&self, idx: usize) {
        self.threads[idx].get_or_init(thread::current);
        let (w, bit) = Self::locate(idx);
        self.words[w].fetch_or(bit, Ordering::AcqRel);
    }

    /// Marks worker `idx` no longer idle. Idempotent: clearing an
    /// already-clear bit is a no-op.
    ///
    /// Callers must invoke this unconditionally after every
    /// [`std::thread::park`] call returns -- see the [module docs](self) for
    /// why a conditional clear (e.g. only on a "real" wakeup) is not safe.
    pub fn clear_idle(&self, idx: usize) {
        let (w, bit) = Self::locate(idx);
        self.words[w].fetch_and(!bit, Ordering::Relaxed);
    }

    /// Wakes one idle worker, if any are registered. Scans words in order and
    /// claims the lowest-indexed set bit via compare-exchange (retrying on
    /// contention), so two concurrent callers can never both claim the same
    /// worker. A no-op if no bits are set.
    pub fn notify_one(&self) {
        for (w_idx, word) in self.words.iter().enumerate() {
            let mut cur = word.load(Ordering::Acquire);
            while cur != 0 {
                let bit_pos = cur.trailing_zeros();
                let mask = 1u64 << bit_pos;
                match word.compare_exchange_weak(
                    cur,
                    cur & !mask,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        let idx = w_idx * 64 + bit_pos as usize;
                        if let Some(t) = self.threads[idx].get() {
                            t.unpark();
                        }
                        return;
                    }
                    Err(actual) => cur = actual, // someone else claimed/changed it, retry this word
                }
            }
        }
    }

    /// Wakes every currently idle worker. Each word is atomically swapped to
    /// zero (claiming every bit it held in one step) before unparking the
    /// corresponding threads.
    pub fn notify_all(&self) {
        for (w_idx, word) in self.words.iter().enumerate() {
            let mut bits = word.swap(0, Ordering::AcqRel);
            while bits != 0 {
                let bit_pos = bits.trailing_zeros();
                let idx = w_idx * 64 + bit_pos as usize;
                if let Some(t) = self.threads[idx].get() {
                    t.unpark();
                }
                bits &= bits - 1; // drop the lowest set bit
            }
        }
    }
}
