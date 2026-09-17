// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

mod tests_worker_registry {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};

    use thryve_utils::threads::thread_registry::WorkerRegistry;

    /// Polls `condition` until it returns `true`, or panics with `msg` once
    /// `budget` elapses. Used instead of `JoinHandle::join()` in the
    /// concurrency tests below: `join()` has no timeout, so a real regression
    /// in `WorkerRegistry` (a missed wakeup) would hang the whole test binary
    /// instead of failing this one test.
    fn wait_until(budget: Duration, msg: &str, mut condition: impl FnMut() -> bool) {
        let start = Instant::now();
        while !condition() {
            if start.elapsed() > budget {
                panic!("{msg}");
            }
            thread::sleep(Duration::from_millis(2));
        }
    }

    // ---- capacity / layout ----

    #[test]
    fn new_sizes_words_to_capacity() {
        assert_eq!(WorkerRegistry::new(0).words.len(), 1); // capacity 0 still gets 1 word
        assert_eq!(WorkerRegistry::new(1).words.len(), 1);
        assert_eq!(WorkerRegistry::new(64).words.len(), 1);
        assert_eq!(WorkerRegistry::new(65).words.len(), 2);
        assert_eq!(WorkerRegistry::new(128).words.len(), 2);
        assert_eq!(WorkerRegistry::new(129).words.len(), 3);
    }

    #[test]
    fn new_allocates_one_thread_slot_per_capacity() {
        let reg = WorkerRegistry::new(70);
        assert_eq!(reg.threads.len(), 70);
    }

    // ---- bit-level set/clear correctness (single-threaded, direct state inspection) ----

    #[test]
    fn set_idle_sets_the_correct_bit_within_a_word() {
        let reg = WorkerRegistry::new(4);
        reg.set_idle(2);
        assert_eq!(reg.words[0].load(Ordering::SeqCst), 0b0100);
    }

    #[test]
    fn set_idle_crosses_word_boundaries_correctly() {
        let reg = WorkerRegistry::new(70); // 2 words: bits 0-63, 64-69
        reg.set_idle(0);
        reg.set_idle(63);
        reg.set_idle(64);
        reg.set_idle(69);

        assert_eq!(
            reg.words[0].load(Ordering::SeqCst),
            (1u64 << 0) | (1u64 << 63)
        );
        assert_eq!(
            reg.words[1].load(Ordering::SeqCst),
            (1u64 << 0) | (1u64 << 5)
        );
    }

    #[test]
    fn set_idle_is_idempotent() {
        let reg = WorkerRegistry::new(4);
        reg.set_idle(1);
        reg.set_idle(1);
        assert_eq!(reg.words[0].load(Ordering::SeqCst), 0b0010);
    }

    #[test]
    fn set_idle_records_the_calling_thread() {
        let reg = WorkerRegistry::new(1);
        reg.set_idle(0);
        assert_eq!(
            reg.threads[0].get().map(|t| t.id()),
            Some(thread::current().id())
        );
    }

    #[test]
    fn clear_idle_clears_only_the_targeted_bit() {
        let reg = WorkerRegistry::new(16);
        reg.set_idle(3);
        reg.set_idle(10);

        reg.clear_idle(3);

        assert_eq!(reg.words[0].load(Ordering::SeqCst), 1u64 << 10);
    }

    #[test]
    fn clear_idle_on_a_bit_that_was_never_set_is_a_noop() {
        let reg = WorkerRegistry::new(4);
        reg.set_idle(1);

        reg.clear_idle(2); // never set

        assert_eq!(reg.words[0].load(Ordering::SeqCst), 0b0010);
    }

    // ---- notify_one / notify_all bit bookkeeping (single-threaded) ----

    #[test]
    fn notify_one_on_an_empty_registry_does_nothing() {
        let reg = WorkerRegistry::new(4);
        reg.notify_one(); // must not panic
        assert_eq!(reg.words[0].load(Ordering::SeqCst), 0);
    }

    #[test]
    fn notify_one_clears_the_lowest_set_bit_only() {
        let reg = WorkerRegistry::new(8);
        reg.set_idle(2);
        reg.set_idle(5);

        reg.notify_one();

        // bit 2 (lower index) is claimed and cleared; bit 5 is untouched.
        assert_eq!(reg.words[0].load(Ordering::SeqCst), 1u64 << 5);
    }

    #[test]
    fn notify_one_finds_a_set_bit_in_a_later_word() {
        let reg = WorkerRegistry::new(70);
        reg.set_idle(66); // word[1], bit 2

        reg.notify_one();

        assert_eq!(reg.words[0].load(Ordering::SeqCst), 0);
        assert_eq!(reg.words[1].load(Ordering::SeqCst), 0);
    }

    #[test]
    fn notify_all_clears_every_set_bit_across_all_words() {
        let reg = WorkerRegistry::new(130); // 3 words
        for idx in [0, 63, 64, 100, 129] {
            reg.set_idle(idx);
        }

        reg.notify_all();

        for word in reg.words.iter() {
            assert_eq!(word.load(Ordering::SeqCst), 0);
        }
    }

    // ---- real park/unpark wake-up behavior (multi-threaded) ----

    #[test]
    fn notify_one_wakes_exactly_one_parked_thread() {
        let reg = Arc::new(WorkerRegistry::new(2));
        let woke = Arc::new(AtomicUsize::new(0));

        let spawn_parker = |idx: usize| {
            let reg = Arc::clone(&reg);
            let woke = Arc::clone(&woke);
            thread::spawn(move || {
                reg.set_idle(idx);
                thread::park();
                woke.fetch_add(1, Ordering::SeqCst);
            })
        };
        let t0 = spawn_parker(0);
        let t1 = spawn_parker(1);

        // Wait until both threads have registered as idle before waking any
        // of them, so notify_one() has something deterministic to find.
        wait_until(
            Duration::from_secs(2),
            "threads never registered as idle",
            || reg.words[0].load(Ordering::SeqCst) == 0b11,
        );

        reg.notify_one();
        wait_until(
            Duration::from_secs(2),
            "notify_one() did not wake any thread",
            || woke.load(Ordering::SeqCst) == 1,
        );
        // Give the still-parked thread a moment it shouldn't need -- it must
        // still be blocked, not woken by the same notify_one() call.
        thread::sleep(Duration::from_millis(50));
        assert_eq!(
            woke.load(Ordering::SeqCst),
            1,
            "notify_one() woke more than one thread"
        );

        reg.notify_one();
        wait_until(
            Duration::from_secs(2),
            "second notify_one() did not wake the remaining thread",
            || woke.load(Ordering::SeqCst) == 2,
        );

        t0.join().unwrap();
        t1.join().unwrap();
    }

    #[test]
    fn notify_all_wakes_every_thread_including_across_multiple_words() {
        const N: usize = 130; // spans 3 AtomicU64 words -- the case a single-word registry can't exercise
        let reg = Arc::new(WorkerRegistry::new(N));
        let woke = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..N)
            .map(|idx| {
                let reg = Arc::clone(&reg);
                let woke = Arc::clone(&woke);
                thread::spawn(move || {
                    reg.set_idle(idx);
                    thread::park();
                    woke.fetch_add(1, Ordering::SeqCst);
                })
            })
            .collect();

        wait_until(
            Duration::from_secs(5),
            "not every thread registered as idle in time",
            || reg.threads.iter().all(|t| t.get().is_some()),
        );

        reg.notify_all();

        wait_until(
            Duration::from_secs(5),
            "notify_all() failed to wake every parked thread",
            || woke.load(Ordering::SeqCst) == N,
        );

        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn a_notify_that_arrives_before_park_is_not_lost() {
        // Exercises the sticky-token property this whole design depends on:
        // unpark() called before the corresponding park() must still wake it,
        // not be silently dropped the way a Condvar's notify would be.
        let reg = WorkerRegistry::new(1);
        reg.set_idle(0);

        reg.notify_one(); // "arrives" before this thread ever calls park()

        let start = Instant::now();
        thread::park(); // must return immediately via the pre-armed token
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "park() blocked despite an earlier unpark() -- the sticky-token guarantee was violated"
        );
    }
}
