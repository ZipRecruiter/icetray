//! A bound on how long one poll of a batch of lookups holds an async
//! worker. A warm lookup completes without awaiting anything that is not
//! ready, so a task driving many of them at once, as a `MultiGet` run
//! does, would run them all inside one poll: tens of milliseconds for a
//! few hundred keys. Tokio's own budget, 128 units a poll, is spent only by
//! its resources, and a warm lookup spends none. Meanwhile the worker
//! answers nothing else, not even a GET that has completed, since it polls
//! the I/O and timer drivers only between task polls. So a run's poll is
//! timed (`sliced`), and the run yields between lookups once the poll has
//! run `LOOKUP_SLICE` (`yield_when_overdue`).

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use pin_project_lite::pin_project;

/// How long one poll of a sliced future runs lookups before it yields. A
/// yield costs the task a trip through the worker's queue and the worker a
/// poll of the drivers, a few microseconds, so a slice of several warm
/// lookups keeps the cost a small share, and a GET's completion waits at
/// most about a slice per task ahead of it.
/// Under saturation, slices of 100, 200 and 500 microseconds measured
/// alike; what matters is that a poll ends at all.
pub(crate) const LOOKUP_SLICE: Duration = Duration::from_micros(200);

thread_local! {
    /// When the current poll of the innermost sliced future on this thread
    /// began; `None` outside every one.
    static POLL_STARTED: Cell<Option<Instant>> = const { Cell::new(None) };
}

pin_project! {
    /// `inner`, with each of its polls timed for `yield_when_overdue`.
    pub(crate) struct Sliced<F> {
        #[pin]
        inner: F,
    }
}

/// `inner`, with each of its polls timed, so that `yield_when_overdue`
/// within it yields once a poll has run `LOOKUP_SLICE`.
pub(crate) fn sliced<F: Future>(inner: F) -> Sliced<F> {
    Sliced { inner }
}

/// Puts back the start of an enclosing sliced poll when a nested one ends,
/// by return or by unwinding.
struct Restore(Option<Instant>);

impl Drop for Restore {
    fn drop(&mut self) {
        POLL_STARTED.set(self.0);
    }
}

impl<F: Future> Future for Sliced<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        let _restore = Restore(POLL_STARTED.replace(Some(Instant::now())));
        self.project().inner.poll(cx)
    }
}

/// Yields once, when the sliced poll running this has run `LOOKUP_SLICE`
/// or longer; returns at once outside a sliced poll, as a lone Get is, and
/// within one that has time left. The yield wakes the task only after the
/// worker has polled its drivers, so a completed GET is seen first. The
/// task driving a batch calls it between lookups, never the lookups
/// themselves: tokio wakes the yields of one poll last first, so lookups
/// yielding apart would resume in reverse order.
pub(crate) async fn yield_when_overdue() {
    let overdue = POLL_STARTED
        .get()
        .is_some_and(|started| started.elapsed() >= LOOKUP_SLICE);
    if overdue {
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::FutureExt as _;
    use futures::task::noop_waker_ref;

    use super::*;

    /// Busy-waits `for_at_least`, standing in for a lookup's decode.
    fn spin(for_at_least: Duration) {
        let started = Instant::now();
        while started.elapsed() < for_at_least {
            std::hint::spin_loop();
        }
    }

    /// A lookup that checks the slice, then works past it.
    async fn overrunning(done: Arc<AtomicUsize>) {
        yield_when_overdue().await;
        spin(LOOKUP_SLICE);
        done.fetch_add(1, Ordering::Relaxed);
    }

    /// Lookups that each run a whole slice: a sliced poll runs at most one
    /// of them, since the first spends the slice and every later one it
    /// reaches yields, and the run ends once each has had its poll; outside
    /// a sliced poll all of them run in one.
    #[tokio::test]
    async fn test_a_sliced_poll_runs_lookups_until_its_slice_is_spent() {
        const LOOKUPS: usize = 3;
        let done = Arc::new(AtomicUsize::new(0));
        let lookups = || {
            let done = Arc::clone(&done);
            async move {
                for _ in 0..LOOKUPS {
                    overrunning(Arc::clone(&done)).await;
                }
            }
        };
        let mut cx = Context::from_waker(noop_waker_ref());
        let mut run = Box::pin(sliced(lookups()));
        let mut polls = 0;
        loop {
            let before = done.load(Ordering::Relaxed);
            let ready = run.as_mut().poll(&mut cx).is_ready();
            polls += 1;
            assert!(done.load(Ordering::Relaxed) - before <= 1, "poll {polls}");
            if ready {
                break;
            }
        }
        assert_eq!(done.load(Ordering::Relaxed), LOOKUPS);
        assert!(polls >= LOOKUPS);

        done.store(0, Ordering::Relaxed);
        assert!(lookups().now_or_never().is_some(), "unsliced, none yields");
        assert_eq!(done.load(Ordering::Relaxed), LOOKUPS);
    }

    /// A sliced poll nested in another times itself and, when it ends,
    /// leaves the outer poll's start as it found it; after the outer poll
    /// no start is left behind.
    #[test]
    fn test_a_nested_sliced_poll_restores_the_outer_start() {
        let mut cx = Context::from_waker(noop_waker_ref());
        let outer = sliced(async {
            let before = POLL_STARTED.get();
            assert!(before.is_some());
            let inner = sliced(async { POLL_STARTED.get() });
            let mut inner_cx = Context::from_waker(noop_waker_ref());
            let inner_start = Box::pin(inner).as_mut().poll(&mut inner_cx);
            assert!(matches!(inner_start, Poll::Ready(Some(start)) if Some(start) >= before));
            assert_eq!(POLL_STARTED.get(), before);
        });
        assert!(Box::pin(outer).as_mut().poll(&mut cx).is_ready());
        assert_eq!(POLL_STARTED.get(), None);
    }
}
