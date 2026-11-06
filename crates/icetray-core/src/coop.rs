//! A bound on how long one poll of a batch of lookups holds an async
//! worker. A warm lookup completes without awaiting anything that is not
//! ready, so a task driving many of them at once, as a `MultiGet` or
//! `MultiScan` run does, would run them all inside one poll: tens of
//! milliseconds for a few hundred keys. Tokio's own budget, 128 units a
//! poll, is spent only by its resources, and a warm lookup spends none.
//! Meanwhile the worker answers nothing else, not even a GET that has
//! completed, since it polls the I/O and timer drivers only between task
//! polls. So a run drives its lookups through `in_key_order`, whose every
//! poll is timed (`sliced`) and which yields between lookups once the poll
//! has run `LOOKUP_SLICE` (`yield_when_overdue`).

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::{FutureExt as _, StreamExt as _, stream};
use pin_project_lite::pin_project;

/// How long one poll of a sliced future runs lookups before it yields. A
/// yield costs the task a trip through the worker's queue and the worker a
/// poll of the drivers, a few microseconds, so a slice of several warm
/// lookups keeps the cost a small share, and a GET's completion waits at
/// most about a slice per task ahead of it. Under saturation, slices of
/// 100, 200 and 500 microseconds measured alike; what matters is that a
/// poll ends at all.
const LOOKUP_SLICE: Duration = Duration::from_micros(200);

thread_local! {
    /// When the current poll of the innermost sliced future on this thread
    /// began; `None` outside every one.
    static POLL_STARTED: Cell<Option<Instant>> = const { Cell::new(None) };
}

pin_project! {
    /// `inner`, with each of its polls timed for `yield_when_overdue`.
    struct Sliced<F> {
        #[pin]
        inner: F,
    }
}

/// `inner`, with each of its polls timed, so that `yield_when_overdue`
/// within it yields once a poll has run `LOOKUP_SLICE`.
fn sliced<F: Future>(inner: F) -> Sliced<F> {
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
/// yielding apart would resume in reverse order and take a batch's GET
/// permits tail first.
async fn yield_when_overdue() {
    let overdue = POLL_STARTED
        .get()
        .is_some_and(|started| started.elapsed() >= LOOKUP_SLICE);
    if overdue {
        tokio::task::yield_now().await;
    }
}

/// The answers of `lookups`, one per key, in key order: at most `at_once`
/// run at a time, started in key order and taken as each ends, so a
/// lookup waiting on a GET holds back neither the lookups after it nor the
/// yield. Each poll is sliced: once it has run `LOOKUP_SLICE`, the task
/// yields after the lookup that ends next, so its worker can answer a
/// completed GET before the batch's other warm lookups. A failing batch
/// fails with its first failing key's failure in key order, once every key
/// before it has passed, and ends there, dropping the lookups still
/// running (RFD 010).
pub(crate) async fn in_key_order<F, T, E>(lookups: Vec<F>, at_once: usize) -> Result<Vec<T>, E>
where
    F: Future<Output = Result<T, E>>,
{
    let keys = lookups.len();
    let numbered = lookups
        .into_iter()
        .enumerate()
        .map(|(at, lookup)| lookup.map(move |answer| (at, answer)));
    sliced(async move {
        let mut ended = stream::iter(numbered).buffer_unordered(at_once);
        let mut answers: Vec<Option<T>> = std::iter::repeat_with(|| None).take(keys).collect();
        // Keys before `passed` have all passed.
        let mut passed = 0;
        let mut first_failure: Option<(usize, E)> = None;
        while let Some((at, answer)) = ended.next().await {
            match answer {
                Ok(answer) => answers[at] = Some(answer),
                Err(error) => {
                    if first_failure.as_ref().is_none_or(|(first, _)| at < *first) {
                        first_failure = Some((at, error));
                    }
                }
            }
            while answers.get(passed).is_some_and(Option::is_some) {
                passed += 1;
            }
            if let Some((first, error)) = first_failure.take() {
                if first == passed {
                    return Err(error);
                }
                first_failure = Some((first, error));
            }
            yield_when_overdue().await;
        }
        Ok(answers.into_iter().flatten().collect())
    })
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::channel::oneshot;
    use futures::task::noop_waker_ref;

    use super::*;

    /// Busy-waits `for_at_least`, standing in for a lookup's decode.
    fn spin(for_at_least: Duration) {
        let started = Instant::now();
        while started.elapsed() < for_at_least {
            std::hint::spin_loop();
        }
    }

    /// A batch of lookups that each run a whole slice: each poll of
    /// `in_key_order` ends one of them, since the first spends the slice
    /// and the batch then yields, and the batch ends once each has had its
    /// poll, answering in key order; outside a sliced poll nothing yields.
    #[test]
    fn test_in_key_order_ends_a_poll_once_its_slice_is_spent() {
        const LOOKUPS: usize = 3;
        let done = Arc::new(AtomicUsize::new(0));
        let lookups = (0..LOOKUPS)
            .map(|at| {
                let done = Arc::clone(&done);
                async move {
                    spin(LOOKUP_SLICE);
                    done.fetch_add(1, Ordering::Relaxed);
                    Ok::<_, ()>(at)
                }
            })
            .collect();
        let mut cx = Context::from_waker(noop_waker_ref());
        let mut batch = Box::pin(in_key_order(lookups, LOOKUPS));
        let mut polls = 0;
        let answers = loop {
            let before = done.load(Ordering::Relaxed);
            let poll = batch.as_mut().poll(&mut cx);
            polls += 1;
            assert!(done.load(Ordering::Relaxed) - before <= 1, "poll {polls}");
            if let Poll::Ready(answers) = poll {
                break answers;
            }
        };
        assert_eq!(answers, Ok(vec![0, 1, 2]));
        assert!(polls >= LOOKUPS);
        assert!(yield_when_overdue().now_or_never().is_some(), "unsliced");
    }

    /// What a hand-answered lookup answers: its key's position, or a
    /// failure naming it.
    type Answer = Result<usize, &'static str>;

    /// Lookups whose answers each wait on a channel, and the senders that
    /// answer them.
    fn answered_by_hand(
        lookups: usize,
    ) -> (
        Vec<oneshot::Sender<Answer>>,
        Vec<impl Future<Output = Answer>>,
    ) {
        (0..lookups)
            .map(|_| {
                let (answer, answered) = oneshot::channel();
                (answer, async move { answered.await.unwrap() })
            })
            .unzip()
    }

    /// Lookups ending last key first are answered in key order.
    #[test]
    fn test_in_key_order_answers_in_key_order_though_lookups_end_in_reverse() {
        let (answers, lookups) = answered_by_hand(4);
        let mut cx = Context::from_waker(noop_waker_ref());
        let mut batch = Box::pin(in_key_order(lookups, 4));
        assert!(batch.as_mut().poll(&mut cx).is_pending());
        for (at, answer) in answers.into_iter().enumerate().rev() {
            answer.send(Ok(at)).unwrap();
        }
        assert_eq!(
            batch.as_mut().poll(&mut cx),
            Poll::Ready(Ok(vec![0, 1, 2, 3]))
        );
    }

    /// A batch whose later keys fail first fails with its first failing
    /// key's failure, and only once every key before that one has passed,
    /// without waiting on the keys after it.
    #[test]
    fn test_in_key_order_fails_with_the_first_failing_key_once_the_keys_before_it_pass() {
        let (mut answers, lookups) = answered_by_hand(4);
        let mut cx = Context::from_waker(noop_waker_ref());
        let mut batch = Box::pin(in_key_order(lookups, 4));
        assert!(batch.as_mut().poll(&mut cx).is_pending());
        let _fourth_unanswered = answers.pop();
        let [first, second, third] = <[_; 3]>::try_from(answers).unwrap();
        third.send(Err("third")).unwrap();
        assert!(
            batch.as_mut().poll(&mut cx).is_pending(),
            "keys before it unsettled"
        );
        second.send(Err("second")).unwrap();
        assert!(
            batch.as_mut().poll(&mut cx).is_pending(),
            "the first key unsettled"
        );
        first.send(Ok(0)).unwrap();
        assert_eq!(batch.as_mut().poll(&mut cx), Poll::Ready(Err("second")));
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
