//! `requests_total` counted around the gRPC service rather than in its
//! handlers (RFD 012): `Counted` wraps the generated server, so a request
//! tonic answers before any handler runs (a body that does not decode or
//! is too large, an encoding the server does not take) is counted too,
//! each request exactly once, under the `grpc-status` its client receives.
//! A trailers-only answer carries the status in its headers and is counted
//! as they leave; any other answer carries it in its trailers and is
//! counted as its body yields them. A request whose body fails to read is
//! counted as `CANCELLED`, whatever it is answered: the failure is its
//! connection's, so its client is gone and receives nothing.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, ready};

use hyper::HeaderMap;
use hyper::body::{Body, Frame, SizeHint};
use tonic::Code;
use tonic::server::NamedService;

use crate::telemetry::{Call, Requests};

/// The header, or trailer, a gRPC answer carries its status code in.
const GRPC_STATUS: &str = "grpc-status";

/// The status code `headers` carry, a response's or its trailers'; one
/// that does not parse is `UNKNOWN`, as a client reads it.
fn grpc_status(headers: &HeaderMap) -> Option<Code> {
    headers
        .get(GRPC_STATUS)
        .map(|value| Code::from_bytes(value.as_bytes()))
}

/// One request being answered, counted in `requests` once: under the code
/// `finish` is given, or as `CANCELLED` when it drops first, which is how
/// a request ends whose client went away or whose `grpc-timeout` passed,
/// when tonic drops the call and answers `CANCELLED` itself. Also
/// `CANCELLED` when `failed` is set, whatever it was answered: its body
/// failed to read (`RequestBody`), and tonic answers that failure with the
/// code it gives the transport's error, `UNKNOWN` for a connection that
/// closed, which no client receives.
#[derive(Debug)]
struct Answering {
    requests: Requests,
    call: Call,
    code: Code,
    failed: Arc<AtomicBool>,
}

impl Answering {
    /// A request of `call`, not yet answered, to count in `requests`, its
    /// body's failure to read to be recorded in `failed`.
    fn new(requests: &Requests, call: Call, failed: Arc<AtomicBool>) -> Self {
        Self {
            requests: requests.clone(),
            call,
            code: Code::Cancelled,
            failed,
        }
    }

    /// Counts the request under `code`.
    fn finish(mut self, code: Code) {
        self.code = code;
    }
}

impl Drop for Answering {
    fn drop(&mut self) {
        let code = if self.failed.load(Ordering::Acquire) {
            Code::Cancelled
        } else {
            self.code
        };
        self.requests.count(self.call, code);
    }
}

/// The gRPC service `inner` with every request of a `Call` counted in
/// `requests` under the status its client receives. Clone: tonic clones the
/// service per connection, and the clones share the handles.
#[derive(Clone, Debug)]
pub struct Counted<S> {
    inner: S,
    requests: Requests,
}

impl<S> Counted<S> {
    /// `inner`, counted in `requests`.
    pub fn new(inner: S, requests: Requests) -> Self {
        Self { inner, requests }
    }
}

impl<S: NamedService> NamedService for Counted<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, B, R> tower::Service<hyper::Request<B>> for Counted<S>
where
    S: tower::Service<hyper::Request<RequestBody<B>>, Response = hyper::Response<R>>,
    S::Future: Unpin,
{
    type Response = hyper::Response<CountedBody<R>>;
    type Error = S::Error;
    type Future = CountedFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: hyper::Request<B>) -> Self::Future {
        let call = Call::of_path(request.uri().path());
        let failed = call.map(|_| Arc::new(AtomicBool::new(false)));
        let answering = call
            .zip(failed.clone())
            .map(|(call, failed)| Answering::new(&self.requests, call, failed));
        CountedFuture {
            inner: self
                .inner
                .call(request.map(|inner| RequestBody { inner, failed })),
            answering,
        }
    }
}

/// A counted request's body, handed to the service as it came, which sets
/// `failed` when a read of it fails. Every error a request body yields is
/// its connection's: a stream the client reset, a connection that closed,
/// or a frame the client sent that broke the protocol. `None` for a
/// request no call counts.
#[derive(Debug)]
pub struct RequestBody<B> {
    inner: B,
    failed: Option<Arc<AtomicBool>>,
}

impl<B: Body + Unpin> Body for RequestBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let polled = Pin::new(&mut self.inner).poll_frame(cx);
        if let (Poll::Ready(Some(Err(_))), Some(failed)) = (&polled, &self.failed) {
            failed.store(true, Ordering::Release);
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// The response of a counted request: counted when it carries its status
/// in its headers, else handed on with its body to be counted at the
/// trailers. A future dropped before it answers counts as `CANCELLED`.
#[derive(Debug)]
pub struct CountedFuture<F> {
    inner: F,
    answering: Option<Answering>,
}

impl<F, R, E> Future for CountedFuture<F>
where
    F: Future<Output = Result<hyper::Response<R>, E>> + Unpin,
{
    type Output = Result<hyper::Response<CountedBody<R>>, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let response = ready!(Pin::new(&mut this.inner).poll(cx))?;
        let mut answering = this.answering.take();
        if let Some(code) = grpc_status(response.headers())
            && let Some(answered) = answering.take()
        {
            answered.finish(code);
        }
        Poll::Ready(Ok(response.map(|inner| CountedBody { inner, answering })))
    }
}

/// A counted response's body: the request is counted under the status its
/// trailers carry as they pass, or as `UNKNOWN` when the body ends without
/// them, since a client then has no status to read. A body that fails or
/// is dropped first counts as `CANCELLED`: its stream is reset.
#[derive(Debug)]
pub struct CountedBody<B> {
    inner: B,
    answering: Option<Answering>,
}

impl<B> CountedBody<B> {
    /// Counts the request under `code`, unless it is counted already.
    fn finish(&mut self, code: Code) {
        if let Some(answering) = self.answering.take() {
            answering.finish(code);
        }
    }
}

impl<B: Body + Unpin> Body for CountedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = &mut *self;
        let polled = Pin::new(&mut this.inner).poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    this.finish(grpc_status(trailers).unwrap_or(Code::Unknown));
                }
            }
            Poll::Ready(None) => this.finish(Code::Unknown),
            Poll::Ready(Some(Err(_))) | Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::convert::Infallible;
    use std::future::{Ready, ready};

    use bytes::Bytes;
    use futures::FutureExt as _;
    use http_body_util::BodyExt as _;
    use hyper::header::HeaderValue;
    use icetray_core::metric;
    use icetray_testkit::verify::{series_sum, series_value};
    use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
    use tower::Service as _;

    use super::*;
    use crate::telemetry::CodeLabel;

    /// A body that yields `frames` in order and then ends, or, when
    /// `fails`, fails as a body read from a closed connection does.
    #[derive(Debug, Default)]
    struct Frames {
        frames: VecDeque<Frame<Bytes>>,
        fails: bool,
    }

    impl Frames {
        /// A body that yields `frames` and then ends.
        fn of(frames: impl IntoIterator<Item = Frame<Bytes>>) -> Self {
            Self {
                frames: frames.into_iter().collect(),
                fails: false,
            }
        }
    }

    impl Body for Frames {
        type Data = Bytes;
        type Error = tonic::Status;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, tonic::Status>>> {
            match self.frames.pop_front() {
                Some(frame) => Poll::Ready(Some(Ok(frame))),
                None if self.fails => {
                    self.fails = false;
                    Poll::Ready(Some(Err(tonic::Status::unknown(
                        "h2 protocol error: error reading a body from connection",
                    ))))
                }
                None => Poll::Ready(None),
            }
        }
    }

    /// A service that reads each request's body until it ends or fails, as
    /// tonic decodes a request, and answers with a response `answer`
    /// builds.
    #[derive(Clone)]
    struct Answers(fn() -> hyper::Response<Frames>);

    impl<B: Body + Unpin> tower::Service<hyper::Request<B>> for Answers {
        type Response = hyper::Response<Frames>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Infallible>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: hyper::Request<B>) -> Self::Future {
            let mut body = request.into_body();
            while let Some(Ok(_)) = body.frame().now_or_never().flatten() {}
            ready(Ok((self.0)()))
        }
    }

    /// Headers carrying `code` as a gRPC status.
    fn status(code: Code) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(GRPC_STATUS, HeaderValue::from(i32::from(code)));
        headers
    }

    /// A trailers-only answer: the status in the headers, no body.
    fn trailers_only() -> hyper::Response<Frames> {
        let mut response = hyper::Response::new(Frames::default());
        *response.headers_mut() = status(Code::Internal);
        response
    }

    /// An answer with a message and then trailers carrying `OK`.
    fn with_trailers() -> hyper::Response<Frames> {
        hyper::Response::new(Frames::of([
            Frame::data(Bytes::from_static(b"message")),
            Frame::trailers(status(Code::Ok)),
        ]))
    }

    /// An answer whose body ends with no trailers.
    fn without_trailers() -> hyper::Response<Frames> {
        hyper::Response::new(Frames::of([Frame::data(Bytes::from_static(b"message"))]))
    }

    /// `service` behind `Counted`, over handles registered with a recorder
    /// of the test's own, so no other test's counts reach it.
    fn counted<S>(service: S) -> (Counted<S>, PrometheusHandle) {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        let requests = metrics::with_local_recorder(&recorder, Requests::register);
        (Counted::new(service, requests), handle)
    }

    /// A request of `call` whose body is `body`.
    fn request(call: Call, body: Frames) -> hyper::Request<Frames> {
        hyper::Request::builder()
            .uri(format!(
                "/{}/{}",
                icetray_proto::v1::icetray_server::SERVICE_NAME,
                call.method()
            ))
            .body(body)
            .unwrap()
    }

    /// The count of `call` answering `code`.
    fn count(handle: &PrometheusHandle, call: Call, code: Code) -> f64 {
        series_value(
            &handle.render(),
            metric::REQUESTS_TOTAL,
            &[
                (metric::LABEL_CALL, &call.to_string()),
                (metric::LABEL_CODE, &CodeLabel(code).to_string()),
            ],
        )
        .expect("every series is registered")
    }

    /// Every request counted, whatever its call and code.
    fn counted_total(handle: &PrometheusHandle) -> f64 {
        series_sum(&handle.render(), metric::REQUESTS_TOTAL, &[])
    }

    /// A status in the headers is counted as the response leaves, before
    /// its body is read.
    #[test]
    fn test_call_counts_a_trailers_only_answer_at_its_headers() {
        let (mut service, handle) = counted(Answers(trailers_only));
        let response = service
            .call(request(Call::Get, Frames::default()))
            .now_or_never()
            .unwrap();
        assert_eq!(
            count(&handle, Call::Get, Code::Internal).to_bits(),
            1.0_f64.to_bits()
        );
        drop(response);
        assert_eq!(counted_total(&handle).to_bits(), 1.0_f64.to_bits());
    }

    /// A status in the trailers is counted as the body yields them, once.
    #[test]
    fn test_call_counts_an_answer_at_its_trailers() {
        let (mut service, handle) = counted(Answers(with_trailers));
        let mut body = service
            .call(request(Call::MultiGet, Frames::default()))
            .now_or_never()
            .unwrap()
            .unwrap()
            .into_body();
        assert_eq!(counted_total(&handle).to_bits(), 0.0_f64.to_bits());
        let data = body.frame().now_or_never().flatten().unwrap().unwrap();
        assert!(data.is_data());
        assert_eq!(counted_total(&handle).to_bits(), 0.0_f64.to_bits());
        let trailers = body.frame().now_or_never().flatten().unwrap().unwrap();
        assert!(trailers.is_trailers());
        assert_eq!(
            count(&handle, Call::MultiGet, Code::Ok).to_bits(),
            1.0_f64.to_bits()
        );
        assert!(body.frame().now_or_never().flatten().is_none());
        drop(body);
        assert_eq!(counted_total(&handle).to_bits(), 1.0_f64.to_bits());
    }

    /// A request that ends before its status reaches the client is counted
    /// as `CANCELLED`, the future dropped unanswered or the body dropped
    /// before its trailers; a body that ends without trailers is `UNKNOWN`.
    #[test]
    fn test_call_counts_a_request_that_ends_without_a_status() {
        let (mut service, handle) = counted(Answers(with_trailers));
        drop(service.call(request(Call::Get, Frames::default())));
        assert_eq!(
            count(&handle, Call::Get, Code::Cancelled).to_bits(),
            1.0_f64.to_bits()
        );
        let response = service
            .call(request(Call::Get, Frames::default()))
            .now_or_never()
            .unwrap();
        drop(response);
        assert_eq!(
            count(&handle, Call::Get, Code::Cancelled).to_bits(),
            2.0_f64.to_bits()
        );

        let (mut service, handle) = counted(Answers(without_trailers));
        let mut body = service
            .call(request(Call::TableInfo, Frames::default()))
            .now_or_never()
            .unwrap()
            .unwrap()
            .into_body();
        while body.frame().now_or_never().flatten().is_some() {}
        assert_eq!(
            count(&handle, Call::TableInfo, Code::Unknown).to_bits(),
            1.0_f64.to_bits()
        );
        drop(body);
        assert_eq!(counted_total(&handle).to_bits(), 1.0_f64.to_bits());
    }

    /// A request whose body fails to read from its connection is counted
    /// as `CANCELLED`, whatever the service answers, since its client went
    /// away before it finished sending; a request read whole is counted
    /// under the code it is answered.
    #[test]
    fn test_call_counts_a_request_whose_body_fails_as_cancelled() {
        let (mut service, handle) = counted(Answers(trailers_only));
        let keys = || Frames::of([Frame::data(Bytes::from_static(b"keys"))]);
        let failing = Frames {
            fails: true,
            ..keys()
        };
        let whole = keys();

        drop(
            service
                .call(request(Call::MultiGet, failing))
                .now_or_never()
                .unwrap(),
        );
        assert_eq!(
            count(&handle, Call::MultiGet, Code::Cancelled).to_bits(),
            1.0_f64.to_bits()
        );
        assert_eq!(
            count(&handle, Call::MultiGet, Code::Internal).to_bits(),
            0.0_f64.to_bits()
        );

        drop(
            service
                .call(request(Call::MultiGet, whole))
                .now_or_never()
                .unwrap(),
        );
        assert_eq!(
            count(&handle, Call::MultiGet, Code::Internal).to_bits(),
            1.0_f64.to_bits()
        );
        assert_eq!(counted_total(&handle).to_bits(), 2.0_f64.to_bits());
    }

    /// A request for a path that is no call is answered and not counted.
    #[test]
    fn test_call_counts_no_request_outside_the_contract_s_calls() {
        let (mut service, handle) = counted(Answers(trailers_only));
        let other = hyper::Request::builder()
            .uri("/grpc.health.v1.Health/Check")
            .body(Frames::default())
            .unwrap();
        let response = service.call(other).now_or_never().unwrap();
        drop(response);
        assert_eq!(counted_total(&handle).to_bits(), 0.0_f64.to_bits());
    }
}
