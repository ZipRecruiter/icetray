//! Deterministic simulation (RFD 014): the real `object_store` S3 client and
//! the real `s3s` server with every byte crossing `turmoil`'s simulated
//! network, so latency is injected per link and virtual time shows how many
//! round trips a lookup costs. GET and hedge counts and the timings under a
//! fixed link latency are the same on every run of one seed. Under a latency
//! range they are not: the simulated host serves the fixture with `s3s-fs`
//! over blocking file reads, so concurrent reads complete in wall-clock
//! order and take the seeded latency draws in that order, and a test under
//! varying latency asserts bounds, never equal timings.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use http_body_util::BodyExt as _;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Uri};
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::{TokioExecutor, TokioIo};
use object_store::ClientOptions;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpErrorKind, HttpRequest, HttpRequestBody,
    HttpResponse, HttpResponseBody, HttpService,
};
use pin_project_lite::pin_project;
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use s3s_fs::FileSystem;
use tower::Service;
use turmoil::net::{TcpListener, TcpStream};

use crate::error::{Result, TestkitError};
use crate::s3::{ACCESS_KEY, REGION, SECRET_KEY};

/// The port the simulated S3 host listens on: the one `MinIO` serves on,
/// so a logged endpoint reads as an S3 one; nothing else binds a port in
/// the simulation.
pub const SIM_S3_PORT: u16 = 9000;

/// The address the simulated host binds: every interface, `SIM_S3_PORT`.
const ANY_INTERFACE: &str = "0.0.0.0";

/// A deterministic long tail: every `every`-th request to the simulated S3
/// host, counted across connections from server start, sleeps `delay`
/// before it is served. An `every` of zero stalls nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SlowEvery {
    /// The period, in requests.
    pub every: u32,
    /// The stall.
    pub delay: Duration,
}

impl SlowEvery {
    /// Whether request number `n` (from zero) stalls.
    fn stalls(self, n: u32) -> bool {
        n.checked_rem(self.every)
            .is_some_and(|remainder| remainder + 1 == self.every)
    }
}

/// Serves `s3s-fs` rooted at `root` on the simulated host this runs on.
/// Never returns; run it as a `turmoil` host.
pub async fn serve_s3(root: PathBuf) -> turmoil::Result {
    serve_s3_with(root, None).await
}

/// `serve_s3` with an injected tail.
pub async fn serve_s3_with(root: PathBuf, slow: Option<SlowEvery>) -> turmoil::Result {
    let filesystem = FileSystem::new(&root).map_err(TestkitError::from)?;
    let service = {
        let mut builder = S3ServiceBuilder::new(filesystem);
        builder.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));
        Arc::new(builder.build())
    };
    let requests = Arc::new(AtomicU32::new(0));
    let listener = TcpListener::bind((ANY_INTERFACE, SIM_S3_PORT)).await?;
    loop {
        let (stream, _peer) = listener.accept().await?;
        let service = Arc::clone(&service);
        let requests = Arc::clone(&requests);
        tokio::task::spawn_local(async move {
            let handler = service_fn(move |request: Request<Incoming>| {
                let n = requests.fetch_add(1, Ordering::SeqCst);
                let service = Arc::clone(&service);
                async move {
                    if let Some(slow) = slow
                        && slow.stalls(n)
                    {
                        tokio::time::sleep(slow.delay).await;
                    }
                    // `S3Service` also has an inherent `call` over another body
                    // type; the trait method is the one hyper's body fits.
                    hyper::service::Service::call(service.as_ref(), request).await
                }
            });
            let connection = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), handler);
            if let Err(error) = connection.await {
                // A client closing its pooled connection ends it with an error
                // that is nobody's failure.
                tracing::debug!(%error, "simulated s3 connection ended");
            }
        });
    }
}

/// An `AmazonS3` client for `bucket` whose HTTP crosses the simulated
/// network to `http://<host>:SIM_S3_PORT`, signed with the fixture keys.
pub fn sim_s3_store(host: &str, bucket: &str) -> Result<AmazonS3> {
    Ok(AmazonS3Builder::new()
        .with_endpoint(format!("http://{host}:{SIM_S3_PORT}"))
        .with_bucket_name(bucket)
        .with_region(REGION)
        .with_access_key_id(ACCESS_KEY)
        .with_secret_access_key(SECRET_KEY)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_http_connector(TurmoilHttpConnector)
        .build()?)
}

type ConnectFuture = Pin<Box<dyn Future<Output = std::io::Result<TurmoilConnection>> + Send>>;

/// The `tower::Service<Uri>` that opens `turmoil` TCP connections for the
/// hyper client.
#[derive(Clone, Copy, Debug, Default)]
struct TurmoilConnect;

impl Service<Uri> for TurmoilConnect {
    type Response = TurmoilConnection;
    type Error = std::io::Error;
    type Future = ConnectFuture;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        Box::pin(async move {
            let authority = uri
                .authority()
                .ok_or_else(|| std::io::Error::other("uri has no authority"))?
                .as_str()
                .to_string();
            let io = TokioIo::new(TcpStream::connect(authority).await?);
            Ok(TurmoilConnection { io })
        })
    }
}

pin_project! {
    /// `turmoil::net::TcpStream` behind hyper's own I/O traits, through
    /// hyper-util's `TokioIo`; a type of this crate's own, so it can
    /// implement hyper-util's `Connection`.
    struct TurmoilConnection {
        #[pin]
        io: TokioIo<TcpStream>,
    }
}

impl hyper::rt::Read for TurmoilConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        hyper::rt::Read::poll_read(self.project().io, cx, buf)
    }
}

impl hyper::rt::Write for TurmoilConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        hyper::rt::Write::poll_write(self.project().io, cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        hyper::rt::Write::poll_flush(self.project().io, cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        hyper::rt::Write::poll_shutdown(self.project().io, cx)
    }
}

impl Connection for TurmoilConnection {
    fn connected(&self) -> Connected {
        Connected::new()
    }
}

/// `object_store`'s transport trait over a hyper client that connects
/// through `turmoil`. Transport errors are `HttpErrorKind::Unknown`: nothing
/// the simulation tests measure needs a finer class.
#[derive(Clone, Debug)]
struct TurmoilHttpService {
    client: HyperClient<TurmoilConnect, HttpRequestBody>,
}

#[async_trait::async_trait]
impl HttpService for TurmoilHttpService {
    async fn call(&self, request: HttpRequest) -> std::result::Result<HttpResponse, HttpError> {
        let response = self
            .client
            .request(request)
            .await
            .map_err(|error| HttpError::new(HttpErrorKind::Unknown, error))?;
        let (parts, body) = response.into_parts();
        let body = HttpResponseBody::new(
            body.map_err(|error| HttpError::new(HttpErrorKind::Unknown, error)),
        );
        Ok(HttpResponse::from_parts(parts, body))
    }
}

/// The factory `AmazonS3Builder::with_http_connector` takes.
#[derive(Clone, Copy, Debug, Default)]
struct TurmoilHttpConnector;

impl HttpConnector for TurmoilHttpConnector {
    fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
        let client = HyperClient::builder(TokioExecutor::new()).build(TurmoilConnect);
        Ok(HttpClient::new(TurmoilHttpService { client }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The period of the sample tail.
    const EVERY: u32 = 5;

    /// A stall for the sample tail.
    const DELAY: Duration = Duration::from_millis(300);

    #[test]
    fn test_slow_every_stalls_every_nth_request_from_the_first_period() {
        let slow = SlowEvery {
            every: EVERY,
            delay: DELAY,
        };
        let stalled: Vec<u32> = (0..3 * EVERY).filter(|&n| slow.stalls(n)).collect();
        assert_eq!(stalled, vec![EVERY - 1, 2 * EVERY - 1, 3 * EVERY - 1]);
        let never = SlowEvery {
            every: 0,
            delay: DELAY,
        };
        assert!(!(0..3 * EVERY).any(|n| never.stalls(n)));
        let always = SlowEvery {
            every: 1,
            delay: DELAY,
        };
        assert!((0..3 * EVERY).all(|n| always.stalls(n)));
    }
}
