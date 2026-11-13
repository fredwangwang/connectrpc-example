//! `--verbose` support: a `ClientTransport` wrapper that logs every HTTP
//! request/response (headers, trailers, and complete bodies) to stderr.
//!
//! It sits at the HTTP layer, so it works the same for Connect over HTTP/1.1,
//! Connect over HTTP/2 and gRPC. It shows HTTP *messages*; HTTP/2 frames
//! (HEADERS/DATA/SETTINGS, HPACK) are below hyper and not visible here.
//! Bodies are shown as hex plus the decoded protobuf message. Enveloped
//! bodies (Connect streaming, gRPC) are split into their 5-byte-header
//! envelopes first. gzip-compressed payloads are decompressed for display.

use std::io::Read;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use connectrpc::client::{ClientBody, ClientTransport};
use futures_util::future::BoxFuture;
use http::{HeaderMap, Request, Response};
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt;

use crate::proto::sample::v1::{
    EchoRequest, EchoResponse, Notification, ReportTelemetryRequest, ReportTelemetryResponse,
    SubscribeRequest,
};

#[derive(Clone)]
pub struct Verbose<T>(pub T);

impl<T> ClientTransport for Verbose<T>
where
    T: ClientTransport,
    T::ResponseBody: Unpin,
{
    type ResponseBody = Logged<T::ResponseBody>;
    type Error = T::Error;

    fn send(
        &self,
        request: Request<ClientBody>,
    ) -> BoxFuture<'static, Result<Response<Self::ResponseBody>, Self::Error>> {
        let (parts, body) = request.into_parts();
        let path = parts.uri.path().to_string();
        eprintln!("> {} {}", parts.method, parts.uri);
        print_headers('>', &parts.headers);
        let body = Logged::new(body, Decoder::new(&path, true, &parts.headers)).boxed();
        let fut = self.0.send(Request::from_parts(parts, body));
        Box::pin(async move {
            let resp = fut.await?;
            let (parts, body) = resp.into_parts();
            eprintln!("< {:?} {}", parts.version, parts.status);
            print_headers('<', &parts.headers);
            let body = Logged::new(body, Decoder::new(&path, false, &parts.headers));
            Ok(Response::from_parts(parts, body))
        })
    }
}

fn print_headers(arrow: char, headers: &HeaderMap) {
    for (k, v) in headers {
        eprintln!("{arrow} {k}: {}", v.to_str().unwrap_or("<binary>"));
    }
}

/// Body wrapper that reports frames to a [`Decoder`] as they pass through.
pub struct Logged<B> {
    inner: B,
    dec: Decoder,
    ended: bool,
}

impl<B> Logged<B> {
    fn new(inner: B, dec: Decoder) -> Self {
        Self { inner, dec, ended: false }
    }
}

impl<B> Logged<B> {
    fn finish(&mut self) {
        if !self.ended {
            self.ended = true;
            self.dec.on_end();
        }
    }
}

impl<B: Body<Data = Bytes> + Unpin> Body for Logged<B> {
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = &mut *self;
        let r = Pin::new(&mut this.inner).poll_frame(cx);
        match &r {
            Poll::Ready(Some(Ok(f))) => this.dec.on_frame(f),
            Poll::Ready(None) => this.finish(),
            _ => {}
        }
        // hyper may stop polling once is_end_stream() is true, so the final
        // `None` is not guaranteed to arrive; finish on the last frame too.
        if matches!(r, Poll::Ready(Some(Ok(_)))) && this.inner.is_end_stream() {
            this.finish();
        }
        r
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Method {
    Echo,
    ReportTelemetry,
    Subscribe,
    Unknown,
}

struct Decoder {
    arrow: char,
    method: Method,
    is_req: bool,
    json: bool,
    enveloped: bool,
    gzip: bool,
    buf: BytesMut,
}

impl Decoder {
    fn new(path: &str, is_req: bool, headers: &HeaderMap) -> Self {
        let ct = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let method = match path.rsplit('/').next() {
            Some("Echo") => Method::Echo,
            Some("ReportTelemetry") => Method::ReportTelemetry,
            Some("Subscribe") => Method::Subscribe,
            _ => Method::Unknown,
        };
        let gzip = ["content-encoding", "connect-content-encoding", "grpc-encoding"]
            .iter()
            .any(|h| headers.get(*h).and_then(|v| v.to_str().ok()) == Some("gzip"));
        Self {
            arrow: if is_req { '>' } else { '<' },
            method,
            is_req,
            json: ct.contains("json"),
            enveloped: ct.contains("connect+") || ct.contains("grpc"),
            gzip,
            buf: BytesMut::new(),
        }
    }

    fn on_frame(&mut self, f: &Frame<Bytes>) {
        if let Some(data) = f.data_ref() {
            self.buf.extend_from_slice(data);
            if self.enveloped {
                self.drain_envelopes();
            }
        } else if let Some(t) = f.trailers_ref() {
            eprintln!("{} trailers:", self.arrow);
            print_headers(self.arrow, t);
        }
    }

    fn on_end(&mut self) {
        if self.enveloped {
            if !self.buf.is_empty() {
                eprintln!("{} leftover {} bytes (partial envelope)", self.arrow, self.buf.len());
            }
        } else if !self.buf.is_empty() {
            let b = self.buf.split().freeze();
            let shown = self.render(&b, self.gzip);
            eprintln!("{} body ({} bytes): {}", self.arrow, b.len(), shown);
        }
    }

    /// Envelope = 1 flag byte (0x01 compressed, 0x02 end-of-stream/trailers)
    /// + 4-byte big-endian length + payload.
    fn drain_envelopes(&mut self) {
        while self.buf.len() >= 5 {
            let len = u32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]) as usize;
            if self.buf.len() < 5 + len {
                return;
            }
            let env = self.buf.split_to(5 + len).freeze();
            let flags = env[0];
            let payload = &env[5..];
            let shown = if flags & 0x02 != 0 {
                // Connect end-of-stream message (JSON). gRPC never sends this
                // flag: its trailers are HTTP/2 trailers, shown separately.
                match self.inflate(payload, flags & 0x01 != 0) {
                    Ok(b) => String::from_utf8_lossy(&b).into_owned(),
                    Err(e) => e,
                }
            } else {
                self.render(payload, flags & 0x01 != 0)
            };
            eprintln!(
                "{} envelope flags=0x{flags:02x} len={len}: {shown}",
                self.arrow
            );
        }
    }

    fn inflate(&self, payload: &[u8], compressed: bool) -> Result<Vec<u8>, String> {
        if !compressed {
            return Ok(payload.to_vec());
        }
        if !self.gzip {
            return Err(format!("<compressed with unsupported codec> {}", hex(payload)));
        }
        let mut out = Vec::new();
        flate2::read::GzDecoder::new(payload)
            .read_to_end(&mut out)
            .map_err(|e| format!("<gzip error: {e}> {}", hex(payload)))?;
        Ok(out)
    }

    fn render(&self, payload: &[u8], compressed: bool) -> String {
        let data = match self.inflate(payload, compressed) {
            Ok(d) => d,
            Err(e) => return e,
        };
        let note = if compressed {
            format!("gzip {}B -> {}B; ", payload.len(), data.len())
        } else {
            String::new()
        };
        if self.json {
            return format!("{note}{}", String::from_utf8_lossy(&data));
        }
        format!("{note}{}  =>  {}", hex(&data), self.decode(&data))
    }

    fn decode(&self, p: &[u8]) -> String {
        use buffa::Message;
        macro_rules! dec {
            ($t:ty) => {
                match <$t>::decode_from_slice(p) {
                    Ok(m) => format!("{m:?}"),
                    Err(e) => format!("<decode error: {e}>"),
                }
            };
        }
        match (self.method, self.is_req) {
            (Method::Echo, true) => dec!(EchoRequest),
            (Method::Echo, false) => dec!(EchoResponse),
            (Method::ReportTelemetry, true) => dec!(ReportTelemetryRequest),
            (Method::ReportTelemetry, false) => dec!(ReportTelemetryResponse),
            (Method::Subscribe, true) => dec!(SubscribeRequest),
            (Method::Subscribe, false) => dec!(Notification),
            (Method::Unknown, _) => "<unknown method>".into(),
        }
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
