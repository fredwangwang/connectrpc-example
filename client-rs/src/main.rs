use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, ValueEnum};
use connectrpc::Protocol;
use connectrpc::client::{ClientConfig, ClientTransport, HttpClient};

mod verbose;

pub mod proto {
    connectrpc::include_generated!();
}

use proto::sample::v1::{
    EchoRequest, Event, Notification, ReportTelemetryRequest, SampleServiceClient, SubscribeRequest,
};

#[derive(Clone, Copy, ValueEnum)]
enum Mode {
    /// Connect protocol over HTTP/1.1
    #[value(name = "http1.1")]
    Http1,
    /// Connect protocol over HTTP/2 (h2c, prior knowledge)
    #[value(name = "http2")]
    Http2,
    /// gRPC protocol over HTTP/2 (h2c, prior knowledge)
    #[value(name = "grpc")]
    Grpc,
}

#[derive(Clone, Copy, ValueEnum)]
enum Cmd {
    /// Unary call
    Echo,
    /// Unary call carrying a repeated batch of events
    Telemetry,
    /// Server stream of notifications
    Subscribe,
}

#[derive(Parser)]
struct Args {
    /// Server base URL
    #[arg(long, default_value = "http://localhost:8080")]
    addr: String,
    /// Wire protocol + HTTP version
    #[arg(long, value_enum, default_value = "http1.1")]
    mode: Mode,
    #[arg(long, value_enum, default_value = "echo")]
    cmd: Cmd,
    /// Log HTTP headers and complete request/response payloads to stderr
    #[arg(long, short)]
    verbose: bool,
    /// Message for echo
    #[arg(long, default_value = "hello")]
    message: String,
    /// Number of events in a telemetry batch
    #[arg(long, default_value_t = 5)]
    batch_size: usize,
    /// Telemetry batch id (reuse to test server dedupe)
    #[arg(long, default_value = "batch-1")]
    batch_id: String,
    /// Subscribe: resume after this notification id
    #[arg(long, default_value_t = 0)]
    last_seen_id: u64,
    /// Subscribe: stop after this many notifications (0 = until server ends)
    #[arg(long, default_value_t = 8)]
    max: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let (transport, protocol) = match args.mode {
        Mode::Http1 => (HttpClient::plaintext(), Protocol::Connect),
        Mode::Http2 => (HttpClient::plaintext_http2_only(), Protocol::Connect),
        Mode::Grpc => (HttpClient::plaintext_http2_only(), Protocol::Grpc),
    };
    let config = ClientConfig::new(args.addr.parse()?).with_protocol(protocol);
    if args.verbose {
        run(SampleServiceClient::new(verbose::Verbose(transport), config), args).await
    } else {
        run(SampleServiceClient::new(transport, config), args).await
    }
}

async fn run<T>(client: SampleServiceClient<T>, args: Args) -> Result<(), Box<dyn std::error::Error>>
where
    T: ClientTransport,
    T::ResponseBody: Unpin,
    <T::ResponseBody as http_body::Body>::Error: std::fmt::Display,
{
    match args.cmd {
        Cmd::Echo => {
            let resp = client
                .echo(EchoRequest {
                    message: args.message,
                    ..Default::default()
                })
                .await?
                .into_owned();
            println!("echo: {:?} (server saw {})", resp.message, resp.http_version);
        }
        Cmd::Telemetry => {
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
            let events = (0..args.batch_size)
                .map(|i| Event {
                    name: "cpu".into(),
                    value: i as f64,
                    timestamp_unix_ms: now + i as i64,
                    ..Default::default()
                })
                .collect();
            let resp = client
                .report_telemetry(ReportTelemetryRequest {
                    batch_id: args.batch_id,
                    events,
                    ..Default::default()
                })
                .await?
                .into_owned();
            println!(
                "telemetry: accepted={} duplicate={} (server saw {})",
                resp.accepted, resp.duplicate, resp.http_version
            );
        }
        Cmd::Subscribe => {
            let mut stream = client
                .subscribe(SubscribeRequest {
                    last_seen_id: args.last_seen_id,
                    max_notifications: args.max,
                    ..Default::default()
                })
                .await?;
            while let Some(msg) = stream.message::<Notification>().await? {
                let n = msg.view();
                if n.heartbeat {
                    println!("[{}] heartbeat ({})", n.id, n.http_version);
                } else {
                    println!("[{}] {} ({})", n.id, n.text, n.http_version);
                }
            }
            println!("stream ended");
        }
    }
    Ok(())
}
