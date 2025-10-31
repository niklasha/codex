#![deny(clippy::print_stdout, clippy::print_stderr)]

use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use codex_app_server_protocol::JSONRPCMessage;
use codex_common::CliConfigOverrides;
use codex_core::config::Config;
use codex_core::config::ConfigOverrides;
use codex_feedback::CodexFeedback;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::{self};
use tokio::sync::mpsc;
use tracing::Level;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::message_processor::MessageProcessor;
use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::OutgoingMessageSender;

mod codex_message_processor;
mod error_code;
mod fuzzy_file_search;
mod message_processor;
mod models;
mod outgoing_message;
mod websocket;

/// Subprotocol name advertised by the WebSocket transport.
pub const WEBSOCKET_SUBPROTOCOL: &str = "codex.app-server.v1";
/// Default HTTP path used by the WebSocket listener.
pub const DEFAULT_WEBSOCKET_PATH: &str = "/app-server/v1";
/// Helper notification emitted immediately after a WebSocket upgrade completes.
pub(crate) const SERVER_HELLO_METHOD: &str = "server/hello";
/// Size of the bounded channels used to communicate between tasks. The value
/// is a balance between throughput and memory usage – 128 messages should be
/// plenty for an interactive CLI.
pub(crate) const CHANNEL_CAPACITY: usize = 128;

#[derive(Clone)]
struct SharedState {
    codex_linux_sandbox_exe: Option<PathBuf>,
    config: Arc<Config>,
    feedback: CodexFeedback,
}

impl SharedState {
    fn new(
        codex_linux_sandbox_exe: Option<PathBuf>,
        config: Arc<Config>,
        feedback: CodexFeedback,
    ) -> Self {
        Self {
            codex_linux_sandbox_exe,
            config,
            feedback,
        }
    }
}

#[derive(Clone)]
pub struct WebsocketOptions {
    pub bind_addr: SocketAddr,
    pub path: String,
}

impl WebsocketOptions {
    pub fn new(bind_addr: SocketAddr, path: impl Into<String>) -> Self {
        let mut path = path.into();
        if path.is_empty() {
            path = DEFAULT_WEBSOCKET_PATH.to_string();
        } else if !path.starts_with('/') {
            path = format!("/{path}");
        }
        Self { bind_addr, path }
    }
}

#[derive(Clone)]
pub struct ServerOptions {
    pub websocket: Option<WebsocketOptions>,
    pub stdio_enabled: bool,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            websocket: None,
            stdio_enabled: true,
        }
    }
}

pub async fn run_main(
    codex_linux_sandbox_exe: Option<PathBuf>,
    cli_config_overrides: CliConfigOverrides,
    server_options: ServerOptions,
) -> IoResult<()> {
    // Parse CLI overrides once and derive the base Config eagerly so later
    // components do not need to work with raw TOML values.
    let cli_kv_overrides = cli_config_overrides.parse_overrides().map_err(|e| {
        std::io::Error::new(
            ErrorKind::InvalidInput,
            format!("error parsing -c overrides: {e}"),
        )
    })?;
    let config = Arc::new(
        Config::load_with_cli_overrides(cli_kv_overrides, ConfigOverrides::default())
            .await
            .map_err(|e| {
                std::io::Error::new(ErrorKind::InvalidData, format!("error loading config: {e}"))
            })?,
    );

    let feedback = CodexFeedback::new();

    let otel =
        codex_core::otel_init::build_provider(&config, env!("CARGO_PKG_VERSION")).map_err(|e| {
            std::io::Error::new(
                ErrorKind::InvalidData,
                format!("error loading otel config: {e}"),
            )
        })?;

    let stderr_fmt = tracing_subscriber::fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(EnvFilter::from_default_env());

    let feedback_layer = tracing_subscriber::fmt::layer()
        .with_writer(feedback.make_writer())
        .with_ansi(false)
        .with_target(false)
        .with_filter(Targets::new().with_default(Level::TRACE));

    let _ = tracing_subscriber::registry()
        .with(stderr_fmt)
        .with(feedback_layer)
        .with(otel.as_ref().map(|provider| {
            OpenTelemetryTracingBridge::new(&provider.logger).with_filter(
                tracing_subscriber::filter::filter_fn(codex_core::otel_init::codex_export_filter),
            )
        }))
        .try_init();

    let shared_state = SharedState::new(
        codex_linux_sandbox_exe,
        Arc::clone(&config),
        feedback.clone(),
    );

    let stdio_future = if server_options.stdio_enabled {
        Some(run_stdio(shared_state.clone()))
    } else {
        None
    };

    let websocket_future = server_options
        .websocket
        .clone()
        .map(|options| websocket::serve(shared_state.clone(), options));

    match (stdio_future, websocket_future) {
        (Some(stdio), Some(websocket)) => {
            tokio::pin!(stdio);
            tokio::pin!(websocket);
            tokio::select! {
                result = &mut stdio => result?,
                result = &mut websocket => result?,
            }
        }
        (Some(stdio), None) => stdio.await?,
        (None, Some(websocket)) => websocket.await?,
        (None, None) => {}
    }

    Ok(())
}

async fn run_stdio(shared_state: SharedState) -> IoResult<()> {
    let (incoming_tx, mut incoming_rx) = mpsc::channel::<JSONRPCMessage>(CHANNEL_CAPACITY);
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<OutgoingMessage>();

    let stdin_reader_handle = tokio::spawn({
        let incoming_tx = incoming_tx.clone();
        async move {
            let stdin = io::stdin();
            let reader = BufReader::new(stdin);
            let mut lines = reader.lines();

            while let Some(line) = lines.next_line().await.unwrap_or_default() {
                match serde_json::from_str::<JSONRPCMessage>(&line) {
                    Ok(msg) => {
                        if incoming_tx.send(msg).await.is_err() {
                            // Receiver gone – nothing left to do.
                            break;
                        }
                    }
                    Err(e) => error!("Failed to deserialize JSONRPCMessage: {e}"),
                }
            }

            debug!("stdin reader finished (EOF)");
        }
    });

    let processor_handle = tokio::spawn({
        let outgoing_message_sender = OutgoingMessageSender::new(outgoing_tx);
        let mut processor = MessageProcessor::new(
            outgoing_message_sender,
            shared_state.codex_linux_sandbox_exe.clone(),
            shared_state.config.clone(),
            shared_state.feedback.clone(),
        );
        async move {
            while let Some(msg) = incoming_rx.recv().await {
                match msg {
                    JSONRPCMessage::Request(r) => processor.process_request(r).await,
                    JSONRPCMessage::Response(r) => processor.process_response(r).await,
                    JSONRPCMessage::Notification(n) => processor.process_notification(n).await,
                    JSONRPCMessage::Error(e) => processor.process_error(e),
                }
            }

            info!("processor task exited (channel closed)");
        }
    });

    let stdout_writer_handle = tokio::spawn(async move {
        let mut stdout = io::stdout();
        while let Some(outgoing_message) = outgoing_rx.recv().await {
            let Ok(value) = serde_json::to_value(outgoing_message) else {
                error!("Failed to convert OutgoingMessage to JSON value");
                continue;
            };
            match serde_json::to_string(&value) {
                Ok(mut json) => {
                    json.push('\n');
                    if let Err(e) = stdout.write_all(json.as_bytes()).await {
                        error!("Failed to write to stdout: {e}");
                        break;
                    }
                }
                Err(e) => error!("Failed to serialize JSONRPCMessage: {e}"),
            }
        }

        info!("stdout writer exited (channel closed)");
    });

    let _ = tokio::join!(stdin_reader_handle, processor_handle, stdout_writer_handle);
    Ok(())
}
