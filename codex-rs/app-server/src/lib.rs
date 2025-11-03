#![deny(clippy::print_stdout, clippy::print_stderr)]

use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::path::PathBuf;
use std::sync::Arc;

use codex_common::CliConfigOverrides;
use codex_core::config::Config;
use codex_core::config::ConfigOverrides;
use codex_feedback::CodexFeedback;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use tracing::Level;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod bespoke_event_handling;
mod codex_message_processor;
mod error_code;
mod fuzzy_file_search;
mod message_processor;
mod models;
mod outgoing_message;
mod transport;

pub use self::transport::STDIO_TRANSPORT_NAME;
pub use self::transport::Transport;
pub use self::transport::TransportHandle;
pub use self::transport::into_handle as into_transport_handle;
pub use self::transport::stdio_transport;

/// Size of the bounded channels used to communicate between tasks. The value
/// is a balance between throughput and memory usage – 128 messages should be
/// plenty for an interactive CLI.
pub(crate) const CHANNEL_CAPACITY: usize = 128;

#[derive(Clone)]
pub struct SharedState {
    pub(crate) codex_linux_sandbox_exe: Option<PathBuf>,
    pub(crate) config: Arc<Config>,
    pub(crate) feedback: CodexFeedback,
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
pub struct ServerOptions {
    transports: Vec<TransportHandle>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            transports: vec![transport::stdio_transport()],
        }
    }
}

impl ServerOptions {
    pub fn with_transports(transports: Vec<TransportHandle>) -> Self {
        Self { transports }
    }

    pub fn transports(&self) -> &[TransportHandle] {
        &self.transports
    }

    pub fn into_transports(self) -> Vec<TransportHandle> {
        self.transports
    }

    pub fn clear_transports(&mut self) {
        self.transports.clear();
    }

    pub fn add_transport_handle(&mut self, transport: TransportHandle) {
        self.transports.push(transport);
    }

    pub fn add_transport<T>(&mut self, transport: T)
    where
        T: Transport + 'static,
    {
        self.transports.push(transport::into_handle(transport));
    }

    pub fn ensure_stdio(&mut self) {
        if !self
            .transports
            .iter()
            .any(|transport| transport.name() == transport::STDIO_TRANSPORT_NAME)
        {
            self.transports.push(transport::stdio_transport());
        }
    }

    pub fn disable_stdio(&mut self) {
        self.transports
            .retain(|transport| transport.name() != transport::STDIO_TRANSPORT_NAME);
    }
}

pub async fn run_main(
    codex_linux_sandbox_exe: Option<PathBuf>,
    cli_config_overrides: CliConfigOverrides,
    server_options: ServerOptions,
) -> IoResult<()> {
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

    transport::run_all(shared_state, server_options.into_transports()).await
}
