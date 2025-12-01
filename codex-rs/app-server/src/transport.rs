use std::collections::HashMap;
use std::future::Future;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::pin::Pin;
use std::sync::Arc;

use codex_app_server_protocol::JSONRPCMessage;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::{self};
use tokio::sync::mpsc;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::CHANNEL_CAPACITY;
use crate::SharedState;
use crate::message_processor::MessageProcessor;
use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::OutgoingMessageSender;

pub const STDIO_TRANSPORT_NAME: &str = "stdio";

pub type TransportHandle = Arc<dyn Transport>;
type TransportBuilder = fn(&TransportSpec) -> IoResult<TransportHandle>;

pub trait Transport: Send + Sync {
    fn name(&self) -> &'static str;

    fn run(&self, shared_state: SharedState) -> TransportFuture;
}

pub type TransportFuture = Pin<Box<dyn Future<Output = IoResult<()>> + Send>>;

pub fn into_handle<T>(transport: T) -> TransportHandle
where
    T: Transport + 'static,
{
    Arc::new(transport)
}

pub fn stdio_transport() -> TransportHandle {
    into_handle(StdioTransport)
}

#[derive(Debug, Clone)]
pub struct TransportSpec {
    pub name: String,
    pub options: HashMap<String, String>,
}

impl TransportSpec {
    pub fn new(name: String, options: HashMap<String, String>) -> Self {
        Self { name, options }
    }

    pub fn stdio() -> Self {
        Self {
            name: STDIO_TRANSPORT_NAME.to_string(),
            options: HashMap::new(),
        }
    }
}

#[derive(Default)]
pub struct TransportRegistry {
    builders: HashMap<&'static str, TransportBuilder>,
}

impl TransportRegistry {
    pub fn with_builtin() -> Self {
        let mut registry = Self::default();
        registry.register(STDIO_TRANSPORT_NAME, build_stdio_transport);
        registry
    }

    pub fn default_registry() -> Self {
        Self::with_builtin()
    }

    pub fn register(&mut self, name: &'static str, builder: TransportBuilder) {
        self.builders.insert(name, builder);
    }

    pub fn build(&self, spec: &TransportSpec) -> IoResult<TransportHandle> {
        let builder = self.builders.get(spec.name.as_str()).ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("unknown transport `{}`", spec.name),
            )
        })?;
        builder(spec)
    }

    pub fn build_all(&self, specs: &[TransportSpec]) -> IoResult<Vec<TransportHandle>> {
        let mut transports = Vec::with_capacity(specs.len());
        for spec in specs {
            transports.push(self.build(spec)?);
        }
        Ok(transports)
    }
}

pub(crate) async fn run_all(
    shared_state: SharedState,
    transports: Vec<TransportHandle>,
) -> IoResult<()> {
    if transports.is_empty() {
        return Ok(());
    }

    let mut handles = Vec::with_capacity(transports.len());

    for transport in transports {
        let name = transport.name();
        let future = transport.run(shared_state.clone());
        let handle = tokio::spawn(future);
        handles.push((name, handle));
    }

    for (name, handle) in handles {
        let result = handle
            .await
            .map_err(|err| Error::other(format!("{name} transport task panicked: {err}")))?;
        result?;
    }

    Ok(())
}

fn build_stdio_transport(spec: &TransportSpec) -> IoResult<TransportHandle> {
    if !spec.options.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            format!("transport `{}` does not accept options", spec.name),
        ));
    }
    Ok(stdio_transport())
}

struct StdioTransport;

impl Transport for StdioTransport {
    fn name(&self) -> &'static str {
        STDIO_TRANSPORT_NAME
    }

    fn run(&self, shared_state: SharedState) -> TransportFuture {
        Box::pin(run_stdio(shared_state))
    }
}

async fn run_stdio(shared_state: SharedState) -> IoResult<()> {
    let (incoming_tx, mut incoming_rx) = mpsc::channel::<JSONRPCMessage>(CHANNEL_CAPACITY);
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<OutgoingMessage>(CHANNEL_CAPACITY);

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
            Arc::clone(&shared_state.cli_overrides),
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
