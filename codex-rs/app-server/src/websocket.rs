use std::borrow::Cow;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Result as IoResult;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use codex_app_server_protocol::JSONRPCMessage;
use futures_util::SinkExt;
use futures_util::StreamExt;
use http::HeaderValue;
use http::Method;
use http::StatusCode;
use http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::error::Error as WsError;
use tokio_tungstenite::tungstenite::handshake::server::Request;
use tokio_tungstenite::tungstenite::handshake::server::Response;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tracing::debug;
use tracing::info;
use tracing::trace;
use tracing::warn;

use crate::CHANNEL_CAPACITY;
use crate::SharedState;
use crate::message_processor::MessageProcessor;
use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::OutgoingMessageSender;

type UpgradeError = http::Response<Option<String>>;

pub const WEBSOCKET_SUBPROTOCOL: &str = "codex.app-server.v1";
pub const DEFAULT_WEBSOCKET_PATH: &str = "/app-server/v1";
pub(crate) const SERVER_HELLO_METHOD: &str = "server/hello";
const SERVER_ERROR_METHOD: &str = "server/error";
const MAX_PONG_LAG: Duration = Duration::from_secs(60);
const PING_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq)]
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

pub(crate) async fn serve(state: SharedState, options: WebsocketOptions) -> IoResult<()> {
    let WebsocketOptions { bind_addr, path } = options;
    let listener = TcpListener::bind(bind_addr).await?;
    let local_addr = listener.local_addr()?;
    info!(%local_addr, path=%path, "websocket listener started");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let connection_state = state.clone();
        let connection_path = path.clone();
        tokio::spawn(async move {
            if let Err(err) =
                handle_connection(stream, peer_addr, connection_state, connection_path).await
            {
                warn!(%peer_addr, error = ?err, "websocket connection closed with error");
            }
        });
    }
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    peer_addr: SocketAddr,
    state: SharedState,
    expected_path: String,
) -> IoResult<()> {
    let callback = {
        let expected_path = expected_path.clone();
        let request_logger = peer_addr;
        move |req: &Request, response: Response| -> Result<Response, UpgradeError> {
            trace!(
                peer = %request_logger,
                method = %req.method(),
                uri = %req.uri(),
                "websocket upgrade requested"
            );
            negotiate_subprotocol(req, response, &expected_path)
        }
    };

    let mut websocket = accept_hdr_async(stream, callback)
        .await
        .map_err(map_ws_error)?;

    info!(%peer_addr, "websocket handshake accepted");

    let hello_frame = serde_json::json!({
        "method": SERVER_HELLO_METHOD,
        "params": {
            "version": env!("CARGO_PKG_VERSION"),
            "protocol": WEBSOCKET_SUBPROTOCOL,
        }
    });
    websocket
        .send(Message::Text(
            serde_json::to_string(&hello_frame)
                .map_err(|err| Error::new(ErrorKind::InvalidData, err))?,
        ))
        .await
        .map_err(map_ws_error)?;

    let (mut sink, mut stream_reader) = websocket.split();

    let (incoming_tx, mut incoming_rx) = mpsc::channel::<JSONRPCMessage>(CHANNEL_CAPACITY);
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<OutgoingMessage>();
    let (control_tx, mut control_rx) = mpsc::unbounded_channel::<Message>();

    let last_pong = Arc::new(tokio::sync::Mutex::new(Instant::now()));

    let processor_handle = tokio::spawn({
        let outgoing_message_sender = OutgoingMessageSender::new(outgoing_tx);
        let mut processor = MessageProcessor::new(
            outgoing_message_sender,
            state.codex_linux_sandbox_exe.clone(),
            state.config.clone(),
            state.feedback.clone(),
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
            info!(%peer_addr, "processor task exited (channel closed)");
        }
    });

    let read_handle = tokio::spawn({
        let control_tx = control_tx.clone();
        let incoming_tx = incoming_tx.clone();
        let last_pong = Arc::clone(&last_pong);
        async move {
            while let Some(frame) = stream_reader.next().await {
                match frame {
                    Ok(Message::Text(text)) => {
                        match serde_json::from_str::<JSONRPCMessage>(&text) {
                            Ok(msg) => {
                                if incoming_tx.send(msg).await.is_err() {
                                    break;
                                }
                            }
                            Err(err) => {
                                warn!(%peer_addr, error=?err, "invalid JSON frame");
                                send_server_error(
                                    &control_tx,
                                    "invalid_json",
                                    &format!("failed to parse JSON: {err}"),
                                );
                            }
                        }
                    }
                    Ok(Message::Binary(_)) => {
                        warn!(%peer_addr, "binary frames are not supported");
                        send_server_error(
                            &control_tx,
                            "unsupported_frame",
                            "binary frames are not supported",
                        );
                        let _ = control_tx.send(Message::Close(Some(CloseFrame {
                            code: CloseCode::Unsupported,
                            reason: Cow::from("binary frames are not supported"),
                        })));
                        break;
                    }
                    Ok(Message::Ping(payload)) => {
                        let _ = control_tx.send(Message::Pong(payload));
                    }
                    Ok(Message::Pong(_)) => {
                        *last_pong.lock().await = Instant::now();
                    }
                    Ok(Message::Close(frame)) => {
                        let _ = control_tx.send(Message::Close(frame));
                        break;
                    }
                    Err(err) => {
                        warn!(%peer_addr, error=?err, "read error");
                        let _ = control_tx.send(Message::Close(Some(CloseFrame {
                            code: CloseCode::Protocol,
                            reason: Cow::from("websocket read error"),
                        })));
                        break;
                    }
                    Ok(Message::Frame(_)) => {
                        // This variant is not constructed by tungstenite when reading.
                    }
                }
            }
            debug!(%peer_addr, "websocket reader finished");
            drop(incoming_tx);
        }
    });

    let writer_handle = tokio::spawn({
        let control_tx = control_tx.clone();
        async move {
            loop {
                tokio::select! {
                    control = control_rx.recv() => {
                        match control {
                            Some(frame) => {
                                if let Err(err) = sink.send(frame).await {
                                    warn!(%peer_addr, error=?err, "failed to send control frame");
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    outgoing = outgoing_rx.recv() => {
                        match outgoing {
                            Some(message) => {
                                let Ok(value) = serde_json::to_value(message) else {
                                    warn!(%peer_addr, "failed to convert outgoing message to JSON value");
                                    continue;
                                };
                                match serde_json::to_string(&value) {
                                    Ok(json) => {
                                        if let Err(err) = sink.send(Message::Text(json)).await {
                                            warn!(%peer_addr, error=?err, "failed to send JSON frame");
                                            break;
                                        }
                                    }
                                    Err(err) => {
                                        warn!(%peer_addr, error=?err, "failed to serialize outgoing JSON");
                                    }
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
            let _ = sink.close().await;
            let _ = control_tx.send(Message::Close(None));
            debug!(%peer_addr, "websocket writer finished");
        }
    });

    let ping_handle = tokio::spawn({
        let control_tx = control_tx.clone();
        let last_pong = Arc::clone(&last_pong);
        async move {
            let mut interval = time::interval(PING_INTERVAL);
            loop {
                interval.tick().await;
                if control_tx.send(Message::Ping(Vec::new())).is_err() {
                    break;
                }

                let stale = {
                    let guard = last_pong.lock().await;
                    guard.elapsed() > MAX_PONG_LAG
                };
                if stale {
                    warn!(%peer_addr, "closing connection due to missing pong");
                    let _ = control_tx.send(Message::Close(Some(CloseFrame {
                        code: CloseCode::Protocol,
                        reason: Cow::from("pong timeout"),
                    })));
                    break;
                }
            }
        }
    });

    let _ = tokio::join!(read_handle, writer_handle, processor_handle, ping_handle);
    Ok(())
}

fn negotiate_subprotocol(
    req: &Request,
    mut response: Response,
    expected_path: &str,
) -> Result<Response, UpgradeError> {
    if req.method() != Method::GET {
        return Err(upgrade_error(
            StatusCode::METHOD_NOT_ALLOWED,
            "only GET supported",
        ));
    }
    if req.uri().path() != expected_path {
        return Err(upgrade_error(StatusCode::NOT_FOUND, "unexpected path"));
    }

    let raw = req.headers().get(SEC_WEBSOCKET_PROTOCOL).ok_or_else(|| {
        upgrade_error(
            StatusCode::UPGRADE_REQUIRED,
            "missing Sec-WebSocket-Protocol header",
        )
    })?;
    let header = raw.to_str().map_err(|_| {
        upgrade_error(
            StatusCode::BAD_REQUEST,
            "invalid Sec-WebSocket-Protocol header",
        )
    })?;

    let has_protocol = header
        .split(',')
        .map(str::trim)
        .any(|value| value == WEBSOCKET_SUBPROTOCOL);
    if !has_protocol {
        return Err(upgrade_error(
            StatusCode::UPGRADE_REQUIRED,
            "required subprotocol codex.app-server.v1 not offered",
        ));
    }

    response.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_static(WEBSOCKET_SUBPROTOCOL),
    );

    Ok(response)
}

fn send_server_error(control_tx: &mpsc::UnboundedSender<Message>, code: &str, details: &str) {
    let payload = serde_json::json!({
        "method": SERVER_ERROR_METHOD,
        "params": {
            "code": code,
            "details": details,
        }
    });
    if let Ok(text) = serde_json::to_string(&payload) {
        let _ = control_tx.send(Message::Text(text));
    }
}

fn map_ws_error(err: WsError) -> Error {
    match err {
        WsError::Io(inner) => inner,
        other => Error::other(other),
    }
}

fn upgrade_error(status: StatusCode, message: &str) -> UpgradeError {
    let mut response =
        http::Response::new(Some(serde_json::json!({ "error": message }).to_string()));
    *response.status_mut() = status;
    response.headers_mut().insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    response
}
