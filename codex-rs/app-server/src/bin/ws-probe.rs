use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use codex_app_server::WEBSOCKET_SUBPROTOCOL;
use futures_util::SinkExt;
use futures_util::StreamExt;
use http::header::SEC_WEBSOCKET_PROTOCOL;
use serde_json::Value;
use serde_json::json;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message;

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Parser, Debug)]
struct Cli {
    /// WebSocket endpoint (e.g. ws://127.0.0.1:9999/app-server/v1)
    #[clap(
        long,
        value_name = "URL",
        default_value = "ws://127.0.0.1:9999/app-server/v1"
    )]
    url: String,

    /// Optional text message to send via sendUserMessage after initialization.
    #[clap(long)]
    user_message: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut request = cli.url.clone().into_client_request()?;
    request
        .headers_mut()
        .insert(SEC_WEBSOCKET_PROTOCOL, WEBSOCKET_SUBPROTOCOL.parse()?);

    let (mut socket, _response) = connect_async(request).await?;

    if let Some(Ok(Message::Text(frame))) = socket.next().await {
        println!("<- {frame}");
    }

    send_initialize(&mut socket).await?;

    if let Some(user_text) = cli.user_message.as_deref() {
        let conversation_id = start_conversation(&mut socket).await?;
        add_listener(&mut socket, &conversation_id).await?;
        send_user_message(&mut socket, &conversation_id, user_text).await?;
        wait_for_task_complete(&mut socket, &conversation_id).await?;
    }

    Ok(())
}

async fn send_initialize(socket: &mut WsStream) -> Result<()> {
    let init = json!({
        "id": 0,
        "method": "initialize",
        "params": { "clientInfo": { "name": "ws-probe", "version": env!("CARGO_PKG_VERSION") } }
    });
    socket.send(Message::Text(init.to_string())).await?;
    let _ = wait_for_response(socket, 0).await?;
    socket
        .send(Message::Text(r#"{"method":"initialized"}"#.to_string()))
        .await?;
    Ok(())
}

async fn start_conversation(socket: &mut WsStream) -> Result<String> {
    let new_conversation = json!({
        "id": 1,
        "method": "newConversation",
        "params": {}
    });
    socket
        .send(Message::Text(new_conversation.to_string()))
        .await?;

    let response = wait_for_response(socket, 1).await?;
    let conversation_id = response
        .get("result")
        .and_then(|value| value.get("conversationId"))
        .and_then(Value::as_str)
        .context("newConversation missing result.conversationId")?;
    println!("conversationId={conversation_id}");
    Ok(conversation_id.to_string())
}

async fn send_user_message(socket: &mut WsStream, conversation_id: &str, text: &str) -> Result<()> {
    let frame = json!({
        "id": 3,
        "method": "sendUserMessage",
        "params": {
            "conversationId": conversation_id,
            "items": [
                { "type": "text", "data": { "text": text } }
            ]
        }
    });
    socket.send(Message::Text(frame.to_string())).await?;
    let _ = wait_for_response(socket, 3).await?;
    Ok(())
}

async fn wait_for_response(socket: &mut WsStream, target_id: i64) -> Result<Value> {
    while let Some(frame) = socket.next().await {
        match frame {
            Ok(Message::Text(text)) => {
                println!("<- {}", &text);
                match serde_json::from_str::<Value>(&text) {
                    Ok(value) => {
                        if value.get("id").and_then(Value::as_i64) == Some(target_id) {
                            return Ok(value);
                        }
                    }
                    Err(err) => {
                        eprintln!("! failed to parse JSON frame: {err}");
                    }
                }
            }
            Ok(Message::Ping(payload)) => {
                socket.send(Message::Pong(payload)).await?;
            }
            Ok(Message::Close(frame)) => {
                println!("<- close {frame:?}");
                anyhow::bail!("connection closed before receiving response {target_id}");
            }
            Ok(Message::Binary(_)) | Ok(Message::Pong(_)) => {
                // Ignore.
            }
            Err(err) => return Err(err.into()),
            Ok(Message::Frame(_)) => {}
        }
    }

    anyhow::bail!("socket closed before receiving response {target_id}");
}

async fn add_listener(socket: &mut WsStream, conversation_id: &str) -> Result<()> {
    let frame = json!({
        "id": 2,
        "method": "addConversationListener",
        "params": {
            "conversationId": conversation_id,
            "experimentalRawEvents": true
        }
    });
    socket.send(Message::Text(frame.to_string())).await?;
    let _ = wait_for_response(socket, 2).await?;
    Ok(())
}

async fn wait_for_task_complete(socket: &mut WsStream, conversation_id: &str) -> Result<()> {
    loop {
        match socket.next().await {
            Some(Ok(Message::Text(text))) => {
                println!("<- {}", text);
                if let Ok(value) = serde_json::from_str::<Value>(&text) {
                    let method_matches = value
                        .get("method")
                        .and_then(Value::as_str)
                        .map(|m| m == "codex/event/task_complete")
                        .unwrap_or(false);
                    let conversation_matches = value
                        .get("params")
                        .and_then(|params| params.get("conversationId"))
                        .and_then(Value::as_str)
                        == Some(conversation_id);
                    if method_matches && conversation_matches {
                        return Ok(());
                    }
                }
            }
            Some(Ok(Message::Ping(payload))) => {
                socket.send(Message::Pong(payload)).await?;
            }
            Some(Ok(Message::Pong(_))) | Some(Ok(Message::Binary(_))) => {
                // Ignore ancillary frames.
            }
            Some(Ok(Message::Close(frame))) => {
                println!("<- close {frame:?}");
                anyhow::bail!("connection closed before task completion");
            }
            Some(Err(err)) => return Err(err.into()),
            None => anyhow::bail!("socket closed before task completion"),
            Some(Ok(Message::Frame(_))) => {}
        }
    }
}
