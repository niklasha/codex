use std::io::Error;
use std::io::Result as IoResult;
use std::sync::Arc;
use std::time::Duration;

use codex_app_server_protocol::JSONRPCMessage;
use futures_util::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaError;
use rdkafka::message::Header;
use rdkafka::message::Message;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::producer::Producer;
use serde_json::Map;
use serde_json::Value;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::CHANNEL_CAPACITY;
use crate::SharedState;
use crate::message_processor::MessageProcessor;
use crate::outgoing_message::OutgoingMessage;
use crate::outgoing_message::OutgoingMessageSender;
use crate::transport::Transport;
use crate::transport::TransportFuture;
use crate::transport::TransportHandle;
use crate::transport::TransportRegistry;
use crate::transport::TransportSpec;
use crate::transport::into_handle;

pub const KAFKA_TRANSPORT_NAME: &str = "kafka";
const BOOTSTRAP_SERVERS: &str = "bootstrap_servers";
const BROKERS: &str = "brokers";
const INPUT_TOPIC: &str = "input_topic";
const OUTPUT_TOPIC: &str = "output_topic";
const GROUP_ID: &str = "group_id";
const CLIENT_ID: &str = "client_id";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaOptions {
    pub brokers: String,
    pub input_topic: String,
    pub output_topic: String,
    pub group_id: String,
    pub client_id: Option<String>,
}

impl KafkaOptions {
    pub fn new(
        brokers: impl Into<String>,
        input_topic: impl Into<String>,
        output_topic: impl Into<String>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            brokers: brokers.into(),
            input_topic: input_topic.into(),
            output_topic: output_topic.into(),
            group_id: group_id.into(),
            client_id: None,
        }
    }

    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }
}

pub fn transport_handle(options: KafkaOptions) -> TransportHandle {
    into_handle(KafkaTransport { options })
}

pub fn register_kafka_transport(registry: &mut TransportRegistry) {
    registry.register(KAFKA_TRANSPORT_NAME, kafka_transport_from_spec);
}

fn kafka_transport_from_spec(spec: &TransportSpec) -> IoResult<TransportHandle> {
    let options = options_from_spec(spec)?;
    Ok(transport_handle(options))
}

fn options_from_spec(spec: &TransportSpec) -> IoResult<KafkaOptions> {
    let brokers =
        required_option(spec, BOOTSTRAP_SERVERS).or_else(|_| required_option(spec, BROKERS))?;
    let input_topic = required_option(spec, INPUT_TOPIC)?;
    let output_topic = required_option(spec, OUTPUT_TOPIC)?;
    let group_id = required_option(spec, GROUP_ID)?;
    let mut options = KafkaOptions::new(brokers, input_topic, output_topic, group_id);
    if let Some(client_id) = spec.options.get(CLIENT_ID) {
        options = options.with_client_id(client_id);
    }
    Ok(options)
}

fn required_option(spec: &TransportSpec, key: &str) -> IoResult<String> {
    spec.options.get(key).cloned().ok_or_else(|| {
        Error::other(format!(
            "transport `{}` missing required option `{key}`",
            spec.name
        ))
    })
}

struct KafkaTransport {
    options: KafkaOptions,
}

impl Transport for KafkaTransport {
    fn name(&self) -> &'static str {
        KAFKA_TRANSPORT_NAME
    }

    fn run(&self, shared_state: SharedState) -> TransportFuture {
        Box::pin(run_kafka(shared_state, self.options.clone()))
    }
}

async fn run_kafka(shared_state: SharedState, options: KafkaOptions) -> IoResult<()> {
    let consumer = build_consumer(&options)?;
    consumer
        .subscribe(&[&options.input_topic])
        .map_err(kafka_error)?;
    let producer = build_producer(&options)?;

    let (incoming_tx, mut incoming_rx) = mpsc::channel::<JSONRPCMessage>(CHANNEL_CAPACITY);
    let (outgoing_tx, mut outgoing_rx) = mpsc::channel::<OutgoingMessage>(CHANNEL_CAPACITY);

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
            while let Some(message) = incoming_rx.recv().await {
                match message {
                    JSONRPCMessage::Request(r) => processor.process_request(r).await,
                    JSONRPCMessage::Response(r) => processor.process_response(r).await,
                    JSONRPCMessage::Notification(n) => processor.process_notification(n).await,
                    JSONRPCMessage::Error(e) => processor.process_error(e),
                }
            }
            info!("kafka processor task exited (channel closed)");
        }
    });

    let input_topic = options.input_topic.clone();
    let consumer_handle = tokio::spawn({
        let consumer = consumer;
        let incoming_tx = incoming_tx.clone();
        async move {
            let mut stream = consumer.stream();
            while let Some(result) = stream.next().await {
                match result {
                    Ok(message) => {
                        if let Some(payload) = message.payload() {
                            match std::str::from_utf8(payload) {
                                Ok(text) => match serde_json::from_str::<JSONRPCMessage>(text) {
                                    Ok(json) => {
                                        if incoming_tx.send(json).await.is_err() {
                                            break;
                                        }
                                        if let Err(err) =
                                            consumer.commit_message(&message, CommitMode::Async)
                                        {
                                            warn!(
                                                error = ?err,
                                                topic = %input_topic,
                                                "failed to commit kafka message"
                                            );
                                        }
                                    }
                                    Err(err) => warn!(
                                        error = %err,
                                        topic = %input_topic,
                                        "invalid JSON payload from kafka"
                                    ),
                                },
                                Err(err) => warn!(
                                    error = %err,
                                    topic = %input_topic,
                                    "invalid UTF-8 payload from kafka"
                                ),
                            }
                        } else {
                            warn!(topic = %input_topic, "kafka message missing payload");
                        }
                    }
                    Err(err) => warn!(
                        error = ?err,
                        topic = %input_topic,
                        "error receiving kafka message"
                    ),
                }
            }
            debug!(topic = %input_topic, "kafka consumer task finished");
            drop(incoming_tx);
        }
    });

    let output_topic = options.output_topic.clone();
    let producer_handle = tokio::spawn(async move {
        while let Some(message) = outgoing_rx.recv().await {
            let Ok(value) = serde_json::to_value(&message) else {
                warn!("failed to convert outgoing message to JSON value");
                continue;
            };
            let metadata = KafkaMetadata::from_message(&message, &value);
            match serde_json::to_string(&value) {
                Ok(json) => {
                    let mut record = FutureRecord::to(&output_topic).payload(&json);
                    let kafka_key_value = metadata
                        .conversation_id
                        .clone()
                        .or_else(|| kafka_key(&message));
                    if let Some(ref key) = kafka_key_value {
                        record = record.key(key);
                    }
                    let headers = metadata.into_headers();
                    record = record.headers(headers);
                    if let Err((err, _)) = producer.send(record, Duration::from_secs(0)).await {
                        warn!(error = ?err, topic = %output_topic, "failed to send kafka message");
                    }
                }
                Err(err) => warn!(
                    error = %err,
                    topic = %output_topic,
                    "failed to serialize outgoing message"
                ),
            }
        }
        if let Err(err) = producer.flush(Duration::from_secs(5)) {
            warn!(error = ?err, topic = %output_topic, "failed to flush kafka producer");
        }
        debug!(topic = %output_topic, "kafka producer task finished");
    });

    let _ = tokio::join!(consumer_handle, processor_handle, producer_handle);
    Ok(())
}

fn build_consumer(options: &KafkaOptions) -> IoResult<StreamConsumer> {
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", &options.brokers)
        .set("group.id", &options.group_id)
        .set("enable.partition.eof", "false")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "latest");
    if let Some(client_id) = &options.client_id {
        config.set("client.id", client_id);
    }
    config.create().map_err(kafka_error)
}

fn build_producer(options: &KafkaOptions) -> IoResult<FutureProducer> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &options.brokers);
    if let Some(client_id) = &options.client_id {
        config.set("client.id", client_id);
    }
    config.create().map_err(kafka_error)
}

fn kafka_key(message: &OutgoingMessage) -> Option<String> {
    match message {
        OutgoingMessage::Response(response) => Some(request_id_to_string(&response.id)),
        OutgoingMessage::Error(error) => Some(request_id_to_string(&error.id)),
        _ => None,
    }
}

fn request_id_to_string(id: &codex_app_server_protocol::RequestId) -> String {
    match id {
        codex_app_server_protocol::RequestId::String(value) => value.clone(),
        codex_app_server_protocol::RequestId::Integer(value) => value.to_string(),
    }
}

fn kafka_error(err: KafkaError) -> Error {
    Error::other(err.to_string())
}

const SCHEMA_VERSION: &str = codex_app_server_protocol::JSONRPC_VERSION;

#[derive(Debug, Default)]
struct KafkaMetadata {
    conversation_id: Option<String>,
    message_id: Option<String>,
    parent_id: Option<String>,
    role: Option<String>,
    method: Option<String>,
    error: Option<String>,
}

impl KafkaMetadata {
    fn from_message(message: &OutgoingMessage, json: &Value) -> Self {
        let metadata = Self {
            conversation_id: extract_conversation_id(json),
            message_id: extract_message_id(json, message),
            parent_id: extract_parent_id(json),
            role: extract_role(json),
            method: extract_top_level_method(json),
            error: match message {
                OutgoingMessage::Error(err) => Some(err.error.message.clone()),
                _ => None,
            },
        };
        metadata
    }

    fn into_headers(self) -> OwnedHeaders {
        let mut headers = OwnedHeaders::new();
        headers = headers.insert(Header {
            key: "schema-version",
            value: Some(SCHEMA_VERSION),
        });

        if let Some(value) = self.conversation_id.as_deref() {
            headers = headers.insert(Header {
                key: "conversation-id",
                value: Some(value),
            });
        }

        if let Some(value) = self.message_id.as_deref() {
            headers = headers.insert(Header {
                key: "message-id",
                value: Some(value),
            });
        }

        if let Some(value) = self.parent_id.as_deref() {
            headers = headers.insert(Header {
                key: "parent-id",
                value: Some(value),
            });
        }

        if let Some(value) = self.role.as_deref() {
            headers = headers.insert(Header {
                key: "role",
                value: Some(value),
            });
        }

        if let Some(value) = self.method.as_deref() {
            headers = headers.insert(Header {
                key: "method",
                value: Some(value),
            });
        }

        if let Some(value) = self.error.as_deref() {
            headers = headers.insert(Header {
                key: "error",
                value: Some(value),
            });
        }

        headers
    }
}

fn extract_conversation_id(json: &Value) -> Option<String> {
    match json {
        Value::Object(obj) => lookup_string(obj, &["conversationId", "threadId"])
            .or_else(|| {
                obj.get("params")
                    .and_then(|value| value.as_object())
                    .and_then(|params| lookup_conversation_in_container(params))
            })
            .or_else(|| {
                obj.get("result")
                    .and_then(|value| value.as_object())
                    .and_then(|result| lookup_conversation_in_container(result))
            }),
        _ => None,
    }
}

fn lookup_conversation_in_container(map: &Map<String, Value>) -> Option<String> {
    lookup_string(map, &["conversationId", "threadId"])
        .or_else(|| {
            map.get("thread")
                .and_then(|value| value.as_object())
                .and_then(|thread| lookup_string(thread, &["id"]))
        })
        .or_else(|| {
            map.get("turn")
                .and_then(|value| value.as_object())
                .and_then(|turn| lookup_string(turn, &["threadId"]))
        })
}

fn extract_message_id(json: &Value, message: &OutgoingMessage) -> Option<String> {
    match json {
        Value::Object(obj) => match message {
            OutgoingMessage::Request(_)
            | OutgoingMessage::Response(_)
            | OutgoingMessage::Error(_) => obj.get("id").and_then(value_to_string),
            OutgoingMessage::Notification(_) | OutgoingMessage::AppServerNotification(_) => obj
                .get("params")
                .and_then(|value| value.as_object())
                .and_then(|params| {
                    params.get("id").and_then(value_to_string).or_else(|| {
                        params
                            .get("msg")
                            .and_then(|msg| msg.as_object())
                            .and_then(|msg| msg.get("id"))
                            .and_then(value_to_string)
                    })
                }),
        },
        _ => None,
    }
}

fn extract_parent_id(json: &Value) -> Option<String> {
    match json {
        Value::Object(obj) => obj
            .get("params")
            .and_then(|value| value.as_object())
            .and_then(|params| {
                lookup_string(
                    params,
                    &["parentId", "parentThreadId", "parentConversationId"],
                )
            })
            .or_else(|| {
                obj.get("result")
                    .and_then(|value| value.as_object())
                    .and_then(|result| {
                        lookup_string(
                            result,
                            &["parentId", "parentThreadId", "parentConversationId"],
                        )
                    })
            }),
        _ => None,
    }
}

fn extract_role(json: &Value) -> Option<String> {
    let method = json.get("method").and_then(|value| value.as_str())?;
    if !method.starts_with("codex/event/") {
        return None;
    }
    let params = json.get("params").and_then(|value| value.as_object())?;
    let msg = params.get("msg").and_then(|value| value.as_object())?;
    let event_type = msg.get("type").and_then(|value| value.as_str())?;
    if event_type.starts_with("agent_") {
        Some("assistant".to_string())
    } else if event_type.starts_with("user_") {
        Some("user".to_string())
    } else {
        None
    }
}

fn extract_top_level_method(json: &Value) -> Option<String> {
    json.get("method")
        .and_then(|value| value.as_str())
        .map(|s| s.to_string())
}

fn lookup_string(map: &Map<String, Value>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| map.get(*key).and_then(value_to_string))
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(num) => Some(num.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::outgoing_message::OutgoingNotification;
    use serde_json::json;

    #[test]
    fn extracts_metadata_from_codex_event_notification() {
        let params = json!({
            "conversationId": "conv-123",
            "id": "event-42",
            "msg": {
                "type": "agent_message",
                "message": "hello"
            }
        });
        let notification = OutgoingNotification {
            method: "codex/event/agent_message".to_string(),
            params: Some(params),
        };
        let message = OutgoingMessage::Notification(notification);
        let value = serde_json::to_value(&message).expect("serialize notification");
        let metadata = KafkaMetadata::from_message(&message, &value);
        assert_eq!(metadata.conversation_id.as_deref(), Some("conv-123"));
        assert_eq!(metadata.message_id.as_deref(), Some("event-42"));
        assert_eq!(metadata.role.as_deref(), Some("assistant"));
        assert_eq!(
            metadata.method.as_deref(),
            Some("codex/event/agent_message")
        );
    }

    #[test]
    fn extracts_conversation_from_response_result() {
        let response = OutgoingMessage::Response(crate::outgoing_message::OutgoingResponse {
            id: codex_app_server_protocol::RequestId::String("req-1".to_string()),
            result: json!({
                "conversationId": "conv-abc",
                "status": "ok"
            }),
        });
        let value = serde_json::to_value(&response).expect("serialize response");
        let metadata = KafkaMetadata::from_message(&response, &value);
        assert_eq!(metadata.conversation_id.as_deref(), Some("conv-abc"));
        assert_eq!(metadata.message_id.as_deref(), Some("req-1"));
        assert!(metadata.role.is_none());
        assert!(metadata.method.is_none());
    }
}
