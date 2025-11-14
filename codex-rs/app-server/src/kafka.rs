use std::io::Error;
use std::io::Result as IoResult;
use std::time::Duration;

use codex_app_server_protocol::JSONRPCMessage;
use futures_util::StreamExt;
use rdkafka::ClientConfig;
use rdkafka::consumer::CommitMode;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::producer::FutureProducer;
use rdkafka::producer::FutureRecord;
use rdkafka::producer::Producer;
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
use crate::transport::into_handle;

pub const KAFKA_TRANSPORT_NAME: &str = "kafka";

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

pub(crate) fn transport_handle(options: KafkaOptions) -> TransportHandle {
    into_handle(KafkaTransport { options })
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
    let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<OutgoingMessage>();

    let processor_handle = tokio::spawn({
        let outgoing_message_sender = OutgoingMessageSender::new(outgoing_tx);
        let mut processor = MessageProcessor::new(
            outgoing_message_sender,
            shared_state.codex_linux_sandbox_exe.clone(),
            shared_state.config.clone(),
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
            match serde_json::to_string(&value) {
                Ok(json) => {
                    let key = kafka_key(&message);
                    let mut record = FutureRecord::to(&output_topic).payload(&json);
                    if let Some(ref key) = key {
                        record = record.key(key);
                    }
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
