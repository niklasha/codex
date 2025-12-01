use anyhow::Context;
use clap::Parser;
use codex_app_server::KafkaOptions;
use codex_app_server::ServerOptions;
use codex_app_server::kafka;
use codex_app_server::run_main;
use codex_arg0::arg0_dispatch_or_else;
use codex_common::CliConfigOverrides;

#[derive(Debug, Parser)]
#[command(name = "codex-app-server", version, disable_help_subcommand = true)]
struct Cli {
    #[clap(flatten)]
    config_overrides: CliConfigOverrides,

    #[clap(flatten)]
    kafka: KafkaCliOptions,

    /// Keep stdin/stdout JSONL transport enabled while Kafka runs.
    #[clap(long)]
    stdio: bool,
}

#[derive(Debug, Default, Parser)]
struct KafkaCliOptions {
    /// Kafka bootstrap servers list, e.g. localhost:9092.
    #[clap(long = "kafka-bootstrap-servers", value_name = "HOSTS")]
    bootstrap_servers: Option<String>,

    /// Kafka topic that provides inbound JSON-RPC messages for the app server.
    #[clap(long = "kafka-input-topic", value_name = "TOPIC")]
    input_topic: Option<String>,

    /// Kafka topic that receives outbound JSON-RPC messages from the app server.
    #[clap(long = "kafka-output-topic", value_name = "TOPIC")]
    output_topic: Option<String>,

    /// Kafka consumer group id.
    #[clap(long = "kafka-group-id", value_name = "GROUP")]
    group_id: Option<String>,

    /// Optional Kafka client id used for both the consumer and producer.
    #[clap(long = "kafka-client-id", value_name = "ID")]
    client_id: Option<String>,
}

impl KafkaCliOptions {
    fn try_build(&self) -> anyhow::Result<Option<KafkaOptions>> {
        let present = self.bootstrap_servers.is_some()
            || self.input_topic.is_some()
            || self.output_topic.is_some()
            || self.group_id.is_some()
            || self.client_id.is_some();

        if !present {
            return Ok(None);
        }

        let brokers = self
            .bootstrap_servers
            .as_deref()
            .context("--kafka-bootstrap-servers is required when enabling Kafka transport")?;
        let input_topic = self
            .input_topic
            .as_deref()
            .context("--kafka-input-topic is required when enabling Kafka transport")?;
        let output_topic = self
            .output_topic
            .as_deref()
            .context("--kafka-output-topic is required when enabling Kafka transport")?;
        let group_id = self
            .group_id
            .as_deref()
            .context("--kafka-group-id is required when enabling Kafka transport")?;

        let mut options = KafkaOptions::new(brokers, input_topic, output_topic, group_id);
        if let Some(client_id) = &self.client_id {
            options = options.with_client_id(client_id);
        }
        Ok(Some(options))
    }
}

fn main() -> anyhow::Result<()> {
    arg0_dispatch_or_else(|codex_linux_sandbox_exe| async move {
        let Cli {
            config_overrides,
            kafka,
            stdio,
        } = Cli::parse();

        let mut server_options = ServerOptions::default();
        if let Some(kafka_options) = kafka.try_build()? {
            if !stdio {
                server_options.disable_stdio();
            }
            server_options.add_transport_handle(kafka::transport_handle(kafka_options));
        }

        run_main(codex_linux_sandbox_exe, config_overrides, server_options).await?;
        Ok(())
    })
}
