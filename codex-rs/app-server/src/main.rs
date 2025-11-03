use std::net::SocketAddr;

use clap::Parser;
use codex_app_server::DEFAULT_WEBSOCKET_PATH;
use codex_app_server::ServerOptions;
use codex_app_server::WebsocketOptions;
use codex_app_server::run_main;
use codex_arg0::arg0_dispatch_or_else;
use codex_common::CliConfigOverrides;

#[derive(Parser, Debug)]
#[command(name = "codex-app-server", version, disable_help_subcommand = true)]
struct Cli {
    #[clap(flatten)]
    config_overrides: CliConfigOverrides,

    /// Bind address for the WebSocket listener (e.g. 0.0.0.0:7443)
    #[clap(long = "websocket", value_name = "ADDR")]
    websocket: Option<SocketAddr>,

    /// HTTP path for the WebSocket endpoint.
    #[clap(long = "websocket-path", value_name = "PATH", default_value = DEFAULT_WEBSOCKET_PATH)]
    websocket_path: String,

    /// Keep stdin/stdout JSONL transport enabled while WebSocket runs.
    #[clap(long)]
    stdio: bool,
}

fn main() -> anyhow::Result<()> {
    arg0_dispatch_or_else(|codex_linux_sandbox_exe| async move {
        let Cli {
            config_overrides,
            websocket,
            websocket_path,
            stdio,
        } = Cli::parse();

        let mut server_options = ServerOptions::default();
        if let Some(addr) = websocket {
            if !stdio {
                server_options.disable_stdio();
            }
            server_options.add_websocket(WebsocketOptions::new(addr, websocket_path));
        }

        run_main(codex_linux_sandbox_exe, config_overrides, server_options).await?;
        Ok(())
    })
}
