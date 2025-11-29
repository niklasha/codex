use clap::Parser;
use codex_app_server::run_main;
use codex_arg0::arg0_dispatch_or_else;
use codex_common::CliConfigOverrides;

fn main() -> anyhow::Result<()> {
    arg0_dispatch_or_else(|codex_linux_sandbox_exe| async move {
        let cli_config_overrides = CliConfigOverrides::parse();
        run_main(codex_linux_sandbox_exe, cli_config_overrides).await?;
        Ok(())
    })
}
