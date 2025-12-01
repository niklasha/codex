use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use assert_cmd::cargo::cargo_bin;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::process::ChildStderr;
use tokio::process::ChildStdin;
use tokio::process::ChildStdout;
use tokio::process::Command;
use tokio::time::Instant;
use tokio::time::timeout;

const INVALID_OVERRIDE: &[&str] = &["-c", "model_provider"];
const VALID_OVERRIDES: &[&str] = &["-c", "model_provider=ollama", "-c", "model=gpt-oss:20b"];
const PROVIDER_NEEDLE: &str = "provider=ModelProviderInfo { name: \"Ollama\"";

#[tokio::test]
async fn invalid_cli_override_produces_parse_error() -> Result<()> {
    let output = run_app_server_once(INVALID_OVERRIDE).await?;

    assert!(
        !output.status.success(),
        "codex-app-server succeeded unexpectedly: {}",
        output.status
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("error parsing -c overrides"),
        "stderr missing parse error:\n{stderr}"
    );
    Ok(())
}

#[tokio::test]
async fn cli_overrides_are_logged() -> Result<()> {
    let (stderr_output, saw_provider) = run_full_handshake_and_capture(VALID_OVERRIDES).await?;

    assert!(
        saw_provider,
        "stderr never mentioned {PROVIDER_NEEDLE}; captured stderr:\n{stderr_output}"
    );
    Ok(())
}

async fn run_app_server_once(args: &[&str]) -> Result<std::process::Output> {
    let codex_home = TempDir::new().context("create temp CODEX_HOME")?;
    write_minimal_config(codex_home.path())?;

    let binary = cargo_bin("codex-app-server");
    let mut cmd = Command::new(binary);
    cmd.kill_on_drop(true);
    cmd.args(args)
        .env("CODEX_HOME", codex_home.path())
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let child = cmd.spawn().context("spawn codex-app-server")?;
    let output = timeout(Duration::from_secs(5), child.wait_with_output())
        .await
        .context("codex-app-server timed out waiting for output")?
        .context("wait for codex-app-server output")?;
    Ok(output)
}

async fn run_full_handshake_and_capture(args: &[&str]) -> Result<(String, bool)> {
    let codex_home = TempDir::new().context("create temp CODEX_HOME")?;
    write_minimal_config(codex_home.path())?;

    let binary = cargo_bin("codex-app-server");
    let mut cmd = Command::new(binary);
    cmd.kill_on_drop(true);
    cmd.args(args)
        .env("CODEX_HOME", codex_home.path())
        .env("RUST_LOG", "debug")
        .env("RUST_LOG_STYLE", "never")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().context("spawn codex-app-server")?;
    let mut stdin = child.stdin.take().context("stdin missing")?;
    let mut stdout = BufReader::new(child.stdout.take().context("stdout missing")?);
    let stderr = child.stderr.take().context("stderr missing")?;

    perform_handshake(&mut stdin, &mut stdout).await?;

    // Drop stdin to signal no further requests.
    drop(stdin);

    let (stderr_output, saw_provider) = capture_provider(stderr).await?;

    let _ = child.kill().await;
    let _ = child.wait().await;
    Ok((stderr_output, saw_provider))
}

async fn perform_handshake(
    stdin: &mut ChildStdin,
    stdout: &mut BufReader<ChildStdout>,
) -> Result<()> {
    send_json_line(
        stdin,
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "clientInfo": {
                    "name": "integration-test",
                    "title": null,
                    "version": "0.0.0"
                }
            }
        }),
    )
    .await?;
    expect_response(stdout, 1).await?;

    send_json_line(
        stdin,
        json!({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {}
        }),
    )
    .await?;

    send_json_line(
        stdin,
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "thread/start",
            "params": {
                "input": [
                    {"type": "text", "text": "Hello"}
                ]
            }
        }),
    )
    .await?;
    expect_response(stdout, 2).await?;

    Ok(())
}

async fn send_json_line(writer: &mut ChildStdin, value: Value) -> Result<()> {
    let mut line = serde_json::to_vec(&value)?;
    line.push(b'\n');
    writer.write_all(&line).await?;
    writer.flush().await?;
    Ok(())
}

async fn expect_response(reader: &mut BufReader<ChildStdout>, expected_id: i64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut line = String::new();
    loop {
        if Instant::now() >= deadline {
            anyhow::bail!("timed out waiting for response id {expected_id}");
        }
        line.clear();
        let remaining = deadline.saturating_duration_since(Instant::now());
        let bytes = timeout(remaining, reader.read_line(&mut line))
            .await
            .context("timed out reading stdout")??;
        if bytes == 0 {
            anyhow::bail!("stdout closed before receiving response id {expected_id}");
        }
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line.trim()).context("parse stdout JSON")?;
        if value.get("id").and_then(Value::as_i64) == Some(expected_id) {
            break;
        }
    }
    Ok(())
}

async fn capture_provider(stderr: ChildStderr) -> Result<(String, bool)> {
    let mut reader = BufReader::new(stderr);
    let mut stderr_output = String::new();
    let mut line = String::new();
    let mut saw_provider = false;
    let deadline = Instant::now() + Duration::from_secs(20);

    loop {
        if Instant::now() >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining, reader.read_line(&mut line)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(_)) => {
                stderr_output.push_str(&line);
                if line.contains(PROVIDER_NEEDLE) {
                    saw_provider = true;
                    break;
                }
                line.clear();
            }
            Ok(Err(err)) => return Err(err.into()),
            Err(_) => break,
        }
    }

    Ok((stderr_output, saw_provider))
}

fn write_minimal_config(path: &std::path::Path) -> Result<()> {
    std::fs::write(
        path.join("config.toml"),
        r#"model = "gpt-5.1-codex"
model_provider = "openai"
"#,
    )
    .context("write config.toml")
}
