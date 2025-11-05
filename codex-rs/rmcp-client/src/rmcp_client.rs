use std::collections::HashMap;
use std::ffi::OsString;
use std::io;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use anyhow::anyhow;
use futures::FutureExt;
use futures::future::BoxFuture;
use mcp_types::CallToolRequestParams;
use mcp_types::CallToolResult;
use mcp_types::InitializeRequestParams;
use mcp_types::InitializeResult;
use mcp_types::ListResourceTemplatesRequestParams;
use mcp_types::ListResourceTemplatesResult;
use mcp_types::ListResourcesRequestParams;
use mcp_types::ListResourcesResult;
use mcp_types::ListToolsRequestParams;
use mcp_types::ListToolsResult;
use mcp_types::ReadResourceRequestParams;
use mcp_types::ReadResourceResult;
use reqwest::header::AUTHORIZATION;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use rmcp::model::CallToolRequestParam;
use rmcp::model::InitializeRequestParam;
use rmcp::model::PaginatedRequestParam;
use rmcp::model::ReadResourceRequestParam;
use rmcp::service::ClientInitializeError;
use rmcp::service::RoleClient;
use rmcp::service::RunningService;
use rmcp::service::{self};
use rmcp::transport::SseClientTransport;
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::auth::AuthClient;
use rmcp::transport::auth::OAuthState;
use rmcp::transport::child_process::TokioChildProcess;
use rmcp::transport::sse_client::SseClientConfig;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::process::Command;
use tokio::sync::Mutex;
use tokio::time;
use tracing::info;
use tracing::warn;

use crate::load_oauth_tokens;
use crate::logging_client_handler::LoggingClientHandler;
use crate::oauth::OAuthCredentialsStoreMode;
use crate::oauth::OAuthPersistor;
use crate::oauth::StoredOAuthTokens;
use crate::utils::apply_default_headers;
use crate::utils::build_default_headers;
use crate::utils::convert_call_tool_result;
use crate::utils::convert_to_mcp;
use crate::utils::convert_to_rmcp;
use crate::utils::create_env_for_mcp_server;
use crate::utils::run_with_timeout;

enum PendingTransport {
    ChildProcess(TokioChildProcess),
    StreamableHttp {
        transport: StreamableHttpClientTransport<reqwest::Client>,
    },
    StreamableHttpWithOAuth {
        transport: StreamableHttpClientTransport<AuthClient<reqwest::Client>>,
        oauth_persistor: OAuthPersistor,
    },
    LegacySse {
        transport: SseClientTransport<reqwest::Client>,
    },
    LegacySseWithOAuth {
        transport: SseClientTransport<AuthClient<reqwest::Client>>,
        oauth_persistor: OAuthPersistor,
    },
}

impl PendingTransport {
    fn into_service_future(
        self,
        handler: LoggingClientHandler,
    ) -> (
        BoxFuture<
            'static,
            Result<RunningService<RoleClient, LoggingClientHandler>, ClientInitializeError>,
        >,
        Option<OAuthPersistor>,
    ) {
        match self {
            PendingTransport::ChildProcess(transport) => {
                (service::serve_client(handler, transport).boxed(), None)
            }
            PendingTransport::StreamableHttp { transport } => {
                (service::serve_client(handler, transport).boxed(), None)
            }
            PendingTransport::StreamableHttpWithOAuth {
                transport,
                oauth_persistor,
            } => (
                service::serve_client(handler, transport).boxed(),
                Some(oauth_persistor),
            ),
            PendingTransport::LegacySse { transport } => {
                (service::serve_client(handler, transport).boxed(), None)
            }
            PendingTransport::LegacySseWithOAuth {
                transport,
                oauth_persistor,
            } => (
                service::serve_client(handler, transport).boxed(),
                Some(oauth_persistor),
            ),
        }
    }
}

#[derive(Clone)]
struct StreamableHttpSetup {
    server_name: String,
    url: String,
    bearer_token: Option<String>,
    default_headers: HeaderMap,
    store_mode: OAuthCredentialsStoreMode,
    initial_oauth_tokens: Option<StoredOAuthTokens>,
}

impl StreamableHttpSetup {
    async fn create_streamable_transport(&self) -> Result<PendingTransport> {
        if let Some(tokens) = &self.initial_oauth_tokens {
            let (transport, oauth_persistor) = create_oauth_transport_and_runtime(
                &self.server_name,
                &self.url,
                tokens.clone(),
                self.store_mode,
                self.default_headers.clone(),
            )
            .await?;
            Ok(PendingTransport::StreamableHttpWithOAuth {
                transport,
                oauth_persistor,
            })
        } else {
            let mut http_config = StreamableHttpClientTransportConfig::with_uri(self.url.clone());
            if let Some(token) = &self.bearer_token {
                http_config = http_config.auth_header(token.clone());
            }
            let http_client =
                apply_default_headers(reqwest::Client::builder(), &self.default_headers).build()?;
            let transport = StreamableHttpClientTransport::with_client(http_client, http_config);
            Ok(PendingTransport::StreamableHttp { transport })
        }
    }

    async fn create_legacy_transport(&self) -> Result<PendingTransport> {
        if let Some(tokens) = &self.initial_oauth_tokens {
            let (transport, oauth_persistor) = create_oauth_sse_transport_and_runtime(
                &self.server_name,
                &self.url,
                tokens.clone(),
                self.store_mode,
                self.default_headers.clone(),
            )
            .await?;
            Ok(PendingTransport::LegacySseWithOAuth {
                transport,
                oauth_persistor,
            })
        } else {
            let mut headers = self.default_headers.clone();
            if let Some(token) = &self.bearer_token {
                let header_value = HeaderValue::from_str(&format!("Bearer {token}"))?;
                headers.insert(AUTHORIZATION, header_value);
            }
            let http_client =
                apply_default_headers(reqwest::Client::builder(), &headers).build()?;
            let transport = SseClientTransport::start_with_client(
                http_client,
                SseClientConfig {
                    sse_endpoint: Arc::<str>::from(self.url.clone()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|err| anyhow!("failed to start legacy SSE transport: {err}"))?;
            Ok(PendingTransport::LegacySse { transport })
        }
    }
}

enum ClientState {
    Connecting {
        transport: Option<PendingTransport>,
        streamable_setup: Option<StreamableHttpSetup>,
    },
    Ready {
        service: Arc<RunningService<RoleClient, LoggingClientHandler>>,
        oauth: Option<OAuthPersistor>,
    },
}

/// MCP client implemented on top of the official `rmcp` SDK.
/// https://github.com/modelcontextprotocol/rust-sdk
pub struct RmcpClient {
    state: Mutex<ClientState>,
}

impl RmcpClient {
    pub async fn new_stdio_client(
        program: OsString,
        args: Vec<OsString>,
        env: Option<HashMap<String, String>>,
        env_vars: &[String],
        cwd: Option<PathBuf>,
    ) -> io::Result<Self> {
        let program_name = program.to_string_lossy().into_owned();
        let mut command = Command::new(&program);
        command
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .env_clear()
            .envs(create_env_for_mcp_server(env, env_vars))
            .args(&args);
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }

        let (transport, stderr) = TokioChildProcess::builder(command)
            .stderr(Stdio::piped())
            .spawn()?;

        if let Some(stderr) = stderr {
            tokio::spawn(async move {
                let mut reader = BufReader::new(stderr).lines();
                loop {
                    match reader.next_line().await {
                        Ok(Some(line)) => {
                            info!("MCP server stderr ({program_name}): {line}");
                        }
                        Ok(None) => break,
                        Err(error) => {
                            warn!("Failed to read MCP server stderr ({program_name}): {error}");
                            break;
                        }
                    }
                }
            });
        }

        Ok(Self {
            state: Mutex::new(ClientState::Connecting {
                transport: Some(PendingTransport::ChildProcess(transport)),
                streamable_setup: None,
            }),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn new_streamable_http_client(
        server_name: &str,
        url: &str,
        bearer_token: Option<String>,
        http_headers: Option<HashMap<String, String>>,
        env_http_headers: Option<HashMap<String, String>>,
        store_mode: OAuthCredentialsStoreMode,
    ) -> Result<Self> {
        let default_headers = build_default_headers(http_headers, env_http_headers)?;

        let initial_oauth_tokens = match bearer_token {
            Some(_) => None,
            None => match load_oauth_tokens(server_name, url, store_mode) {
                Ok(tokens) => tokens,
                Err(err) => {
                    warn!("failed to read tokens for server `{server_name}`: {err}");
                    None
                }
            },
        };

        let setup = StreamableHttpSetup {
            server_name: server_name.to_string(),
            url: url.to_string(),
            bearer_token: bearer_token.clone(),
            default_headers: default_headers.clone(),
            store_mode,
            initial_oauth_tokens: initial_oauth_tokens.clone(),
        };
        let transport = setup.create_streamable_transport().await?;
        Ok(Self {
            state: Mutex::new(ClientState::Connecting {
                transport: Some(transport),
                streamable_setup: Some(setup),
            }),
        })
    }

    /// Perform the initialization handshake with the MCP server.
    /// https://modelcontextprotocol.io/specification/2025-06-18/basic/lifecycle#initialization
    pub async fn initialize(
        &self,
        params: InitializeRequestParams,
        timeout: Option<Duration>,
    ) -> Result<InitializeResult> {
        let rmcp_params: InitializeRequestParam = convert_to_rmcp(params.clone())?;
        let client_handler = LoggingClientHandler::new(rmcp_params);

        let mut attempted_legacy = false;

        loop {
            let (transport_future, oauth_persistor, streamable_setup) = {
                let mut guard = self.state.lock().await;
                match &mut *guard {
                    ClientState::Connecting {
                        transport,
                        streamable_setup,
                    } => {
                        let setup = streamable_setup.clone();
                        let pending = transport
                            .take()
                            .ok_or_else(|| anyhow!("client already initializing"))?;
                        let (future, oauth) = pending.into_service_future(client_handler.clone());
                        (future, oauth, setup)
                    }
                    ClientState::Ready { .. } => return Err(anyhow!("client already initialized")),
                }
            };

            let setup_for_retry = streamable_setup.clone();

            let service_result = if let Some(duration) = timeout {
                match time::timeout(duration, transport_future).await {
                    Ok(result) => result,
                    Err(_) => {
                        if let Some(setup) = setup_for_retry
                            && let Ok(new_transport) = setup.create_streamable_transport().await
                        {
                            let mut guard = self.state.lock().await;
                            if let ClientState::Connecting { transport, .. } = &mut *guard {
                                *transport = Some(new_transport);
                            }
                        }
                        return Err(anyhow!(
                            "timed out handshaking with MCP server after {duration:?}"
                        ));
                    }
                }
            } else {
                transport_future.await
            };

            match service_result {
                Ok(service) => {
                    let initialize_result_rmcp = service.peer().peer_info().ok_or_else(|| {
                        anyhow!("handshake succeeded but server info was missing")
                    })?;
                    let initialize_result = convert_to_mcp(initialize_result_rmcp)?;

                    {
                        let mut guard = self.state.lock().await;
                        *guard = ClientState::Ready {
                            service: Arc::new(service),
                            oauth: oauth_persistor.clone(),
                        };
                    }

                    if let Some(runtime) = oauth_persistor
                        && let Err(error) = runtime.persist_if_needed().await
                    {
                        warn!("failed to persist OAuth tokens after initialize: {error}");
                    }

                    return Ok(initialize_result);
                }
                Err(err) => {
                    if !attempted_legacy
                        && streamable_setup.is_some()
                        && should_attempt_legacy(&err)
                        && let Some(setup) = streamable_setup.clone()
                    {
                        match setup.create_legacy_transport().await {
                            Ok(new_transport) => {
                                {
                                    let mut guard = self.state.lock().await;
                                    if let ClientState::Connecting { transport, .. } = &mut *guard {
                                        *transport = Some(new_transport);
                                    }
                                }
                                attempted_legacy = true;
                                tracing::debug!(
                                    "Retrying MCP handshake using legacy SSE fallback transport"
                                );
                                continue;
                            }
                            Err(build_err) => {
                                tracing::warn!(
                                    "failed to build legacy SSE transport for MCP server `{}`: {build_err:?}",
                                    setup.server_name
                                );
                            }
                        }
                    }

                    if let Some(setup) = streamable_setup {
                        match setup.create_streamable_transport().await {
                            Ok(new_transport) => {
                                let mut guard = self.state.lock().await;
                                if let ClientState::Connecting { transport, .. } = &mut *guard {
                                    *transport = Some(new_transport);
                                }
                            }
                            Err(build_err) => {
                                tracing::warn!(
                                    "failed to rebuild streamable transport for MCP server `{}` after handshake error: {build_err:?}",
                                    setup.server_name
                                );
                            }
                        }
                    }

                    return Err(anyhow!("handshaking with MCP server failed: {err}"));
                }
            }
        }
    }

    pub async fn list_tools(
        &self,
        params: Option<ListToolsRequestParams>,
        timeout: Option<Duration>,
    ) -> Result<ListToolsResult> {
        let service = self.service().await?;
        let rmcp_params = params
            .map(convert_to_rmcp::<_, PaginatedRequestParam>)
            .transpose()?;

        let fut = service.list_tools(rmcp_params);
        let result = run_with_timeout(fut, timeout, "tools/list").await?;
        let converted = convert_to_mcp(result)?;
        self.persist_oauth_tokens().await;
        Ok(converted)
    }

    pub async fn list_resources(
        &self,
        params: Option<ListResourcesRequestParams>,
        timeout: Option<Duration>,
    ) -> Result<ListResourcesResult> {
        let service = self.service().await?;
        let rmcp_params = params
            .map(convert_to_rmcp::<_, PaginatedRequestParam>)
            .transpose()?;

        let fut = service.list_resources(rmcp_params);
        let result = run_with_timeout(fut, timeout, "resources/list").await?;
        let converted = convert_to_mcp(result)?;
        self.persist_oauth_tokens().await;
        Ok(converted)
    }

    pub async fn list_resource_templates(
        &self,
        params: Option<ListResourceTemplatesRequestParams>,
        timeout: Option<Duration>,
    ) -> Result<ListResourceTemplatesResult> {
        let service = self.service().await?;
        let rmcp_params = params
            .map(convert_to_rmcp::<_, PaginatedRequestParam>)
            .transpose()?;

        let fut = service.list_resource_templates(rmcp_params);
        let result = run_with_timeout(fut, timeout, "resources/templates/list").await?;
        let converted = convert_to_mcp(result)?;
        self.persist_oauth_tokens().await;
        Ok(converted)
    }

    pub async fn read_resource(
        &self,
        params: ReadResourceRequestParams,
        timeout: Option<Duration>,
    ) -> Result<ReadResourceResult> {
        let service = self.service().await?;
        let rmcp_params: ReadResourceRequestParam = convert_to_rmcp(params)?;
        let fut = service.read_resource(rmcp_params);
        let result = run_with_timeout(fut, timeout, "resources/read").await?;
        let converted = convert_to_mcp(result)?;
        self.persist_oauth_tokens().await;
        Ok(converted)
    }

    pub async fn call_tool(
        &self,
        name: String,
        arguments: Option<serde_json::Value>,
        timeout: Option<Duration>,
    ) -> Result<CallToolResult> {
        let service = self.service().await?;
        let params = CallToolRequestParams { arguments, name };
        let rmcp_params: CallToolRequestParam = convert_to_rmcp(params)?;
        let fut = service.call_tool(rmcp_params);
        let rmcp_result = run_with_timeout(fut, timeout, "tools/call").await?;
        let converted = convert_call_tool_result(rmcp_result)?;
        self.persist_oauth_tokens().await;
        Ok(converted)
    }

    async fn service(&self) -> Result<Arc<RunningService<RoleClient, LoggingClientHandler>>> {
        let guard = self.state.lock().await;
        match &*guard {
            ClientState::Ready { service, .. } => Ok(Arc::clone(service)),
            ClientState::Connecting { .. } => Err(anyhow!("MCP client not initialized")),
        }
    }

    async fn oauth_persistor(&self) -> Option<OAuthPersistor> {
        let guard = self.state.lock().await;
        match &*guard {
            ClientState::Ready {
                oauth: Some(runtime),
                service: _,
            } => Some(runtime.clone()),
            _ => None,
        }
    }

    /// This should be called after every tool call so that if a given tool call triggered
    /// a refresh of the OAuth tokens, they are persisted.
    async fn persist_oauth_tokens(&self) {
        if let Some(runtime) = self.oauth_persistor().await
            && let Err(error) = runtime.persist_if_needed().await
        {
            warn!("failed to persist OAuth tokens: {error}");
        }
    }
}

async fn create_oauth_transport_and_runtime(
    server_name: &str,
    url: &str,
    initial_tokens: StoredOAuthTokens,
    credentials_store: OAuthCredentialsStoreMode,
    default_headers: HeaderMap,
) -> Result<(
    StreamableHttpClientTransport<AuthClient<reqwest::Client>>,
    OAuthPersistor,
)> {
    let http_client =
        apply_default_headers(reqwest::Client::builder(), &default_headers).build()?;
    let mut oauth_state = OAuthState::new(url.to_string(), Some(http_client.clone())).await?;

    oauth_state
        .set_credentials(
            &initial_tokens.client_id,
            initial_tokens.token_response.0.clone(),
        )
        .await?;

    let manager = match oauth_state {
        OAuthState::Authorized(manager) => manager,
        OAuthState::Unauthorized(manager) => manager,
        OAuthState::Session(_) | OAuthState::AuthorizedHttpClient(_) => {
            return Err(anyhow!("unexpected OAuth state during client setup"));
        }
    };

    let auth_client = AuthClient::new(http_client, manager);
    let auth_manager = auth_client.auth_manager.clone();

    let transport = StreamableHttpClientTransport::with_client(
        auth_client,
        StreamableHttpClientTransportConfig::with_uri(url.to_string()),
    );

    let runtime = OAuthPersistor::new(
        server_name.to_string(),
        url.to_string(),
        auth_manager,
        credentials_store,
        Some(initial_tokens),
    );

    Ok((transport, runtime))
}

async fn create_oauth_sse_transport_and_runtime(
    server_name: &str,
    url: &str,
    initial_tokens: StoredOAuthTokens,
    credentials_store: OAuthCredentialsStoreMode,
    default_headers: HeaderMap,
) -> Result<(
    SseClientTransport<AuthClient<reqwest::Client>>,
    OAuthPersistor,
)> {
    let http_client =
        apply_default_headers(reqwest::Client::builder(), &default_headers).build()?;
    let mut oauth_state = OAuthState::new(url.to_string(), Some(http_client.clone())).await?;

    oauth_state
        .set_credentials(
            &initial_tokens.client_id,
            initial_tokens.token_response.0.clone(),
        )
        .await?;

    let manager = match oauth_state {
        OAuthState::Authorized(manager) => manager,
        OAuthState::Unauthorized(manager) => manager,
        OAuthState::Session(_) | OAuthState::AuthorizedHttpClient(_) => {
            return Err(anyhow!("unexpected OAuth state during client setup"));
        }
    };

    let auth_client = AuthClient::new(http_client, manager);
    let auth_manager = auth_client.auth_manager.clone();

    let transport = SseClientTransport::start_with_client(
        auth_client,
        SseClientConfig {
            sse_endpoint: Arc::<str>::from(url.to_string()),
            ..Default::default()
        },
    )
    .await
    .map_err(|err| anyhow!("failed to start legacy SSE transport with OAuth: {err}"))?;

    let runtime = OAuthPersistor::new(
        server_name.to_string(),
        url.to_string(),
        auth_manager,
        credentials_store,
        Some(initial_tokens),
    );

    Ok((transport, runtime))
}

fn should_attempt_legacy(error: &ClientInitializeError) -> bool {
    matches!(
        error,
        ClientInitializeError::TransportError { error, .. }
            if error.is::<StreamableHttpClientTransport<reqwest::Client>, RoleClient>()
                || error.is::<StreamableHttpClientTransport<AuthClient<reqwest::Client>>, RoleClient>()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::transport::streamable_http_client::StreamableHttpError;

    #[test]
    fn should_attempt_legacy_for_streamable_client_error() {
        let err = ClientInitializeError::transport::<StreamableHttpClientTransport<reqwest::Client>>(
            StreamableHttpError::<reqwest::Error>::ServerDoesNotSupportSse,
            "context",
        );
        assert!(should_attempt_legacy(&err));
    }

    #[test]
    fn should_attempt_legacy_for_streamable_client_oauth_error() {
        let err = ClientInitializeError::transport::<
            StreamableHttpClientTransport<AuthClient<reqwest::Client>>,
        >(
            StreamableHttpError::<reqwest::Error>::ServerDoesNotSupportSse,
            "context",
        );
        assert!(should_attempt_legacy(&err));
    }

    #[test]
    fn should_not_attempt_legacy_for_other_errors() {
        let err = ClientInitializeError::ConnectionClosed("boom".into());
        assert!(!should_attempt_legacy(&err));
    }
}
