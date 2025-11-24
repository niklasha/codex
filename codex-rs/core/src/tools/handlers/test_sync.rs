use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicI32;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Barrier;
use tokio::time::sleep;

use crate::function_tool::FunctionCallError;
use crate::tools::context::ToolInvocation;
use crate::tools::context::ToolOutput;
use crate::tools::context::ToolPayload;
use crate::tools::registry::ToolHandler;
use crate::tools::registry::ToolKind;

pub struct TestSyncHandler;

const DEFAULT_TIMEOUT_MS: u64 = 1_000;

static BARRIERS: OnceLock<tokio::sync::Mutex<HashMap<String, Arc<BarrierState>>>> = OnceLock::new();
static GLOBAL_METRICS: OnceLock<Arc<ConcurrencyMetrics>> = OnceLock::new();

struct BarrierState {
    id: String,
    barrier: Arc<Barrier>,
    participants: usize,
    metrics: Arc<ConcurrencyMetrics>,
}

struct ConcurrencyMetrics {
    active: AtomicI32,
    max_active: AtomicI32,
}

impl ConcurrencyMetrics {
    fn new() -> Self {
        Self {
            active: AtomicI32::new(0),
            max_active: AtomicI32::new(0),
        }
    }

    fn guard(self: &Arc<Self>) -> ConcurrencyGuard {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        ConcurrencyGuard {
            metrics: self.clone(),
        }
    }

    fn max_active(&self) -> i32 {
        self.max_active.load(Ordering::SeqCst)
    }
}

struct ConcurrencyGuard {
    metrics: Arc<ConcurrencyMetrics>,
}

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        self.metrics.active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Debug, Deserialize)]
struct BarrierArgs {
    id: String,
    participants: usize,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

#[derive(Debug, Deserialize)]
struct TestSyncArgs {
    #[serde(default)]
    sleep_before_ms: Option<u64>,
    #[serde(default)]
    sleep_after_ms: Option<u64>,
    #[serde(default)]
    barrier: Option<BarrierArgs>,
}

#[derive(Debug, Serialize)]
struct TestSyncResult {
    status: &'static str,
    max_concurrency: i32,
}

fn default_timeout_ms() -> u64 {
    DEFAULT_TIMEOUT_MS
}

fn barrier_map() -> &'static tokio::sync::Mutex<HashMap<String, Arc<BarrierState>>> {
    BARRIERS.get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
}

fn global_metrics() -> Arc<ConcurrencyMetrics> {
    GLOBAL_METRICS
        .get_or_init(|| Arc::new(ConcurrencyMetrics::new()))
        .clone()
}

async fn barrier_state(args: &BarrierArgs) -> Result<Arc<BarrierState>, FunctionCallError> {
    let mut map = barrier_map().lock().await;
    match map.entry(args.id.clone()) {
        Entry::Occupied(entry) => {
            let state = entry.get();
            if state.participants != args.participants {
                let existing = state.participants;
                let barrier_id = &args.id;
                return Err(FunctionCallError::RespondToModel(format!(
                    "barrier {barrier_id} already registered with {existing} participants"
                )));
            }
            Ok(state.clone())
        }
        Entry::Vacant(entry) => {
            let barrier = Arc::new(Barrier::new(args.participants));
            let state = Arc::new(BarrierState {
                id: args.id.clone(),
                barrier,
                participants: args.participants,
                metrics: Arc::new(ConcurrencyMetrics::new()),
            });
            entry.insert(state.clone());
            Ok(state)
        }
    }
}

#[async_trait]
impl ToolHandler for TestSyncHandler {
    fn kind(&self) -> ToolKind {
        ToolKind::Function
    }

    async fn handle(&self, invocation: ToolInvocation) -> Result<ToolOutput, FunctionCallError> {
        let ToolInvocation { payload, .. } = invocation;

        let arguments = match payload {
            ToolPayload::Function { arguments } => arguments,
            _ => {
                return Err(FunctionCallError::RespondToModel(
                    "test_sync_tool handler received unsupported payload".to_string(),
                ));
            }
        };

        let args: TestSyncArgs = serde_json::from_str(&arguments).map_err(|err| {
            FunctionCallError::RespondToModel(format!(
                "failed to parse function arguments: {err:?}"
            ))
        })?;

        let (metrics, barrier_state) = match &args.barrier {
            Some(barrier_args) => {
                let state = barrier_state(barrier_args).await?;
                (
                    state.metrics.clone(),
                    Some((state, barrier_args.timeout_ms)),
                )
            }
            None => (global_metrics(), None),
        };
        let _concurrency_guard = metrics.guard();

        if let Some(delay) = args.sleep_before_ms
            && delay > 0
        {
            sleep(Duration::from_millis(delay)).await;
        }

        if let Some((state, timeout_ms)) = barrier_state {
            wait_on_barrier(state, timeout_ms).await?;
        }

        if let Some(delay) = args.sleep_after_ms
            && delay > 0
        {
            sleep(Duration::from_millis(delay)).await;
        }

        let content = serde_json::to_string(&TestSyncResult {
            status: "ok",
            max_concurrency: metrics.max_active(),
        })
        .unwrap_or_else(|_| "ok".to_string());

        Ok(ToolOutput::Function {
            content,
            content_items: None,
            success: Some(true),
        })
    }
}

async fn wait_on_barrier(
    state: Arc<BarrierState>,
    timeout_ms: u64,
) -> Result<(), FunctionCallError> {
    if state.participants == 0 {
        return Err(FunctionCallError::RespondToModel(
            "barrier participants must be greater than zero".to_string(),
        ));
    }

    if timeout_ms == 0 {
        return Err(FunctionCallError::RespondToModel(
            "barrier timeout must be greater than zero".to_string(),
        ));
    }

    let timeout = Duration::from_millis(timeout_ms);
    let wait_result = tokio::time::timeout(timeout, state.barrier.wait())
        .await
        .map_err(|_| {
            FunctionCallError::RespondToModel("test_sync_tool barrier wait timed out".to_string())
        })?;

    if wait_result.is_leader() {
        let mut map = barrier_map().lock().await;
        if let Some(entry) = map.get(&state.id)
            && Arc::ptr_eq(&entry.barrier, &state.barrier)
        {
            map.remove(&state.id);
        }
    }

    Ok(())
}
