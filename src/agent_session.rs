//! Persistent MongoDB command, cursor, and change-stream agent sessions.

use async_trait::async_trait;
use bson::Document;
use chrono::Utc;
use serde_json::{Value, json};
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use voidb_core::{
    AGENT_LIVE_SESSION_PROTOCOL_VERSION, AgentLiveSessionAuditIdentity,
    AgentLiveSessionBackpressureMode, AgentLiveSessionBufferOverflow, AgentLiveSessionBufferPolicy,
    AgentLiveSessionCallCancellation, AgentLiveSessionCancelBehavior, AgentLiveSessionCloseEffect,
    AgentLiveSessionContract, AgentLiveSessionControlPolicy, AgentLiveSessionCursor,
    AgentLiveSessionCursorKind, AgentLiveSessionCursorScopePolicy, AgentLiveSessionDeliveryPolicy,
    AgentLiveSessionEventInput, AgentLiveSessionEventKind, AgentLiveSessionHeartbeatPolicy,
    AgentLiveSessionKind, AgentLiveSessionOperations, AgentLiveSessionReadRequest,
    AgentLiveSessionReconnectMode, AgentLiveSessionReconnectPolicy,
    AgentLiveSessionResourceDescriptor, AgentLiveSessionResumeMode,
    AgentLiveSessionSourcePacedState, AgentLiveSessionStartRequest, AgentSessionCallRequest,
    AgentSessionCallResult, AgentSessionOpenContext, CapabilityRiskLevel, PluginAgentSession,
    PluginAgentSessionFactory, PluginSessionError, PluginSessionErrorCode, PluginSessionHealth,
    PluginSessionPurpose, RedactionStatus, RedactionTarget, agent_live_session_cursor_scope,
    collect_redaction_targets, redact_text_with_targets,
};

use crate::MongoConfig;
use crate::service::PersistentMongoSession;
use crate::service::agent_live::{
    MongoChangeEvent, MongoChangeStreamSource, MongoCursorItem, MongoCursorKind, MongoCursorSource,
    MongoLiveErrorKind,
};

pub(crate) const CURSOR_READ_CAPABILITY: &str = "mongodb.cursor_read";
pub(crate) const CHANGE_STREAM_READ_CAPABILITY: &str = "mongodb.change_stream_read";

const DEFAULT_BATCH_SIZE: usize = 100;
const MAX_BATCH_SIZE: usize = 500;
const DEFAULT_MAX_TIME_MS: u64 = 30_000;
const DEFAULT_MAX_AWAIT_MS: u64 = 5_000;
const MIN_MAX_AWAIT_MS: u64 = 250;
const MAX_WAIT_MS: u64 = 30_000;
const HEARTBEAT_INTERVAL_MS: u64 = 10_000;
const IDLE_TIMEOUT_MS: u64 = 60_000;

pub struct MongoAgentSessionFactory {
    config: MongoConfig,
    redaction_targets: Arc<Vec<RedactionTarget>>,
}

impl MongoAgentSessionFactory {
    pub fn new(config: MongoConfig) -> Self {
        let redaction_targets = serde_json::to_value(&config)
            .map(|value| collect_redaction_targets(&value))
            .unwrap_or_default();
        Self {
            config,
            redaction_targets: Arc::new(redaction_targets),
        }
    }
}

#[async_trait]
impl PluginAgentSessionFactory for MongoAgentSessionFactory {
    fn plugin_id(&self) -> &str {
        "mongodb"
    }

    async fn open(
        &self,
        context: AgentSessionOpenContext,
    ) -> Result<Arc<dyn PluginAgentSession>, PluginSessionError> {
        let family = MongoSessionFamily::from_binding(&context)?;
        if family == MongoSessionFamily::Command {
            let session = PersistentMongoSession::open(&self.config)
                .await
                .map_err(|_| owner_error("MongoDB driver session could not be opened."))?;
            return Ok(Arc::new(MongoCommandSession {
                session: Mutex::new(Some(session)),
                redaction_targets: Arc::clone(&self.redaction_targets),
            }));
        }

        let capability = family.capability();
        let (purpose, contract) =
            mongodb_live_session_contract(capability).expect("MongoDB live family has a contract");
        if context.binding.purpose != purpose {
            return Err(error(
                PluginSessionErrorCode::BindingMismatch,
                "The MongoDB live-session purpose does not match its capability family.",
            ));
        }
        contract.validate(&context.binding.allowed_capabilities)?;
        contract.validate_start(&context.request.input)?;
        let start: AgentLiveSessionStartRequest =
            serde_json::from_value(context.request.input.clone()).map_err(|_| {
                error(
                    PluginSessionErrorCode::PolicyDenied,
                    "The MongoDB live-session start envelope is invalid.",
                )
            })?;
        let (database, collection) = resource_namespace(&start.resource)?;

        let source = match family {
            MongoSessionFamily::Cursor => {
                let kind = cursor_kind(&start.parameters)?;
                let filter = optional_document(&start.parameters, "filter")?.unwrap_or_default();
                let pipeline = document_array(&start.parameters, "pipeline", 64)?;
                let sort = optional_document(&start.parameters, "sort")?;
                let batch_size = parameter_usize(
                    &start.parameters,
                    "batch_size",
                    DEFAULT_BATCH_SIZE,
                    1,
                    MAX_BATCH_SIZE,
                )?;
                let max_time_ms = parameter_u64(
                    &start.parameters,
                    "max_time_ms",
                    DEFAULT_MAX_TIME_MS,
                    1,
                    MAX_WAIT_MS,
                )?;
                let source = MongoCursorSource::open(
                    &self.config,
                    &database,
                    &collection,
                    kind,
                    filter,
                    pipeline,
                    sort,
                    batch_size as u32,
                    max_time_ms,
                )
                .await
                .map_err(|_| owner_error("MongoDB persistent cursor could not be opened."))?;
                MongoLiveSource::Cursor(Box::new(Mutex::new(Some(MongoCursorSession {
                    source,
                    pending: VecDeque::new(),
                    delivery: AgentLiveSessionSourcePacedState::default(),
                    batch_size,
                    max_time_ms,
                    redaction_targets: Arc::clone(&self.redaction_targets),
                }))))
            }
            MongoSessionFamily::ChangeStream => {
                let pipeline_value = start
                    .parameters
                    .get("pipeline")
                    .cloned()
                    .unwrap_or_else(|| json!([]));
                let pipeline = document_array(&start.parameters, "pipeline", 32)?;
                let batch_size = parameter_usize(
                    &start.parameters,
                    "batch_size",
                    DEFAULT_BATCH_SIZE,
                    1,
                    MAX_BATCH_SIZE,
                )?;
                let max_await_ms = parameter_u64(
                    &start.parameters,
                    "max_await_ms",
                    DEFAULT_MAX_AWAIT_MS,
                    MIN_MAX_AWAIT_MS,
                    MAX_WAIT_MS,
                )?;
                let full_document = start
                    .parameters
                    .get("full_document")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let scope = agent_live_session_cursor_scope(
                    capability,
                    &json!({
                        "resource": start.resource.clone(),
                        "pipeline": pipeline_value,
                        "full_document": full_document
                    }),
                )?;
                if start
                    .resume_from
                    .as_ref()
                    .and_then(|cursor| cursor.scope.as_deref())
                    .is_some_and(|supplied| supplied != scope)
                {
                    return Err(error(
                        PluginSessionErrorCode::BindingMismatch,
                        "The MongoDB resume cursor belongs to a different collection or pipeline.",
                    ));
                }
                let resume_token = start
                    .resume_from
                    .as_ref()
                    .map(|cursor| cursor.value.as_str());
                let source = MongoChangeStreamSource::open(
                    &self.config,
                    database,
                    collection,
                    pipeline,
                    batch_size as u32,
                    max_await_ms,
                    full_document,
                    resume_token,
                )
                .await
                .map_err(|_| owner_error("MongoDB change stream could not be opened."))?;
                MongoLiveSource::ChangeStream(Box::new(Mutex::new(Some(MongoChangeSession {
                    source,
                    pending: VecDeque::new(),
                    delivery: AgentLiveSessionSourcePacedState::default(),
                    scope,
                    batch_size,
                    max_await_ms,
                    exhausted: false,
                    latest_resume_token: None,
                    last_heartbeat: Instant::now(),
                    redaction_targets: Arc::clone(&self.redaction_targets),
                }))))
            }
            MongoSessionFamily::Command => unreachable!(),
        };

        Ok(Arc::new(MongoAgentLiveSession {
            family,
            contract,
            source,
            cancellations: AgentLiveSessionCallCancellation::default(),
            closed: AtomicBool::new(false),
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MongoSessionFamily {
    Command,
    Cursor,
    ChangeStream,
}

impl MongoSessionFamily {
    fn from_binding(context: &AgentSessionOpenContext) -> Result<Self, PluginSessionError> {
        let actual = context
            .binding
            .allowed_capabilities
            .iter()
            .map(|capability| capability.strip_prefix("mongodb.").unwrap_or(capability))
            .collect::<HashSet<_>>();
        let family = if actual == HashSet::from(["run_command"]) {
            Self::Command
        } else if actual == HashSet::from(["cursor_read"]) {
            Self::Cursor
        } else if actual == HashSet::from(["change_stream_read"]) {
            Self::ChangeStream
        } else {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "A MongoDB session requires one complete, unmixed capability family.",
            ));
        };
        let purpose_valid = match family {
            Self::Command => matches!(
                context.binding.purpose,
                PluginSessionPurpose::DatabaseQuery | PluginSessionPurpose::DatabaseTransaction
            ),
            Self::Cursor => context.binding.purpose == PluginSessionPurpose::DatabaseQuery,
            Self::ChangeStream => context.binding.purpose == PluginSessionPurpose::WatchStream,
        };
        if !purpose_valid {
            return Err(error(
                PluginSessionErrorCode::BindingMismatch,
                "The MongoDB session purpose does not match its capability family.",
            ));
        }
        Ok(family)
    }

    fn capability(self) -> &'static str {
        match self {
            Self::Command => "mongodb.run_command",
            Self::Cursor => CURSOR_READ_CAPABILITY,
            Self::ChangeStream => CHANGE_STREAM_READ_CAPABILITY,
        }
    }
}

struct MongoCommandSession {
    session: Mutex<Option<PersistentMongoSession>>,
    redaction_targets: Arc<Vec<RedactionTarget>>,
}

#[async_trait]
impl PluginAgentSession for MongoCommandSession {
    async fn call(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        if request.capability != "mongodb.run_command" {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "MongoDB command sessions accept mongodb.run_command.",
            ));
        }
        if !request.destructive_acknowledged {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "mongodb.run_command requires destructive acknowledgement.",
            ));
        }
        let mut guard = self.session.lock().await;
        let session = guard
            .as_mut()
            .ok_or_else(|| owner_error("MongoDB session is closed."))?;
        let output = match request.input.get("action").and_then(Value::as_str) {
            Some("begin") => {
                session.begin().await.map_err(owner_string_error)?;
                json!({"transaction":"started"})
            }
            Some("commit") => {
                session.commit().await.map_err(owner_string_error)?;
                json!({"transaction":"committed"})
            }
            Some("abort") => {
                session.abort().await.map_err(owner_string_error)?;
                json!({"transaction":"aborted"})
            }
            Some(_) => {
                return Err(error(
                    PluginSessionErrorCode::PolicyDenied,
                    "Unknown MongoDB session action.",
                ));
            }
            None => {
                let command: Document =
                    bson::to_document(request.input.get("command").ok_or_else(|| {
                        error(
                            PluginSessionErrorCode::PolicyDenied,
                            "MongoDB command is required.",
                        )
                    })?)
                    .map_err(|_| {
                        error(
                            PluginSessionErrorCode::PolicyDenied,
                            "MongoDB command must be an object.",
                        )
                    })?;
                let database = request.input.get("database").and_then(Value::as_str);
                serde_json::to_value(
                    session
                        .run_command(database, command)
                        .await
                        .map_err(owner_string_error)?,
                )
                .map_err(|_| owner_error("MongoDB output could not be encoded."))?
            }
        };
        let encoded = serde_json::to_string(&output)
            .map_err(|_| owner_error("MongoDB output could not be encoded."))?;
        let (redacted, _) = redact_text_with_targets(&encoded, &self.redaction_targets);
        let output = serde_json::from_str(&redacted).unwrap_or_else(|_| json!({"redacted":true}));
        AgentSessionCallResult::bounded(request.call_id, output, request.output_limit_bytes)
    }

    async fn health(&self) -> Result<PluginSessionHealth, PluginSessionError> {
        Ok(if self.session.lock().await.is_some() {
            PluginSessionHealth::Ready
        } else {
            PluginSessionHealth::Closed
        })
    }

    async fn cancel(&self, _call_id: &str) -> Result<(), PluginSessionError> {
        close_command_session(&self.session).await;
        Ok(())
    }

    async fn close(&self, _reason: String) -> Result<(), PluginSessionError> {
        close_command_session(&self.session).await;
        Ok(())
    }
}

enum MongoLiveSource {
    Cursor(Box<Mutex<Option<MongoCursorSession>>>),
    ChangeStream(Box<Mutex<Option<MongoChangeSession>>>),
}

struct MongoCursorSession {
    source: MongoCursorSource,
    pending: VecDeque<MongoCursorItem>,
    delivery: AgentLiveSessionSourcePacedState,
    batch_size: usize,
    max_time_ms: u64,
    redaction_targets: Arc<Vec<RedactionTarget>>,
}

struct MongoChangeSession {
    source: MongoChangeStreamSource,
    pending: VecDeque<MongoChangeEvent>,
    delivery: AgentLiveSessionSourcePacedState,
    scope: String,
    batch_size: usize,
    max_await_ms: u64,
    exhausted: bool,
    latest_resume_token: Option<String>,
    last_heartbeat: Instant,
    redaction_targets: Arc<Vec<RedactionTarget>>,
}

struct MongoAgentLiveSession {
    family: MongoSessionFamily,
    contract: AgentLiveSessionContract,
    source: MongoLiveSource,
    cancellations: AgentLiveSessionCallCancellation,
    closed: AtomicBool,
}

#[async_trait]
impl PluginAgentSession for MongoAgentLiveSession {
    async fn call(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        if request.capability != self.family.capability() {
            return Err(error(
                PluginSessionErrorCode::PolicyDenied,
                "The MongoDB call is outside this live-session binding.",
            ));
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(owner_error("The MongoDB live session is closed."));
        }
        let mut read = read_request(&request)?;
        read.max_bytes = read.max_bytes.min(request.output_limit_bytes);
        let call_id = request.call_id.clone();
        let batch = match &self.source {
            MongoLiveSource::Cursor(state) => {
                self.cancellations
                    .run(&call_id, self.read_cursor(state, read))
                    .await?
            }
            MongoLiveSource::ChangeStream(state) => {
                self.cancellations
                    .run(&call_id, self.read_change_stream(state, read))
                    .await?
            }
        };
        let output = serde_json::to_value(batch).map_err(|_| {
            error(
                PluginSessionErrorCode::RedactionFailed,
                "The MongoDB live-session batch could not be serialized.",
            )
        })?;
        AgentSessionCallResult::bounded(request.call_id, output, request.output_limit_bytes)
    }

    async fn health(&self) -> Result<PluginSessionHealth, PluginSessionError> {
        Ok(if self.closed.load(Ordering::Acquire) {
            PluginSessionHealth::Closed
        } else {
            PluginSessionHealth::Ready
        })
    }

    async fn cancel(&self, call_id: &str) -> Result<(), PluginSessionError> {
        self.cancellations.cancel(call_id).await;
        self.close_live_source().await;
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    async fn close(&self, _reason: String) -> Result<(), PluginSessionError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.cancellations.close().await;
        self.close_live_source().await;
        Ok(())
    }
}

impl MongoAgentLiveSession {
    async fn read_cursor(
        &self,
        state: &Mutex<Option<MongoCursorSession>>,
        read: AgentLiveSessionReadRequest,
    ) -> Result<voidb_core::AgentLiveSessionEventBatch, PluginSessionError> {
        self.contract.validate_read(&read)?;
        let mut guard = state.lock().await;
        let state = guard
            .as_mut()
            .ok_or_else(|| owner_error("The MongoDB cursor is closed."))?;
        if state.pending.is_empty() && !state.source.exhausted() {
            let count = state.batch_size.min(read.max_events);
            let timeout_ms = state.max_time_ms.min(read.wait_timeout_ms.max(1));
            let items = state
                .source
                .read(count, timeout_ms)
                .await
                .map_err(|_| owner_error("MongoDB cursor read failed."))?;
            state.pending.extend(items);
        }
        let candidates = state
            .pending
            .iter()
            .map(|item| cursor_event(item, &state.redaction_targets))
            .collect::<Vec<_>>();
        let source_closed = state.source.exhausted();
        let (batch, consumed) = state.delivery.build_batch(
            &read,
            &self.contract,
            &candidates,
            candidates.is_empty() && !source_closed,
            source_closed,
        )?;
        state.pending.drain(..consumed.min(state.pending.len()));
        Ok(batch)
    }

    async fn read_change_stream(
        &self,
        state: &Mutex<Option<MongoChangeSession>>,
        read: AgentLiveSessionReadRequest,
    ) -> Result<voidb_core::AgentLiveSessionEventBatch, PluginSessionError> {
        self.contract.validate_read(&read)?;
        let mut guard = state.lock().await;
        let state = guard
            .as_mut()
            .ok_or_else(|| owner_error("The MongoDB change stream is closed."))?;
        if state.pending.is_empty() && !state.exhausted {
            let count = state.batch_size.min(read.max_events);
            let wait_ms = state.max_await_ms.min(read.wait_timeout_ms.max(1));
            let result = match state.source.read(count, wait_ms).await {
                Ok(result) => result,
                Err(live_error) if live_error.kind == MongoLiveErrorKind::Retryable => {
                    state.delivery.record_reconnect(&self.contract)?;
                    let resume = state
                        .delivery
                        .latest_cursor()
                        .map(|cursor| cursor.value.clone());
                    state
                        .source
                        .reconnect(resume.as_deref())
                        .await
                        .map_err(|_| owner_error("MongoDB change-stream resume failed."))?;
                    state.source.read(count, wait_ms).await.map_err(|_| {
                        owner_error("MongoDB change-stream read failed after resume.")
                    })?
                }
                Err(_) => return Err(owner_error("MongoDB change-stream read failed.")),
            };
            if result.latest_resume_token.is_some() {
                state.latest_resume_token = result.latest_resume_token;
            }
            state.exhausted = result.exhausted;
            state.pending.extend(result.events);
        }
        let mut candidates = state
            .pending
            .iter()
            .map(|event| change_event(event, &state.scope, &state.redaction_targets))
            .collect::<Vec<_>>();
        let heartbeat_due =
            state.last_heartbeat.elapsed() >= Duration::from_millis(HEARTBEAT_INTERVAL_MS);
        if candidates.is_empty()
            && !state.exhausted
            && heartbeat_due
            && let Some(resume_token) = state.latest_resume_token.clone()
        {
            candidates.push(AgentLiveSessionEventInput {
                observed_at: Utc::now(),
                kind: AgentLiveSessionEventKind::Heartbeat,
                data: Value::Null,
                cursor: Some(AgentLiveSessionCursor {
                    kind: AgentLiveSessionCursorKind::Opaque,
                    value: resume_token,
                    scope: Some(state.scope.clone()),
                }),
                redaction: RedactionStatus::NotRequired,
                terminal: false,
            });
        }
        let pending_count = state.pending.len();
        let source_closed = state.exhausted;
        let (batch, consumed) = state.delivery.build_batch(
            &read,
            &self.contract,
            &candidates,
            candidates.is_empty() && !source_closed,
            source_closed,
        )?;
        state.pending.drain(..consumed.min(pending_count));
        if heartbeat_due && consumed > pending_count {
            state.last_heartbeat = Instant::now();
        }
        Ok(batch)
    }

    async fn close_live_source(&self) {
        match &self.source {
            MongoLiveSource::Cursor(state) => {
                state.lock().await.take();
            }
            MongoLiveSource::ChangeStream(state) => {
                state.lock().await.take();
            }
        }
    }
}

pub(crate) fn mongodb_live_session_contract(
    capability: &str,
) -> Option<(PluginSessionPurpose, AgentLiveSessionContract)> {
    let (purpose, resource_type, parameters, event_schema, reconnect, delivery) = match capability {
        CURSOR_READ_CAPABILITY => (
            PluginSessionPurpose::DatabaseQuery,
            "mongodb_collection_cursor",
            json!({
                "type": "object",
                "properties": {
                    "mode": { "type": "string", "enum": ["find", "aggregate"], "default": "find" },
                    "filter": { "type": "object", "additionalProperties": true },
                    "pipeline": { "type": "array", "maxItems": 64, "items": { "type": "object", "additionalProperties": true } },
                    "sort": { "type": "object", "additionalProperties": true },
                    "batch_size": { "type": "integer", "minimum": 1, "maximum": MAX_BATCH_SIZE, "default": DEFAULT_BATCH_SIZE },
                    "max_time_ms": { "type": "integer", "minimum": 1, "maximum": MAX_WAIT_MS, "default": DEFAULT_MAX_TIME_MS }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["document", "document_omitted"],
                "properties": {
                    "document": {},
                    "document_omitted": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            AgentLiveSessionReconnectPolicy::default(),
            AgentLiveSessionDeliveryPolicy {
                backpressure: AgentLiveSessionBackpressureMode::SourcePaced,
                cursor_scope: AgentLiveSessionCursorScopePolicy::Optional,
                heartbeat: AgentLiveSessionHeartbeatPolicy::default(),
                max_read_wait_ms: MAX_WAIT_MS,
            },
        ),
        CHANGE_STREAM_READ_CAPABILITY => (
            PluginSessionPurpose::WatchStream,
            "mongodb_change_stream",
            json!({
                "type": "object",
                "properties": {
                    "pipeline": { "type": "array", "maxItems": 32, "items": { "type": "object", "additionalProperties": true } },
                    "batch_size": { "type": "integer", "minimum": 1, "maximum": MAX_BATCH_SIZE, "default": DEFAULT_BATCH_SIZE },
                    "max_await_ms": { "type": "integer", "minimum": MIN_MAX_AWAIT_MS, "maximum": MAX_WAIT_MS, "default": DEFAULT_MAX_AWAIT_MS },
                    "full_document": { "type": "boolean", "default": false }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": [
                    "operation_type", "namespace", "document_key", "document_key_omitted",
                    "full_document", "update_description", "update_description_omitted",
                    "cluster_time", "wall_time", "resume_token_omitted", "session_metadata_omitted"
                ],
                "properties": {
                    "operation_type": { "type": "string" },
                    "namespace": { "type": ["object", "null"] },
                    "document_key": {},
                    "document_key_omitted": { "type": "boolean" },
                    "full_document": {},
                    "update_description": {},
                    "update_description_omitted": { "type": "boolean" },
                    "cluster_time": { "type": ["string", "null"] },
                    "wall_time": { "type": ["string", "null"] },
                    "resume_token_omitted": { "const": true },
                    "session_metadata_omitted": { "const": true }
                },
                "additionalProperties": false
            }),
            AgentLiveSessionReconnectPolicy {
                mode: AgentLiveSessionReconnectMode::Transient,
                max_attempts: 3,
                initial_backoff_ms: 250,
                max_backoff_ms: 2_000,
                resume: AgentLiveSessionResumeMode::ExactCursor,
                cursor_kind: Some(AgentLiveSessionCursorKind::Opaque),
            },
            AgentLiveSessionDeliveryPolicy {
                backpressure: AgentLiveSessionBackpressureMode::SourcePaced,
                cursor_scope: AgentLiveSessionCursorScopePolicy::Required,
                heartbeat: AgentLiveSessionHeartbeatPolicy {
                    interval_ms: Some(HEARTBEAT_INTERVAL_MS),
                    idle_timeout_ms: Some(IDLE_TIMEOUT_MS),
                },
                max_read_wait_ms: MAX_WAIT_MS,
            },
        ),
        _ => return None,
    };
    Some((
        purpose,
        AgentLiveSessionContract {
            protocol_version: AGENT_LIVE_SESSION_PROTOCOL_VERSION,
            kind: AgentLiveSessionKind::Cursor,
            resource: AgentLiveSessionResourceDescriptor {
                resource_type: resource_type.into(),
                identity_schema: collection_resource_schema(),
                identity_fields: vec!["/database".into(), "/collection".into()],
                audit_identity: AgentLiveSessionAuditIdentity::Fingerprint,
            },
            start_parameters_schema: parameters,
            event_schema,
            operations: AgentLiveSessionOperations {
                events: capability.into(),
                input: None,
                resize: None,
                signal: None,
            },
            buffer: AgentLiveSessionBufferPolicy {
                max_events: MAX_BATCH_SIZE,
                max_bytes: 2 * 1024 * 1024,
                overflow: AgentLiveSessionBufferOverflow::DropOldest,
            },
            reconnect,
            delivery,
            control: AgentLiveSessionControlPolicy {
                cancel: AgentLiveSessionCancelBehavior::CallAndSource,
                close: AgentLiveSessionCloseEffect::StopObservation,
            },
            start_risk: CapabilityRiskLevel::ReadOnly,
        },
    ))
}

fn cursor_event(item: &MongoCursorItem, targets: &[RedactionTarget]) -> AgentLiveSessionEventInput {
    let (data, redaction) = redact_json(
        json!({
            "document": item.document,
            "document_omitted": item.document_omitted
        }),
        targets,
    );
    AgentLiveSessionEventInput {
        observed_at: Utc::now(),
        kind: AgentLiveSessionEventKind::Data,
        data,
        cursor: None,
        redaction,
        terminal: false,
    }
}

fn change_event(
    event: &MongoChangeEvent,
    scope: &str,
    targets: &[RedactionTarget],
) -> AgentLiveSessionEventInput {
    let (data, redaction) = redact_json(event.value.clone(), targets);
    AgentLiveSessionEventInput {
        observed_at: Utc::now(),
        kind: AgentLiveSessionEventKind::Data,
        data,
        cursor: Some(AgentLiveSessionCursor {
            kind: AgentLiveSessionCursorKind::Opaque,
            value: event.resume_token.clone(),
            scope: Some(scope.to_string()),
        }),
        redaction,
        terminal: false,
    }
}

fn redact_json(value: Value, targets: &[RedactionTarget]) -> (Value, RedactionStatus) {
    let Ok(encoded) = serde_json::to_string(&value) else {
        return (Value::Null, RedactionStatus::FailedClosed);
    };
    let (redacted, status) = redact_text_with_targets(&encoded, targets);
    match serde_json::from_str(&redacted) {
        Ok(value) => (value, status),
        Err(_) => (Value::Null, RedactionStatus::FailedClosed),
    }
}

fn collection_resource_schema() -> Value {
    json!({
        "type": "object",
        "required": ["database", "collection"],
        "properties": {
            "database": { "type": "string", "minLength": 1, "maxLength": 256 },
            "collection": { "type": "string", "minLength": 1, "maxLength": 256 }
        },
        "additionalProperties": false
    })
}

fn resource_namespace(resource: &Value) -> Result<(String, String), PluginSessionError> {
    Ok((
        required_string(resource, "database")?,
        required_string(resource, "collection")?,
    ))
}

fn cursor_kind(parameters: &Value) -> Result<MongoCursorKind, PluginSessionError> {
    match parameters
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or("find")
    {
        "find" => Ok(MongoCursorKind::Find),
        "aggregate" => Ok(MongoCursorKind::Aggregate),
        _ => Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "MongoDB cursor mode must be find or aggregate.",
        )),
    }
}

fn optional_document(input: &Value, field: &str) -> Result<Option<Document>, PluginSessionError> {
    input
        .get(field)
        .map(|value| {
            bson::to_document(value).map_err(|_| {
                error(
                    PluginSessionErrorCode::PolicyDenied,
                    "MongoDB cursor document parameter is invalid.",
                )
            })
        })
        .transpose()
}

fn document_array(
    input: &Value,
    field: &str,
    maximum: usize,
) -> Result<Vec<Document>, PluginSessionError> {
    let Some(values) = input.get(field) else {
        return Ok(Vec::new());
    };
    let values = values.as_array().ok_or_else(|| {
        error(
            PluginSessionErrorCode::PolicyDenied,
            "MongoDB pipeline must be an array of documents.",
        )
    })?;
    if values.len() > maximum {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "MongoDB pipeline exceeds the bounded stage limit.",
        ));
    }
    values
        .iter()
        .map(|value| {
            bson::to_document(value).map_err(|_| {
                error(
                    PluginSessionErrorCode::PolicyDenied,
                    "MongoDB pipeline stage is invalid.",
                )
            })
        })
        .collect()
}

fn read_request(
    request: &AgentSessionCallRequest,
) -> Result<AgentLiveSessionReadRequest, PluginSessionError> {
    if request.input.is_null() {
        Ok(AgentLiveSessionReadRequest::default())
    } else {
        serde_json::from_value(request.input.clone()).map_err(|_| {
            error(
                PluginSessionErrorCode::PolicyDenied,
                "The MongoDB live-session read request is invalid.",
            )
        })
    }
}

fn required_string(input: &Value, field: &str) -> Result<String, PluginSessionError> {
    input
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
        })
        .map(str::to_string)
        .ok_or_else(|| {
            error(
                PluginSessionErrorCode::PolicyDenied,
                "MongoDB collection identity is invalid.",
            )
        })
}

fn parameter_usize(
    parameters: &Value,
    field: &str,
    default: usize,
    minimum: usize,
    maximum: usize,
) -> Result<usize, PluginSessionError> {
    let value = parameters
        .get(field)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(default);
    if !(minimum..=maximum).contains(&value) {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "MongoDB live-session numeric parameter is outside its bound.",
        ));
    }
    Ok(value)
}

fn parameter_u64(
    parameters: &Value,
    field: &str,
    default: u64,
    minimum: u64,
    maximum: u64,
) -> Result<u64, PluginSessionError> {
    let value = parameters
        .get(field)
        .and_then(Value::as_u64)
        .unwrap_or(default);
    if !(minimum..=maximum).contains(&value) {
        return Err(error(
            PluginSessionErrorCode::PolicyDenied,
            "MongoDB live-session numeric parameter is outside its bound.",
        ));
    }
    Ok(value)
}

async fn close_command_session(session: &Mutex<Option<PersistentMongoSession>>) {
    if let Some(session) = session.lock().await.take() {
        session.close().await;
    }
}

fn owner_string_error(_: String) -> PluginSessionError {
    owner_error("MongoDB session operation failed.")
}

fn owner_error(message: &str) -> PluginSessionError {
    error(PluginSessionErrorCode::OwnerUnavailable, message)
}

fn error(code: PluginSessionErrorCode, message: &str) -> PluginSessionError {
    PluginSessionError::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live_context(
        capability: &str,
        purpose: PluginSessionPurpose,
        input: Value,
    ) -> AgentSessionOpenContext {
        AgentSessionOpenContext {
            binding: voidb_core::AgentSessionBinding {
                grant_id: "grant".into(),
                profile_id: "profile".into(),
                plugin_id: "mongodb".into(),
                purpose: purpose.clone(),
                allowed_capabilities: vec![capability.into()],
                host_generation: 1,
            },
            request: voidb_core::AgentSessionOpenRequest {
                purpose,
                capabilities: vec![capability.into()],
                lease_seconds: 60,
                concurrency: voidb_core::AgentSessionConcurrency::Serialized,
                destructive_acknowledged: false,
                input,
            },
            lease_expires_at: Utc::now() + chrono::Duration::minutes(1),
        }
    }

    fn unavailable_factory() -> MongoAgentSessionFactory {
        MongoAgentSessionFactory::new(MongoConfig {
            uri: "mongodb://fixture-user:fixture-secret@127.0.0.1:1/fixture".into(),
            default_db: Some("fixture".into()),
            auth: None,
            timeout: 1,
            tls: Default::default(),
        })
    }

    #[test]
    fn live_contracts_are_source_paced_and_change_tokens_are_scoped() {
        let (_, cursor) = mongodb_live_session_contract(CURSOR_READ_CAPABILITY).unwrap();
        cursor.validate(&[CURSOR_READ_CAPABILITY.into()]).unwrap();
        assert_eq!(
            cursor.delivery.backpressure,
            AgentLiveSessionBackpressureMode::SourcePaced
        );

        let (_, changes) = mongodb_live_session_contract(CHANGE_STREAM_READ_CAPABILITY).unwrap();
        changes
            .validate(&[CHANGE_STREAM_READ_CAPABILITY.into()])
            .unwrap();
        assert_eq!(
            changes.delivery.cursor_scope,
            AgentLiveSessionCursorScopePolicy::Required
        );
        assert_eq!(
            changes.reconnect.resume,
            AgentLiveSessionResumeMode::ExactCursor
        );
    }

    #[test]
    fn transaction_commands_cannot_mix_with_live_cursor_families() {
        let context = AgentSessionOpenContext {
            binding: voidb_core::AgentSessionBinding {
                grant_id: "grant".into(),
                profile_id: "profile".into(),
                plugin_id: "mongodb".into(),
                purpose: PluginSessionPurpose::DatabaseTransaction,
                allowed_capabilities: vec![
                    "mongodb.run_command".into(),
                    CURSOR_READ_CAPABILITY.into(),
                ],
                host_generation: 1,
            },
            request: voidb_core::AgentSessionOpenRequest {
                purpose: PluginSessionPurpose::DatabaseTransaction,
                capabilities: Vec::new(),
                lease_seconds: 60,
                concurrency: voidb_core::AgentSessionConcurrency::Serialized,
                destructive_acknowledged: true,
                input: Value::Null,
            },
            lease_expires_at: Utc::now() + chrono::Duration::minutes(1),
        };
        assert!(MongoSessionFamily::from_binding(&context).is_err());
    }

    #[tokio::test]
    async fn change_stream_scope_mismatch_is_rejected_before_connecting() {
        let error = unavailable_factory()
            .open(live_context(
                CHANGE_STREAM_READ_CAPABILITY,
                PluginSessionPurpose::WatchStream,
                json!({
                    "resource": { "database": "fixture", "collection": "events" },
                    "parameters": { "pipeline": [], "max_await_ms": 250 },
                    "resume_from": {
                        "kind": "opaque",
                        "value": "opaque-token",
                        "scope": "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                    }
                }),
            ))
            .await
            .err()
            .expect("scope mismatch must fail before target I/O");
        assert_eq!(error.code, PluginSessionErrorCode::BindingMismatch);
    }

    #[tokio::test]
    async fn persistent_cursor_rejects_cross_generation_resume_before_connecting() {
        let error = unavailable_factory()
            .open(live_context(
                CURSOR_READ_CAPABILITY,
                PluginSessionPurpose::DatabaseQuery,
                json!({
                    "resource": { "database": "fixture", "collection": "events" },
                    "parameters": { "mode": "find" },
                    "resume_from": { "kind": "opaque", "value": "server-handle" }
                }),
            ))
            .await
            .err()
            .expect("server cursor handles must not resume across generations");
        assert_eq!(error.code, PluginSessionErrorCode::PolicyDenied);
    }

    #[test]
    fn live_event_redaction_withholds_configured_secrets() {
        let targets = collect_redaction_targets(&json!({ "password": "fixture-secret" }));
        let (redacted, status) = redact_json(
            json!({ "document": { "value": "fixture-secret" } }),
            &targets,
        );
        assert_eq!(status, RedactionStatus::Applied);
        assert!(!redacted.to_string().contains("fixture-secret"));
    }
}
