//! Standalone MongoDB document-triage data-inspector TUI (Requirement 83
//! pilot).
//!
//! The pilot follows the retained family TUI pattern (see the Redis,
//! MySQL, and PostgreSQL plugins): overlay-first routing, a non-modal error
//! banner, per-view selection memory, a persistent hint footer, full-screen
//! scrollable viewers, confirm-quit, loading markers, Length(4) header/status
//! panels, fixture launch with `build_mongodb_tui_evidence`, a secret-leak
//! scan, and secret-free preflight JSON. All reads and the single supported
//! mutation go through the existing `MongoService` channel contract; the TUI
//! adds no backend operations.
//!
//! Scope is deliberately bounded to document triage: database/collection
//! browsing, paged `Find` documents, a typed document viewer, and one
//! mutation — a single-document delete anchored on the viewed document's
//! `_id`. Indexes, aggregation, insert/update/bulk write, and raw commands
//! are deferred as `later candidate`; the CLI remains the supported general
//! path.

use std::collections::VecDeque;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use bson::{Bson, Document, doc};
use crossterm::event::{self, Event, KeyCode, KeyEvent};
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use voidb_core::{TabInfo, TabManager, TuiLaunchPlan, retained_tui_quality_gate};

use crate::config::MongoConfig;
use crate::service::commands::{DatabaseCommand, DocumentCommand};
use crate::service::events::{DatabaseEvent, DocumentEvent};
use crate::service::{MongoCommand, MongoEvent, MongoService};

const DATABASES_HINTS: &str = "1 databases 2 collections j/k / filter i open r refresh ? q quit";
const COLLECTIONS_HINTS: &str = "1 databases 2 collections j/k / filter i docs r refresh ? q quit";
const DOCS_HINTS: &str =
    "j/k PgUp/PgDn n page b back / page filter f query i doc d del r refetch Esc close";
const DOC_HINTS: &str = "document detail: j/k PgUp/PgDn d del doc Esc close";
/// Bounded page size sent as `limit` to `DocumentCommand::Find`; pages are
/// replaced, never appended, so the viewport stays bounded.
const PAGE_SIZE: u64 = 10;
const VIEWER_LINE_LIMIT: usize = 2_000;
const VIEWER_BYTE_LIMIT: usize = 2 * 1024 * 1024;
const INPUT_LIMIT: usize = 128;
const PAGE_STEP: isize = 10;
const CELL_MAX_WIDTH: usize = 28;
const DETAIL_CAP: usize = 200;

/// Where the TUI launch came from. Mirrors the family TUI source enums.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MongoTuiSource {
    Profile,
    Connection,
    Fixture,
}

/// Launch description for the standalone MongoDB document TUI. Mirrors the
/// family launch shape.
#[derive(Debug, Clone)]
pub struct MongoTuiLaunch {
    pub profile_label: String,
    pub config: Option<MongoConfig>,
    pub source: MongoTuiSource,
    pub fixture_path: Option<String>,
    pub purpose: String,
    pub readonly: bool,
    pub restore: bool,
    pub launch_plan: Option<TuiLaunchPlan>,
}

// ---------------------------------------------------------------------------
// Fixture model
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct MongoTuiFixture {
    profile_label: Option<String>,
    server_label: String,
    uri_scheme: String,
    default_db: Option<String>,
    status: Option<String>,
    target_error: Option<String>,
    #[serde(default)]
    error_collections: Vec<String>,
    error_message: Option<String>,
    #[serde(default)]
    databases: Vec<FixtureDatabase>,
    #[serde(default)]
    collections: std::collections::BTreeMap<String, Vec<FixtureCollection>>,
    #[serde(default)]
    finds: Vec<FixtureFindRecord>,
}

#[derive(Debug, Deserialize)]
struct FixtureDatabase {
    name: String,
    size_on_disk: u64,
    #[serde(default)]
    empty: bool,
}

#[derive(Debug, Deserialize)]
struct FixtureCollection {
    name: String,
    #[serde(default)]
    doc_count: u64,
}

#[derive(Debug, Deserialize)]
struct FixtureFindRecord {
    database: String,
    collection: String,
    #[serde(default)]
    documents: Vec<Value>,
}

/// In-memory fixture state. Fixture actions mutate this state directly so the
/// "document gone from the refetched page" success state is observable
/// without a server.
struct MongoTuiData {
    profile_label: String,
    server_label: String,
    uri_scheme: String,
    default_db: Option<String>,
    status: String,
    target_error: Option<String>,
    error_collections: Vec<String>,
    error_message: String,
    databases: Vec<crate::service::DbInfo>,
    collections: std::collections::BTreeMap<String, Vec<crate::service::CollectionInfo>>,
    finds: Vec<FixtureFind>,
}

struct FixtureFind {
    database: String,
    collection: String,
    documents: Vec<Document>,
}

impl MongoTuiData {
    fn is_error_collection(&self, db: &str, collection: &str) -> bool {
        let namespace = format!("{db}.{collection}");
        self.error_collections
            .iter()
            .any(|entry| entry == &namespace)
    }

    /// Resolve one bounded Find page against the fixture collection.
    /// Top-level scalar equality predicates in the query filter (including
    /// dotted paths) are honored so the server-side filter evidence stays
    /// deterministic without a query engine.
    fn resolve_find(
        &self,
        db: &str,
        collection: &str,
        filter: &Document,
        skip: u64,
        limit: u64,
    ) -> (Vec<Document>, u64) {
        let Some(find) = self
            .finds
            .iter()
            .find(|find| find.database == db && find.collection == collection)
        else {
            return (Vec::new(), 0);
        };
        let filtered: Vec<&Document> = find
            .documents
            .iter()
            .filter(|doc| matches_query(doc, filter))
            .collect();
        let total = filtered.len() as u64;
        let start = (skip as usize).min(filtered.len());
        let end = (start + limit as usize).min(filtered.len());
        (
            filtered[start..end]
                .iter()
                .map(|doc| (*doc).clone())
                .collect(),
            total,
        )
    }

    /// Apply an `_id`-anchored delete to the fixture collection. Returns the
    /// number of documents removed (0 or 1; `_id` is unique per collection).
    fn apply_delete(&mut self, db: &str, collection: &str, doc_id: &Bson) -> u64 {
        let Some(find) = self
            .finds
            .iter_mut()
            .find(|find| find.database == db && find.collection == collection)
        else {
            return 0;
        };
        let before = find.documents.len();
        find.documents
            .retain(|doc| doc.get("_id").map(|value| value != doc_id).unwrap_or(true));
        let deleted = (before - find.documents.len()) as u64;
        if deleted > 0
            && let Some(colls) = self.collections.get_mut(db)
            && let Some(info) = colls.iter_mut().find(|info| info.name == collection)
        {
            info.doc_count = info.doc_count.saturating_sub(deleted);
        }
        deleted
    }
}

/// Honor top-level equality predicates of a MongoDB query filter. Supports
/// dotted paths (`customer.tier`) so the fixture exercises realistic filters
/// without embedding a query engine in the TUI.
fn matches_query(document: &Document, filter: &Document) -> bool {
    filter
        .iter()
        .all(|(key, expected)| resolve_path(document, key) == Some(expected))
}

fn resolve_path<'a>(document: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut segments = path.split('.');
    let first = segments.next()?;
    let mut current = document.get(first)?;
    for segment in segments {
        current = match current {
            Bson::Document(doc) => doc.get(segment)?,
            Bson::Array(items) => {
                let index: usize = segment.parse().ok()?;
                items.get(index)?
            }
            _ => return None,
        };
    }
    Some(current)
}

/// Convert one fixture JSON value (relaxed extended JSON) into a BSON
/// document, exercising the same type conversion the live service produces
/// (`ObjectId`, `DateTime`, nested documents, arrays, binary).
fn bson_document_from_json(value: &Value) -> Result<Document, String> {
    let bson_value =
        Bson::try_from(value.clone()).map_err(|error| format!("invalid extended JSON: {error}"))?;
    match bson_value {
        Bson::Document(document) => Ok(document),
        _ => Err("fixture find record must be a JSON object".to_string()),
    }
}

fn load_fixture(path: &str) -> Result<MongoTuiData> {
    let text = fs::read_to_string(path).with_context(|| format!("read fixture {path}"))?;
    let fixture: MongoTuiFixture =
        serde_json::from_str(&text).with_context(|| format!("parse fixture {path}"))?;
    let mut finds = Vec::new();
    for record in fixture.finds {
        let mut documents = Vec::new();
        for value in &record.documents {
            documents.push(bson_document_from_json(value).map_err(anyhow::Error::msg)?);
        }
        finds.push(FixtureFind {
            database: record.database,
            collection: record.collection,
            documents,
        });
    }
    Ok(MongoTuiData {
        profile_label: fixture
            .profile_label
            .unwrap_or_else(|| "fixture-mongodb".to_string()),
        server_label: fixture.server_label,
        uri_scheme: fixture.uri_scheme,
        default_db: fixture.default_db,
        status: fixture
            .status
            .unwrap_or_else(|| "fixture mongodb operations ready".to_string()),
        target_error: fixture.target_error,
        error_collections: fixture.error_collections,
        error_message: fixture
            .error_message
            .unwrap_or_else(|| "target error: fixture action rejected".to_string()),
        databases: fixture
            .databases
            .into_iter()
            .map(|db| crate::service::DbInfo {
                name: db.name,
                size_on_disk: db.size_on_disk,
                empty: db.empty,
            })
            .collect(),
        collections: fixture
            .collections
            .into_iter()
            .map(|(db, colls)| {
                (
                    db,
                    colls
                        .into_iter()
                        .map(|coll| crate::service::CollectionInfo {
                            name: coll.name,
                            doc_count: coll.doc_count,
                            size: 0,
                            index_count: 0,
                        })
                        .collect(),
                )
            })
            .collect(),
        finds,
    })
}

// ---------------------------------------------------------------------------
// Typed BSON rendering
// ---------------------------------------------------------------------------

/// Truncate a display label with an ellipsis.
fn truncate_label(text: String, cap: usize) -> String {
    if text.chars().count() > cap {
        let shortened: String = text.chars().take(cap.saturating_sub(1)).collect();
        format!("{shortened}\u{2026}")
    } else {
        text
    }
}

/// Compact typed label for a scalar BSON value (used in list summaries and
/// the `_id` echo). Nested documents/arrays render as shape placeholders; the
/// full detail viewer expands them.
fn bson_value_label(value: &Bson) -> String {
    match value {
        Bson::ObjectId(oid) => format!("ObjectId(\"{oid}\")"),
        Bson::DateTime(dt) => dt
            .try_to_rfc3339_string()
            .unwrap_or_else(|_| truncate_label(format!("{dt}"), DETAIL_CAP)),
        Bson::Null => "(null)".to_string(),
        Bson::Undefined => "(undefined)".to_string(),
        Bson::Binary(binary) => format!("<binary {} bytes>", binary.bytes.len()),
        Bson::Document(_) => "{...}".to_string(),
        Bson::Array(items) => format!("[{}]", items.len()),
        Bson::RegularExpression(regex) => format!("/{}/{}", regex.pattern, regex.options),
        Bson::Timestamp(ts) => format!("Timestamp({}, {})", ts.time, ts.increment),
        Bson::String(text) => truncate_label(text.clone(), DETAIL_CAP),
        other => truncate_label(format!("{other}"), DETAIL_CAP),
    }
}

/// One-line summary of a document for the paged list: the `_id` plus the
/// first scalar fields.
fn document_summary(document: &Document) -> String {
    let mut parts = vec![format!(
        "_id={}",
        document
            .get("_id")
            .map(bson_value_label)
            .unwrap_or_else(|| "(missing)".to_string())
    )];
    for (key, value) in document.iter().filter(|(key, _)| *key != "_id").take(3) {
        parts.push(format!(
            "{key}={}",
            truncate_label(bson_value_label(value), CELL_MAX_WIDTH)
        ));
    }
    let extra = document.len().saturating_sub(4);
    if extra > 0 {
        parts.push(format!("+{extra} more"));
    }
    parts.join(" ")
}

/// Recursive typed rendering for the full-screen document viewer: one line
/// per field, nested subdocuments indented, arrays itemized.
fn render_bson_value(key: &str, value: &Bson, depth: usize, lines: &mut Vec<String>) {
    let indent = "  ".repeat(depth);
    match value {
        Bson::Document(nested) => {
            if nested.is_empty() {
                lines.push(format!("{indent}{key}: {{}}"));
            } else {
                lines.push(format!("{indent}{key}: {{"));
                for (nested_key, nested_value) in nested {
                    render_bson_value(nested_key, nested_value, depth + 1, lines);
                }
                lines.push(format!("{indent}}}"));
            }
        }
        Bson::Array(items) => {
            if items.is_empty() {
                lines.push(format!("{indent}{key}: []"));
            } else {
                lines.push(format!("{indent}{key}: [{}]", items.len()));
                for (index, item) in items.iter().enumerate() {
                    render_bson_value(&format!("[{index}]"), item, depth + 1, lines);
                }
            }
        }
        other => lines.push(format!(
            "{indent}{key}: {}",
            truncate_label(bson_value_label(other), DETAIL_CAP)
        )),
    }
}

fn render_document(document: &Document) -> Vec<String> {
    let mut lines = Vec::new();
    for (key, value) in document {
        render_bson_value(key, value, 0, &mut lines);
    }
    lines
}

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum View {
    Databases,
    Collections,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Browser,
    Filter,
}

/// Full-screen or popup surfaces stacked on top of the browser. Input is
/// routed to the active overlay first, so each surface owns its key space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Overlay {
    None,
    Help,
    DocsViewer,
    DocViewer,
    QueryFilterPrompt,
    ConfirmDelete,
    ConfirmQuit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    DeleteDocument,
}

impl OperationKind {
    fn label(&self) -> &'static str {
        match self {
            OperationKind::DeleteDocument => "delete document",
        }
    }

    fn risk(&self) -> &'static str {
        match self {
            OperationKind::DeleteDocument => "destructive",
        }
    }
}

/// Staged mutation plan rendered in the status/summary surfaces before
/// execution. The delete filter is always the exact `_id` of the viewed
/// document; the plan carries the id so the echo is visible everywhere.
#[derive(Debug, Clone, PartialEq)]
struct OperationPlanView {
    kind: OperationKind,
    db: String,
    collection: String,
    doc_id: Bson,
    id_label: String,
    risk: &'static str,
    confirmations_required: u8,
    confirmations: u8,
}

/// Namespace/page stamp for the outstanding Find request. The service event
/// carries no context, so the TUI correlates responses with the request
/// namespace and discards stale pages when the operator moved on.
#[derive(Debug, Clone, PartialEq)]
struct FindRequest {
    db: String,
    collection: String,
    page: usize,
}

/// Bounded text buffer for viewer bodies (same contract as the family TUIs).
struct BoundedLines {
    lines: VecDeque<String>,
    bytes: usize,
    dropped_lines: usize,
    max_lines: usize,
    max_bytes: usize,
}

pub fn write_mongodb_tui_preflight(launch: &MongoTuiLaunch) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&preflight_value(launch))?
    );
    Ok(())
}

pub fn build_mongodb_tui_evidence(launch: &MongoTuiLaunch) -> Result<Value> {
    let data = if let Some(path) = &launch.fixture_path {
        load_fixture(path)?
    } else {
        let config = launch
            .config
            .as_ref()
            .context("mongodb tui evidence requires a profile, connection, or fixture")?;
        config_data(launch.profile_label.clone(), config)
    };

    let mut evidence = json!({
        "schema_version": 1,
        "kind": "mongodb_tui_fixture_evidence",
        "quality_gate": retained_tui_quality_gate(
            "mongodb",
            &["fixture-mongodb-ops", "fixture-mongodb-local"],
            &["target error"],
            80,
            24,
            10_000
        ),
        "preflight": preflight_value(launch),
        "transcript": {
            "profile_label": data.profile_label,
            "server_label": data.server_label,
            "uri_scheme": data.uri_scheme,
            "default_db": data.default_db,
            "status": data.status,
            "target_error": data.target_error,
            "databases": data.databases.iter().map(database_value).collect::<Vec<_>>(),
            "collections": data.collections.iter().map(|(db, colls)| json!({
                "database": db,
                "collections": colls.iter().map(collection_value).collect::<Vec<_>>()
            })).collect::<Vec<_>>(),
            "finds": data.finds.iter().map(find_value).collect::<Vec<_>>(),
            "viewer_bounds": {
                "max_lines": VIEWER_LINE_LIMIT,
                "max_bytes": VIEWER_BYTE_LIMIT
            }
        },
        "plan_transcript": [
            {
                "action": "delete_document",
                "risk": "destructive",
                "typed_confirmation": "exact_collection_name_required",
                "second_confirmation": "explicit_y",
                "delete_filter": "exact {\"_id\": <viewed document _id>} equality filter; never operator-authored",
                "bounded_to": "at most one document because _id is the unique primary key of every MongoDB collection",
                "id_echo": "confirmation popup, staged plan, and result status all echo the _id",
                "readonly_behavior": "plan_visible_but_blocked",
                "post_action": "current_page_refetch"
            }
        ],
        "coverage": [
            "startup",
            "databases_collections_tabs",
            "database_selection",
            "collection_paging_skip_history",
            "partial_final_page",
            "end_of_collection_state",
            "empty_collection_state",
            "query_filter_prompt",
            "client_filter_match_count",
            "document_viewer_typed_rendering",
            "typed_delete_confirmation",
            "delete_requires_document_id",
            "mutation_refetch",
            "readonly_block",
            "non_modal_error_banner",
            "target_error_responsive",
            "confirm_quit",
            "resize",
            "quit_restore",
            "secret_leak_scan"
        ],
        "deferred": {
            "indexes_view": "later candidate; IndexCommand::List exists in the channel contract but the slice ships the document-triage surface only",
            "aggregation_console": "later candidate; pipeline authoring UX needs its own evidence",
            "insert_update_bulk_write": "later candidate; destructive workflows beyond the slice gate",
            "index_create_drop": "later candidate",
            "raw_run_command": "later candidate; destructive-gated surface",
            "server_diagnostics_view": "later candidate; no bounded server-info command in the channel contract",
            "supported_mutations": ["delete_document_by_id"],
            "cli_remains_supported_general_path": true
        },
        "mutation_rescan_policy": {
            "find_semantics": "one bounded Find page at a time (limit 10, skip page*10, sort {_id:1}); pages are replaced, never appended",
            "delete_filter": "always an exact {\"_id\": <viewed document _id>} equality filter built from the viewed document; MongoDB bounds _id equality to at most one document",
            "after_mutation": "the current page is refetched so the deleted document disappears and the total updates",
            "fixture_mutations": "applied to the in-memory fixture documents so paging evidence stays deterministic"
        },
        "secret_leak_scan": null
    });

    let rendered = serde_json::to_string(&evidence)?;
    let markers = secret_leak_markers(&rendered);
    evidence["secret_leak_scan"] = json!({
        "passed": markers.is_empty(),
        "marker_count": markers.len(),
        "markers": markers
    });
    Ok(evidence)
}

pub async fn run_mongodb_tui(launch: MongoTuiLaunch) -> Result<()> {
    let mut app = MongoTuiApp::new(launch)?;
    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, &mut app);
    app.shutdown();
    ratatui::restore();
    result
}

fn preflight_value(launch: &MongoTuiLaunch) -> Value {
    let fixture_data = launch
        .fixture_path
        .as_ref()
        .and_then(|path| load_fixture(path).ok());
    let connection = launch
        .config
        .as_ref()
        .map(connection_value)
        .or_else(|| {
            fixture_data.as_ref().map(|data| {
                json!({
                    "kind": data.uri_scheme,
                    "server_label": data.server_label,
                    "default_db": data.default_db,
                    "secret_material": "redacted"
                })
            })
        })
        .unwrap_or_else(|| {
            json!({
                "kind": "fixture",
                "server_label": "fixture",
                "secret_material": "redacted"
            })
        });
    let default_db = launch
        .config
        .as_ref()
        .and_then(|config| config.default_db.clone())
        .or_else(|| {
            fixture_data
                .as_ref()
                .and_then(|data| data.default_db.clone())
        });
    let launch_plan = launch.launch_plan.as_ref().map(|plan| {
        json!({
            "schema_version": plan.schema_version,
            "plugin_id": plan.plugin_id,
            "command": plan.command,
            "args": plan.args,
            "profile": plan.profile,
            "purpose": plan.purpose,
            "readonly": plan.readonly,
            "restore": plan.restore,
            "raw_input": plan.raw_input,
            "credential_ref_count": plan.credential_grant.credential_refs.len(),
            "credential_grant_id": plan.credential_grant.id,
            "redaction": plan.redaction
        })
    });

    json!({
        "ok": true,
        "command": "mongodb tui",
        "plugin_id": "mongodb",
        "profile_label": fixture_data
            .as_ref()
            .map(|data| data.profile_label.clone())
            .unwrap_or_else(|| launch.profile_label.clone()),
        "source": launch.source,
        "purpose": launch.purpose,
        "readonly": launch.readonly,
        "restore": launch.restore,
        "raw_input": false,
        "fixture": launch.fixture_path.is_some(),
        "default_db": default_db,
        "connection": connection,
        "privacy": {
            "diagnostics_include_uri": false,
            "diagnostics_include_credentials": false,
            "diagnostics_include_document_values": false
        },
        "stream_bounds": {
            "page_size": PAGE_SIZE,
            "document_viewer": { "max_lines": VIEWER_LINE_LIMIT, "max_bytes": VIEWER_BYTE_LIMIT }
        },
        "service_boundary": "MongoService::Channel",
        "modes": [
            "databases",
            "collections",
            "filter",
            "docs_viewer",
            "doc_viewer",
            "query_filter_prompt",
            "typed_delete_confirmation",
            "confirm_quit",
            "target_error"
        ],
        "launch_plan": launch_plan
    })
}

fn run_loop(terminal: &mut ratatui::DefaultTerminal, app: &mut MongoTuiApp) -> Result<()> {
    // Startup auto-load (standalone TUI acceptance): the primary view must
    // show live data without requiring a manual refresh key.
    if app.service.is_some() {
        app.refresh_view();
    }
    terminal.draw(|frame| app.draw(frame))?;
    loop {
        let mut dirty = app.drain_service();
        if app.should_quit {
            return Ok(());
        }

        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key) => {
                    app.handle_key(key);
                    dirty = true;
                }
                Event::Resize(cols, rows) => {
                    app.status = format!("resized mongodb view to {cols}x{rows}");
                    dirty = true;
                }
                _ => {}
            }
        }
        if dirty {
            terminal.draw(|frame| app.draw(frame))?;
        }
    }
}

/// Live-mode launch state derived from the config. The URI itself is never
/// rendered; only the scheme and the default database are shown.
fn config_data(profile_label: String, config: &MongoConfig) -> MongoTuiData {
    let scheme = uri_scheme(&config.uri).unwrap_or_else(|| "mongodb".to_string());
    MongoTuiData {
        profile_label,
        server_label: scheme.clone(),
        uri_scheme: scheme,
        default_db: config.default_db.clone(),
        status: "connecting through MongoService channel mode".to_string(),
        target_error: None,
        error_collections: Vec::new(),
        error_message: "target error: fixture action rejected".to_string(),
        databases: Vec::new(),
        collections: std::collections::BTreeMap::new(),
        finds: Vec::new(),
    }
}

/// Extract only the URI scheme (`mongodb` / `mongodb+srv`). Hosts and
/// credentials are never surfaced anywhere in the TUI.
fn uri_scheme(uri: &str) -> Option<String> {
    uri.split("://").next().map(str::to_string)
}

fn connection_value(config: &MongoConfig) -> Value {
    let auth = match &config.auth {
        Some(crate::config::MongoAuth::Password { .. }) => "password",
        Some(crate::config::MongoAuth::X509 { .. }) => "x509",
        Some(crate::config::MongoAuth::AwsIam { .. }) => "aws_iam",
        None => "none",
    };
    json!({
        "kind": uri_scheme(&config.uri).unwrap_or_else(|| "mongodb".to_string()),
        "default_db": config.default_db,
        "auth": auth,
        "timeout_secs": config.timeout,
        "tls_enabled": config.tls.enabled,
        "secret_material": "redacted"
    })
}

fn database_value(db: &crate::service::DbInfo) -> Value {
    json!({
        "name": db.name,
        "size_on_disk": db.size_on_disk,
        "empty": db.empty
    })
}

fn collection_value(info: &crate::service::CollectionInfo) -> Value {
    json!({
        "name": info.name,
        "doc_count": info.doc_count
    })
}

fn find_value(find: &FixtureFind) -> Value {
    json!({
        "database": find.database,
        "collection": find.collection,
        "total": find.documents.len(),
        "documents": find
            .documents
            .iter()
            .map(|doc| Bson::Document(doc.clone()).into_relaxed_extjson())
            .collect::<Vec<_>>()
    })
}

struct MongoTuiApp {
    profile_label: String,
    server_label: String,
    uri_scheme: String,
    default_db: Option<String>,
    source: MongoTuiSource,
    purpose: String,
    readonly: bool,
    restore: bool,
    view: View,
    mode: Mode,
    overlay: Overlay,
    error_banner: Option<String>,
    /// Browser filter shared by the databases and collections lists (`/`).
    browser_filter: String,
    /// Document a staged delete applies to (captured when `d` is pressed so
    /// the confirmation popup and plan keep echoing the same `_id`).
    delete_target: Option<Document>,
    /// Overlay the delete confirm was opened from, so staging returns the
    /// operator to the same viewer (family behavior).
    delete_return: Overlay,
    // Databases view state.
    databases: Vec<crate::service::DbInfo>,
    dbs_selection: usize,
    dbs_pending: bool,
    // Collections view state.
    current_db: Option<String>,
    collections: Vec<crate::service::CollectionInfo>,
    colls_selection: usize,
    colls_pending: bool,
    // Document viewer state.
    current_collection: Option<String>,
    docs: Vec<Document>,
    docs_total: u64,
    docs_page: usize,
    docs_selection: usize,
    docs_pending: bool,
    docs_request: Option<FindRequest>,
    /// Previous page indices so `b` can walk back without losing context.
    docs_page_history: Vec<usize>,
    docs_filter: String,
    docs_filtering: bool,
    /// Server-side JSON query filter (the `f` prompt); applied through the
    /// service's existing Find filter parameter.
    query_filter: String,
    // Document detail viewer state.
    doc_detail: Option<Document>,
    doc_lines: BoundedLines,
    doc_scroll: usize,
    // Mutation state.
    typed_collection_input: String,
    query_input: String,
    operation_plan: Option<OperationPlanView>,
    fixture: Option<MongoTuiData>,
    service: Option<MongoService>,
    status: String,
    should_quit: bool,
    render_quit: Arc<AtomicBool>,
}

impl MongoTuiApp {
    fn new(launch: MongoTuiLaunch) -> Result<Self> {
        let data = if let Some(path) = &launch.fixture_path {
            load_fixture(path)?
        } else {
            let config = launch
                .config
                .as_ref()
                .context("mongodb tui requires a profile, connection, or fixture")?;
            config_data(launch.profile_label.clone(), config)
        };

        let render_quit = Arc::new(AtomicBool::new(false));
        let service = if launch.fixture_path.is_none() {
            let config = launch
                .config
                .clone()
                .context("mongodb tui requires config outside fixture mode")?;
            let tabs = Arc::new(StandaloneMongoTabManager::new(render_quit.clone()));
            let runtime = tokio::runtime::Handle::current();
            let service = MongoService::new(tabs, runtime);
            service.send(MongoCommand::Connect { config });
            Some(service)
        } else {
            None
        };

        let is_fixture = launch.fixture_path.is_some();
        let mut app = Self {
            profile_label: data.profile_label.clone(),
            server_label: data.server_label.clone(),
            uri_scheme: data.uri_scheme.clone(),
            default_db: data.default_db.clone(),
            source: launch.source,
            purpose: launch.purpose,
            readonly: launch.readonly,
            restore: launch.restore,
            view: View::Databases,
            mode: Mode::Browser,
            overlay: Overlay::None,
            error_banner: data.target_error.clone(),
            browser_filter: String::new(),
            delete_target: None,
            delete_return: Overlay::DocsViewer,
            databases: if is_fixture {
                data.databases.clone()
            } else {
                Vec::new()
            },
            dbs_selection: 0,
            dbs_pending: false,
            current_db: None,
            collections: Vec::new(),
            colls_selection: 0,
            colls_pending: false,
            current_collection: None,
            docs: Vec::new(),
            docs_total: 0,
            docs_page: 0,
            docs_selection: 0,
            docs_pending: false,
            docs_request: None,
            docs_page_history: Vec::new(),
            docs_filter: String::new(),
            docs_filtering: false,
            query_filter: String::new(),
            doc_detail: None,
            doc_lines: BoundedLines::new(VIEWER_LINE_LIMIT, VIEWER_BYTE_LIMIT),
            doc_scroll: 0,
            typed_collection_input: String::new(),
            query_input: String::new(),
            operation_plan: None,
            fixture: if is_fixture { Some(data) } else { None },
            service,
            status: String::new(),
            should_quit: false,
            render_quit,
        };
        if is_fixture {
            // Fixture mode preselects the default database and loads its
            // collections eagerly so the first frame already shows data.
            if let Some(default_db) = app.default_db.clone() {
                app.open_collections(&default_db);
            }
            app.status = app
                .fixture
                .as_ref()
                .map(|fixture| fixture.status.clone())
                .unwrap_or_default();
        }
        if let Some(error) = app
            .fixture
            .as_ref()
            .and_then(|fixture| fixture.target_error.clone())
        {
            app.error_banner = Some(error.clone());
            app.status = format!("{} | {}", app.status, safe_error_summary(&error));
        }
        Ok(app)
    }

    fn shutdown(&mut self) {
        if let Some(service) = &self.service {
            service.send(MongoCommand::Disconnect);
        }
    }

    fn drain_service(&mut self) -> bool {
        let mut changed = false;
        if self.render_quit.load(Ordering::SeqCst) {
            changed |= !self.should_quit;
            self.should_quit = true;
        }
        let Some(mut service) = self.service.take() else {
            return changed;
        };
        while let Some(event) = service.poll_event() {
            changed = true;
            self.handle_service_event(event);
        }
        self.service = Some(service);
        changed
    }

    fn handle_service_event(&mut self, event: MongoEvent) {
        match event {
            MongoEvent::Connected => {
                self.status = "connected; loading databases".to_string();
                self.request_databases();
            }
            MongoEvent::Database(DatabaseEvent::DatabasesListed(databases)) => {
                self.dbs_pending = false;
                self.databases = databases;
                self.trim_dbs_selection();
                if let Some(default_db) = self.default_db.clone()
                    && self.databases.iter().any(|db| db.name == default_db)
                {
                    self.open_collections(&default_db);
                } else {
                    self.status = format!("loaded {} databases", self.databases.len());
                }
            }
            MongoEvent::Database(DatabaseEvent::CollectionsListed { db, collections }) => {
                self.colls_pending = false;
                if Some(db.as_str()) == self.current_db.as_deref() {
                    self.collections = collections;
                    self.trim_colls_selection();
                    self.status = format!("loaded {} collections in {db}", self.collections.len());
                }
                // Stale responses for a database the operator already left
                // are dropped silently.
            }
            MongoEvent::Document(DocumentEvent::Found(result)) => {
                self.docs_pending = false;
                let Some(request) = self.docs_request.take() else {
                    return;
                };
                if request.db == self.current_db.as_deref().unwrap_or_default()
                    && request.collection == self.current_collection.as_deref().unwrap_or_default()
                {
                    self.apply_find_result(result.documents, result.total);
                    self.status = format!(
                        "found {} documents on page {} (total {})",
                        self.docs.len(),
                        self.docs_page + 1,
                        self.docs_total
                    );
                } else {
                    self.status = "stale document page discarded (namespace moved on)".to_string();
                }
            }
            MongoEvent::Document(DocumentEvent::Deleted(result)) => {
                self.operation_plan = None;
                let (db, collection) = self.docs_namespace();
                self.status = format!(
                    "deleted {} document(s) from {db}.{collection}",
                    result.deleted_count
                );
                self.rescan_current_page();
            }
            MongoEvent::Document(DocumentEvent::Counted(count)) => {
                self.status = format!("count outside the pilot surface: {count}");
            }
            MongoEvent::Document(_) => {
                self.status = "write event outside the pilot mutation surface".to_string();
            }
            MongoEvent::Index(_) => {}
            MongoEvent::Error(message) => {
                // Background and request failures surface as a non-modal
                // banner so a failing fetch never hijacks the browser.
                self.dbs_pending = false;
                self.colls_pending = false;
                self.docs_pending = false;
                self.error_banner = Some(message);
                self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
            }
        }
    }

    // === Input routing ===

    fn handle_key(&mut self, key: KeyEvent) {
        // The error banner is informational: any key acknowledges it while
        // the key itself keeps its normal meaning, so background failures
        // never trap the operator in a modal.
        self.error_banner = None;
        match self.overlay {
            Overlay::None => {}
            Overlay::Help => {
                self.overlay = Overlay::None;
                self.status = "help closed".to_string();
                return;
            }
            Overlay::DocsViewer => {
                self.handle_docs_viewer_key(key);
                return;
            }
            Overlay::DocViewer => {
                self.handle_doc_viewer_key(key);
                return;
            }
            Overlay::QueryFilterPrompt => {
                self.handle_query_prompt_key(key);
                return;
            }
            Overlay::ConfirmDelete => {
                self.handle_confirm_delete_key(key);
                return;
            }
            Overlay::ConfirmQuit => {
                self.handle_confirm_quit_key(key);
                return;
            }
        }
        if self.mode == Mode::Filter {
            self.handle_filter_key(key);
            return;
        }

        match key.code {
            KeyCode::Char('q') => self.request_quit(),
            KeyCode::Char('1') => self.set_view(View::Databases),
            KeyCode::Char('2') => self.set_view(View::Collections),
            KeyCode::Tab => {
                let next = match self.view {
                    View::Databases => View::Collections,
                    View::Collections => View::Databases,
                };
                self.set_view(next);
            }
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::PageUp => self.move_selection(-PAGE_STEP),
            KeyCode::PageDown => self.move_selection(PAGE_STEP),
            KeyCode::Home | KeyCode::Char('g') => self.set_selected(0),
            KeyCode::End | KeyCode::Char('G') => {
                let len = self.active_len();
                self.set_selected(len.saturating_sub(1));
            }
            KeyCode::Enter | KeyCode::Char('i') => self.inspect_selected(),
            KeyCode::Char('/') => {
                self.mode = Mode::Filter;
                self.status = "filter: type to narrow live, Enter apply, Esc clear".to_string();
            }
            KeyCode::Char('r') => self.refresh_view(),
            KeyCode::Char('d') => self.open_delete_prompt(),
            KeyCode::Char('y') => self.confirm_plan(),
            KeyCode::Char('?') => self.show_help(),
            KeyCode::Esc => self.cancel_plan(),
            _ => {
                self.status = format!(
                    "mongodb: {}",
                    match self.view {
                        View::Databases => DATABASES_HINTS,
                        View::Collections => COLLECTIONS_HINTS,
                    }
                );
            }
        }
    }

    fn handle_filter_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => {
                self.mode = Mode::Browser;
                self.set_selected(0);
                self.status = format!(
                    "filter applied: {} ({} matches)",
                    self.browser_filter_label(),
                    self.active_len()
                );
            }
            KeyCode::Esc => {
                self.browser_filter.clear();
                self.mode = Mode::Browser;
                self.set_selected(0);
                self.status = "filter cleared".to_string();
            }
            KeyCode::Backspace => {
                self.browser_filter.pop();
                self.set_selected(0);
            }
            KeyCode::Char(ch) => {
                if self.browser_filter.len() < INPUT_LIMIT {
                    self.browser_filter.push(ch);
                }
                self.set_selected(0);
            }
            _ => {}
        }
    }

    fn handle_docs_viewer_key(&mut self, key: KeyEvent) {
        // While the page filter input is open every character belongs to the
        // filter, mirroring the family filter-mode routing.
        if self.docs_filtering {
            match key.code {
                KeyCode::Esc => {
                    self.docs_filtering = false;
                    self.docs_filter.clear();
                    self.set_docs_selected(0);
                    self.status = "page filter cleared".to_string();
                }
                KeyCode::Backspace => {
                    self.docs_filter.pop();
                    self.set_docs_selected(0);
                }
                KeyCode::Enter => {
                    self.docs_filtering = false;
                    self.set_docs_selected(0);
                    self.status = format!(
                        "page filter applied: '{}' ({} match(es) on this page)",
                        self.docs_filter,
                        self.filtered_docs().len()
                    );
                }
                KeyCode::Char(ch) if self.docs_filter.len() < INPUT_LIMIT => {
                    self.docs_filter.push(ch);
                    self.set_docs_selected(0);
                }
                _ => {}
            }
            return;
        }
        let last = self.filtered_docs().len().saturating_sub(1);
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.overlay = Overlay::None;
                self.status = "document viewer closed".to_string();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.set_docs_selected(self.docs_selection.saturating_sub(1))
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.set_docs_selected((self.docs_selection + 1).min(last))
            }
            KeyCode::PageUp => self.move_docs_selection(-PAGE_STEP),
            KeyCode::PageDown => self.move_docs_selection(PAGE_STEP),
            KeyCode::Home | KeyCode::Char('g') => self.set_docs_selected(0),
            KeyCode::End | KeyCode::Char('G') => self.set_docs_selected(last),
            KeyCode::Enter | KeyCode::Char('i') => self.open_doc_detail(),
            KeyCode::Char('n') => self.next_docs_page(),
            KeyCode::Char('b') => self.previous_docs_page(),
            KeyCode::Char('/') => {
                self.docs_filtering = true;
                self.status =
                    "page filter: type to narrow this page live, Enter applies, Esc clears"
                        .to_string();
            }
            KeyCode::Char('f') => self.open_query_prompt(),
            KeyCode::Char('d') => self.open_delete_prompt(),
            KeyCode::Char('r') => {
                self.rescan_current_page();
                self.status = format!("refetching page {}", self.docs_page + 1);
            }
            KeyCode::Char('y') => self.confirm_plan(),
            _ => {
                self.status = format!("mongodb: {DOCS_HINTS}");
            }
        }
    }

    fn handle_doc_viewer_key(&mut self, key: KeyEvent) {
        let max_scroll = self.doc_lines.len().saturating_sub(1);
        match key.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                // Return to the paged list viewer (the detail popup sits on
                // top of it), mirroring the family row-detail behavior.
                self.overlay = Overlay::DocsViewer;
                self.status = "document detail closed".to_string();
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.doc_scroll = self.doc_scroll.saturating_sub(1);
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.doc_scroll = (self.doc_scroll + 1).min(max_scroll);
            }
            KeyCode::PageUp => {
                self.doc_scroll = self.doc_scroll.saturating_sub(PAGE_STEP as usize);
            }
            KeyCode::PageDown => {
                self.doc_scroll = (self.doc_scroll + PAGE_STEP as usize).min(max_scroll);
            }
            KeyCode::Home | KeyCode::Char('g') => self.doc_scroll = 0,
            KeyCode::End | KeyCode::Char('G') => self.doc_scroll = max_scroll,
            KeyCode::Char('d') => self.open_delete_prompt(),
            KeyCode::Char('y') => self.confirm_plan(),
            _ => {
                self.status = format!("mongodb: {DOC_HINTS}");
            }
        }
    }

    fn handle_query_prompt_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Esc => {
                self.query_input.clear();
                self.overlay = Overlay::DocsViewer;
                self.status = "query filter edit cancelled".to_string();
            }
            KeyCode::Backspace => {
                self.query_input.pop();
            }
            KeyCode::Enter => {
                let input = self.query_input.trim().to_string();
                if input.is_empty() {
                    self.query_input.clear();
                    self.overlay = Overlay::DocsViewer;
                    self.query_filter.clear();
                    self.docs_page_history.clear();
                    self.set_docs_selected(0);
                    self.request_find(0);
                    self.status = "query filter cleared; rescanning from page 0".to_string();
                    return;
                }
                if let Err(error) = self.parse_query_filter(&input) {
                    self.status = format!("invalid query filter JSON: {error}");
                    return;
                }
                self.query_input.clear();
                self.overlay = Overlay::DocsViewer;
                self.query_filter = input.clone();
                self.docs_page_history.clear();
                self.set_docs_selected(0);
                self.request_find(0);
                self.status = format!("query filter set to {input}; rescanning from page 0");
            }
            KeyCode::Char(ch) if self.query_input.len() < INPUT_LIMIT => {
                self.query_input.push(ch);
            }
            _ => {}
        }
    }

    /// Validate the JSON query filter by converting it through the same
    /// extended-JSON path the service types use.
    fn parse_query_filter(&self, input: &str) -> std::result::Result<Document, String> {
        let value: Value = serde_json::from_str(input).map_err(|error| error.to_string())?;
        bson_document_from_json(&value)
    }

    fn handle_confirm_delete_key(&mut self, key: KeyEvent) {
        let Some(document) = self.delete_target.clone() else {
            self.overlay = Overlay::None;
            return;
        };
        let Some(collection) = self.current_collection.clone() else {
            self.overlay = Overlay::None;
            return;
        };
        match key.code {
            KeyCode::Esc => {
                self.typed_collection_input.clear();
                self.delete_target = None;
                self.overlay = self.delete_return;
                self.status = "delete cancelled".to_string();
            }
            KeyCode::Backspace => {
                self.typed_collection_input.pop();
            }
            KeyCode::Enter => {
                if self.typed_collection_input != collection {
                    self.status =
                        format!("typed name does not match {collection}; delete not staged");
                    self.typed_collection_input.clear();
                    return;
                }
                let Some(doc_id) = document.get("_id").cloned() else {
                    self.overlay = Overlay::None;
                    return;
                };
                let db = self.current_db.clone().unwrap_or_default();
                self.typed_collection_input.clear();
                self.delete_target = None;
                // Return to the viewer the confirm was opened from. The typed
                // collection name is the first of two confirmations; `y`
                // remains the explicit non-default acknowledgement.
                self.overlay = self.delete_return;
                // The typed collection name is the first of two confirmations;
                // `y` remains the explicit non-default acknowledgement.
                self.operation_plan = Some(OperationPlanView {
                    kind: OperationKind::DeleteDocument,
                    id_label: bson_value_label(&doc_id),
                    doc_id,
                    db,
                    collection,
                    risk: OperationKind::DeleteDocument.risk(),
                    confirmations_required: 2,
                    confirmations: 1,
                });
                self.status = format!(
                    "delete plan staged for {} (typed confirmation recorded); press y to execute",
                    self.plan_target_label()
                );
            }
            KeyCode::Char(ch) if self.typed_collection_input.len() < INPUT_LIMIT => {
                self.typed_collection_input.push(ch);
            }
            _ => {}
        }
    }

    fn handle_confirm_quit_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char('y') | KeyCode::Enter => {
                self.should_quit = true;
            }
            _ => {
                self.overlay = Overlay::None;
                self.status = "quit cancelled".to_string();
            }
        }
    }

    // === Selection and views ===

    fn filtered_databases(&self) -> Vec<crate::service::DbInfo> {
        let needle = self.browser_filter.to_lowercase();
        self.databases
            .iter()
            .filter(|db| needle.is_empty() || db.name.to_lowercase().contains(&needle))
            .cloned()
            .collect()
    }

    fn filtered_collections(&self) -> Vec<crate::service::CollectionInfo> {
        let needle = self.browser_filter.to_lowercase();
        self.collections
            .iter()
            .filter(|info| needle.is_empty() || info.name.to_lowercase().contains(&needle))
            .cloned()
            .collect()
    }

    /// Client-side page filter over the loaded documents.
    fn filtered_docs(&self) -> Vec<Document> {
        let needle = self.docs_filter.to_lowercase();
        self.docs
            .iter()
            .filter(|doc| {
                needle.is_empty() || document_summary(doc).to_lowercase().contains(&needle)
            })
            .cloned()
            .collect()
    }

    /// Length of the active browser list (databases or collections); used by
    /// the shared filter/selection helpers.
    fn active_len(&self) -> usize {
        match self.view {
            View::Databases => self.filtered_databases().len(),
            View::Collections => self.filtered_collections().len(),
        }
    }

    fn selected(&self) -> usize {
        match self.view {
            View::Databases => self.dbs_selection,
            View::Collections => self.colls_selection,
        }
    }

    fn set_selected(&mut self, value: usize) {
        let len = self.active_len();
        let clamped = value.min(len.saturating_sub(1));
        match self.view {
            View::Databases => self.dbs_selection = clamped,
            View::Collections => self.colls_selection = clamped,
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.active_len();
        if len == 0 {
            self.set_selected(0);
            return;
        }
        let last = len as isize - 1;
        let next = (self.selected() as isize + delta).clamp(0, last) as usize;
        self.set_selected(next);
    }

    fn trim_dbs_selection(&mut self) {
        let len = self.filtered_databases().len();
        self.dbs_selection = self.dbs_selection.min(len.saturating_sub(1));
    }

    fn trim_colls_selection(&mut self) {
        let len = self.filtered_collections().len();
        self.colls_selection = self.colls_selection.min(len.saturating_sub(1));
    }

    fn set_docs_selected(&mut self, value: usize) {
        let len = self.filtered_docs().len();
        self.docs_selection = value.min(len.saturating_sub(1));
    }

    fn move_docs_selection(&mut self, delta: isize) {
        let len = self.filtered_docs().len();
        if len == 0 {
            self.set_docs_selected(0);
            return;
        }
        let last = len as isize - 1;
        let next = (self.docs_selection as isize + delta).clamp(0, last) as usize;
        self.set_docs_selected(next);
    }

    fn set_view(&mut self, view: View) {
        if self.view == view {
            return;
        }
        self.view = view;
        self.status = format!(
            "view: {}",
            match view {
                View::Databases => "databases",
                View::Collections => "collections",
            }
        );
    }

    fn selected_database(&self) -> Option<crate::service::DbInfo> {
        if self.view != View::Databases {
            return None;
        }
        self.filtered_databases().get(self.dbs_selection).cloned()
    }

    fn selected_collection(&self) -> Option<crate::service::CollectionInfo> {
        if self.view != View::Collections {
            return None;
        }
        self.filtered_collections()
            .get(self.colls_selection)
            .cloned()
    }

    fn selected_document(&self) -> Option<Document> {
        self.filtered_docs().get(self.docs_selection).cloned()
    }

    /// The document a staged delete applies to: the detail-viewer document
    /// when it is open, otherwise the selected document in the list viewer.
    fn delete_document(&self) -> Option<Document> {
        if self.overlay == Overlay::DocViewer {
            return self.doc_detail.clone();
        }
        self.selected_document()
    }

    fn docs_namespace(&self) -> (String, String) {
        (
            self.current_db.clone().unwrap_or_default(),
            self.current_collection.clone().unwrap_or_default(),
        )
    }

    fn plan_target_label(&self) -> String {
        match &self.operation_plan {
            Some(plan) => format!("{}.{}", plan.db, plan.collection),
            None => "no plan".to_string(),
        }
    }

    fn query_filter_label(&self) -> String {
        if self.query_filter.is_empty() {
            "none".to_string()
        } else {
            truncate_label(self.query_filter.clone(), 32)
        }
    }

    fn browser_filter_label(&self) -> String {
        if self.browser_filter.is_empty() {
            "none".to_string()
        } else {
            self.browser_filter.clone()
        }
    }

    // === Fetches ===

    fn request_databases(&mut self) {
        self.dbs_pending = true;
        if let Some(fixture) = &self.fixture {
            self.databases = fixture.databases.clone();
            self.dbs_pending = false;
            self.trim_dbs_selection();
            self.status = format!("fixture loaded {} databases", self.databases.len());
        } else if let Some(service) = &self.service {
            service.send(MongoCommand::Database(DatabaseCommand::ListDatabases));
            self.status = "loading databases".to_string();
        } else {
            self.dbs_pending = false;
        }
    }

    /// Switch to the collections browser for one database and fetch its
    /// bounded collection list.
    /// Load the bounded collection list for one database. This does NOT
    /// switch the view: the default database is preselected at startup while
    /// the TUI still opens on the databases browser (family behavior); the
    /// view switch happens when the operator presses Enter.
    fn open_collections(&mut self, db: &str) {
        self.current_db = Some(db.to_string());
        self.current_collection = None;
        self.colls_pending = true;
        self.colls_selection = 0;
        if let Some(fixture) = &self.fixture {
            self.collections = fixture.collections.get(db).cloned().unwrap_or_default();
            self.colls_pending = false;
            self.trim_colls_selection();
            self.status = format!(
                "fixture loaded {} collections in {db}",
                self.collections.len()
            );
        } else if let Some(service) = &self.service {
            service.send(MongoCommand::Database(DatabaseCommand::ListCollections {
                db: db.to_string(),
            }));
            self.status = format!("loading collections in {db}");
        } else {
            self.colls_pending = false;
        }
    }

    /// Send one bounded Find page request. Fixture mode resolves the page
    /// from the fixture documents synchronously; live mode sends
    /// `DocumentCommand::Find` with `limit = PAGE_SIZE`,
    /// `skip = page * PAGE_SIZE`, and a deterministic `_id` sort.
    fn request_find(&mut self, page: usize) {
        let (db, collection) = self.docs_namespace();
        if db.is_empty() || collection.is_empty() {
            return;
        }
        let skip = page as u64 * PAGE_SIZE;
        self.docs_page = page;
        self.docs_pending = true;
        self.docs_request = Some(FindRequest {
            db: db.clone(),
            collection: collection.clone(),
            page,
        });
        if let Some(fixture) = &self.fixture {
            if fixture.is_error_collection(&db, &collection) {
                self.docs_pending = false;
                self.docs_request = None;
                self.error_banner = Some(fixture.error_message.clone());
                self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
                return;
            }
            let filter = self
                .parse_query_filter(&self.query_filter)
                .unwrap_or_default();
            let (docs, total) = fixture.resolve_find(&db, &collection, &filter, skip, PAGE_SIZE);
            self.docs_pending = false;
            self.docs_request = None;
            self.apply_find_result(docs, total);
            self.status = format!(
                "fixture found {} documents in {db}.{collection} (page {}, total {})",
                self.docs.len(),
                page + 1,
                total
            );
        } else if let Some(service) = &self.service {
            let filter = self
                .parse_query_filter(&self.query_filter)
                .unwrap_or_default();
            service.send(MongoCommand::Document(DocumentCommand::Find {
                db,
                collection,
                filter,
                limit: Some(PAGE_SIZE as i64),
                skip: Some(skip),
                sort: Some(doc! { "_id": 1 }),
            }));
            self.status = format!(
                "loading documents page {} (query filter: {})",
                page + 1,
                self.query_filter_label()
            );
        } else {
            self.docs_pending = false;
            self.docs_request = None;
        }
    }

    fn apply_find_result(&mut self, documents: Vec<Document>, total: u64) {
        self.docs = documents;
        self.docs_total = total;
        self.set_docs_selected(self.docs_selection);
    }

    /// `n`: replace the current page with the page at `skip + PAGE_SIZE`.
    /// Pages are replaced (not appended) so the viewport stays bounded; the
    /// previous skip is pushed onto a history stack for `b`.
    fn next_docs_page(&mut self) {
        let skip = self.docs_page as u64 * PAGE_SIZE;
        if skip + self.docs.len() as u64 >= self.docs_total {
            self.status =
                "end of collection reached; press b to go back or f to change the query filter"
                    .to_string();
            return;
        }
        self.docs_page_history.push(self.docs_page);
        self.set_docs_selected(0);
        self.request_find(self.docs_page + 1);
    }

    fn previous_docs_page(&mut self) {
        let Some(previous) = self.docs_page_history.pop() else {
            self.status = "no previous document page".to_string();
            return;
        };
        self.set_docs_selected(0);
        self.request_find(previous);
    }

    /// After a mutation the current page is refetched so the deleted document
    /// disappears and the total updates without leaving the TUI.
    fn rescan_current_page(&mut self) {
        self.request_find(self.docs_page);
    }

    fn refresh_view(&mut self) {
        match self.view {
            View::Databases => {
                self.request_databases();
            }
            View::Collections => {
                if let Some(db) = self.current_db.clone() {
                    self.open_collections(&db);
                }
            }
        }
    }

    // === Inspect ===

    fn inspect_selected(&mut self) {
        match self.view {
            View::Databases => {
                let Some(db) = self.selected_database() else {
                    self.status = "nothing selected".to_string();
                    return;
                };
                self.open_collections(&db.name);
                self.set_view(View::Collections);
            }
            View::Collections => {
                let Some(info) = self.selected_collection() else {
                    self.status = "nothing selected".to_string();
                    return;
                };
                self.open_docs_viewer(&info.name);
            }
        }
    }

    /// Open the full-screen paged DOCUMENT VIEWER for the selected collection.
    fn open_docs_viewer(&mut self, collection: &str) {
        let Some(db) = self.current_db.clone() else {
            self.status = "select a database first".to_string();
            return;
        };
        if let Some(fixture) = &self.fixture
            && fixture.is_error_collection(&db, collection)
        {
            self.error_banner = Some(fixture.error_message.clone());
            self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
            return;
        }
        self.current_collection = Some(collection.to_string());
        self.docs_filter.clear();
        self.docs_filtering = false;
        self.docs_page_history.clear();
        self.docs_selection = 0;
        self.docs = Vec::new();
        self.docs_total = 0;
        self.overlay = Overlay::DocsViewer;
        self.request_find(0);
    }

    fn open_doc_detail(&mut self) {
        let Some(document) = self.selected_document() else {
            self.status = "nothing selected".to_string();
            return;
        };
        self.doc_lines.clear();
        for line in render_document(&document) {
            self.doc_lines.push_line(line);
        }
        self.doc_detail = Some(document);
        self.doc_scroll = 0;
        self.overlay = Overlay::DocViewer;
        self.status = "document detail opened".to_string();
    }

    // === Mutations ===

    fn open_query_prompt(&mut self) {
        self.query_input.clear();
        self.overlay = Overlay::QueryFilterPrompt;
        self.status =
            "query filter: JSON equality filter (e.g. {\"status\": \"failed\"}), Enter rescans from page 0"
                .to_string();
    }

    fn open_delete_prompt(&mut self) {
        if self.overlay != Overlay::DocsViewer && self.overlay != Overlay::DocViewer {
            self.status = "open a document viewer (i) to delete a document".to_string();
            return;
        }
        let Some(document) = self.delete_document() else {
            self.status = "select a document to delete".to_string();
            return;
        };
        let has_id = matches!(
            document.get("_id"),
            Some(Bson::ObjectId(_))
                | Some(Bson::String(_))
                | Some(Bson::Int32(_))
                | Some(Bson::Int64(_))
        );
        if !has_id {
            self.status = "document has no _id; delete is not anchored (refused)".to_string();
            return;
        }
        let Some(collection) = self.current_collection.clone() else {
            self.status = "no collection context for the delete".to_string();
            return;
        };
        self.typed_collection_input.clear();
        self.delete_target = Some(document.clone());
        self.delete_return = self.overlay;
        self.overlay = Overlay::ConfirmDelete;
        let id_label = document
            .get("_id")
            .map(bson_value_label)
            .unwrap_or_default();
        self.status = format!(
            "delete {id_label} in {collection}: type the exact collection name to confirm, Esc cancels"
        );
    }

    fn confirm_plan(&mut self) {
        let Some(mut plan) = self.operation_plan.take() else {
            self.status = "no operation plan to confirm".to_string();
            return;
        };
        plan.confirmations = plan.confirmations.saturating_add(1);
        if plan.confirmations < plan.confirmations_required {
            self.status = format!(
                "{} on {}.{} is {}; press y again to execute",
                plan.kind.label(),
                plan.db,
                plan.collection,
                plan.risk
            );
            self.operation_plan = Some(plan);
            return;
        }

        if self.readonly {
            self.status = format!(
                "readonly launch blocked {} on {}.{}",
                plan.kind.label(),
                plan.db,
                plan.collection
            );
            self.operation_plan = Some(plan);
            return;
        }

        if let Some(fixture) = self.fixture.as_mut() {
            if fixture.is_error_collection(&plan.db, &plan.collection) {
                self.error_banner = Some(fixture.error_message.clone());
                self.status = safe_error_summary(self.error_banner.as_deref().unwrap_or(""));
                return;
            }
            let deleted = fixture.apply_delete(&plan.db, &plan.collection, &plan.doc_id);
            // Refetch first so the mutation summary is the final status the
            // operator sees on the refreshed page.
            self.rescan_current_page();
            self.status = format!(
                "fixture executed {} on {}.{}: {} document(s) deleted (_id {})",
                plan.kind.label(),
                plan.db,
                plan.collection,
                deleted,
                plan.id_label
            );
        } else if let Some(service) = &self.service {
            let filter = doc! { "_id": plan.doc_id.clone() };
            service.send(MongoCommand::Document(DocumentCommand::Delete {
                db: plan.db.clone(),
                collection: plan.collection.clone(),
                filter,
            }));
            self.status = format!("{} sent to MongoService", plan.kind.label());
        } else {
            self.status = "no service connection; plan dropped".to_string();
        }
    }

    fn cancel_plan(&mut self) {
        if self.operation_plan.take().is_some() {
            self.status = "operation plan cancelled".to_string();
        } else {
            self.mode = Mode::Browser;
            self.status = "browser mode".to_string();
        }
    }

    fn request_quit(&mut self) {
        let work_active = self.dbs_pending
            || self.colls_pending
            || self.docs_pending
            || self.operation_plan.is_some();
        if work_active {
            self.overlay = Overlay::ConfirmQuit;
            self.status =
                "quit? a fetch or staged plan is active: y quits, any other key stays".to_string();
            return;
        }
        self.should_quit = true;
    }

    fn show_help(&mut self) {
        self.overlay = Overlay::Help;
        self.status = "help open; any key closes".to_string();
    }

    // === Drawing ===

    fn draw(&self, frame: &mut Frame) {
        let area = frame.area();
        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                // Header and status each carry two content rows (title+tabs,
                // status+hints) on top of their borders.
                Constraint::Length(4),
                Constraint::Min(12),
                Constraint::Length(4),
            ])
            .split(area);

        frame.render_widget(self.header(), vertical[0]);
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(vertical[1]);
        match self.view {
            View::Databases => {
                frame.render_widget(self.database_list(body[0]), body[0]);
                frame.render_widget(self.database_details_panel(), body[1]);
            }
            View::Collections => {
                frame.render_widget(self.collection_list(body[0]), body[0]);
                frame.render_widget(self.collection_details_panel(), body[1]);
            }
        }
        frame.render_widget(self.status_panel(), vertical[2]);

        match self.overlay {
            Overlay::None => {}
            Overlay::Help => self.draw_help(frame, area),
            Overlay::DocsViewer => self.draw_docs_viewer(frame, area),
            Overlay::DocViewer => self.draw_doc_viewer(frame, area),
            Overlay::QueryFilterPrompt => self.draw_query_prompt(frame, area),
            Overlay::ConfirmDelete => self.draw_confirm_delete(frame, area),
            Overlay::ConfirmQuit => self.draw_confirm_quit(frame, area),
        }
        if let Some(error) = &self.error_banner {
            self.draw_error_banner(frame, area, error);
        }
    }

    fn header(&self) -> Paragraph<'_> {
        let source = format!("{:?}", self.source).to_lowercase();
        let dbs_mark = if self.dbs_pending { "\u{27f3}" } else { "" };
        let colls_mark = if self.colls_pending { "\u{27f3}" } else { "" };
        let dbs_tab = format!("1:databases{dbs_mark}");
        let colls_tab = format!("2:collections{colls_mark}");
        let tabs = match self.view {
            View::Databases => format!("[{dbs_tab}] {colls_tab}"),
            View::Collections => format!("{dbs_tab} [{colls_tab}]"),
        };
        Paragraph::new(vec![
            Line::from(vec![
                Span::styled(
                    "MongoDB Document TUI (pilot)",
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!(
                    "  profile={} server={} db={} scheme={} query_filter={}",
                    self.profile_label,
                    self.server_label,
                    self.current_db.as_deref().unwrap_or("-"),
                    self.uri_scheme,
                    self.query_filter_label()
                )),
            ]),
            Line::from(format!(
                "{}  source={} purpose={} readonly={} restore={} filter={} docs_filter={}",
                tabs,
                source,
                self.purpose,
                self.readonly,
                self.restore,
                self.browser_filter_label(),
                self.docs_filter_label(),
            )),
        ])
        .block(Block::default().borders(Borders::ALL))
    }

    fn docs_filter_label(&self) -> String {
        if self.docs_filter.is_empty() {
            "none".to_string()
        } else {
            self.docs_filter.clone()
        }
    }

    fn database_list(&self, area: Rect) -> Paragraph<'_> {
        let items = self.filtered_databases();
        let visible = area.height.saturating_sub(2).max(1) as usize;
        let start = window_start(self.dbs_selection, visible, items.len());
        let lines = items
            .iter()
            .enumerate()
            .skip(start)
            .take(visible)
            .map(|(idx, db)| {
                let marker = if idx == self.dbs_selection { ">" } else { " " };
                let style = if idx == self.dbs_selection {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Green)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Line::from(Span::styled(
                    format!(
                        "{marker} {:<20} {:>10} {}",
                        db.name,
                        format_bytes(db.size_on_disk),
                        if db.empty { "empty" } else { "" }
                    ),
                    style,
                ))
            })
            .collect::<Vec<_>>();
        Paragraph::new(if !lines.is_empty() {
            lines
        } else if self.dbs_pending {
            vec![Line::from("loading\u{2026}")]
        } else {
            vec![Line::from(
                "No databases match this filter. Press Esc to clear the filter, r to refresh.",
            )]
        })
        .block(
            Block::default()
                .title(format!(
                    " databases ({}/{}) ",
                    self.dbs_selection.saturating_add(1).min(items.len().max(1)),
                    items.len()
                ))
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false })
    }

    fn database_details_panel(&self) -> Paragraph<'_> {
        let mut lines = match self.filtered_databases().get(self.dbs_selection) {
            Some(db) => vec![
                Line::from(format!("database: {}", db.name)),
                Line::from(format!(
                    "size on disk: {}  empty: {}",
                    format_bytes(db.size_on_disk),
                    db.empty
                )),
                Line::from(format!(
                    "collections loaded: {}",
                    if Some(db.name.as_str()) == self.current_db.as_deref() {
                        self.collections.len()
                    } else {
                        0
                    }
                )),
                Line::from("Enter opens the collections browser"),
            ],
            None => vec![Line::from(
                "Select a database and press Enter to browse its collections.",
            )],
        };

        if let Some(plan) = &self.operation_plan {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "operation plan",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(format!("action: {}", plan.kind.label())));
            lines.push(Line::from(format!(
                "target: {}.{}",
                plan.db, plan.collection
            )));
            lines.push(Line::from(format!("_id: {}", plan.id_label)));
            lines.push(Line::from(format!("risk: {}", plan.risk)));
            lines.push(Line::from(format!(
                "confirmations: {}/{}",
                plan.confirmations, plan.confirmations_required
            )));
        }

        Paragraph::new(lines)
            .block(Block::default().title(" Details ").borders(Borders::ALL))
            .wrap(Wrap { trim: false })
    }

    fn collection_list(&self, area: Rect) -> Paragraph<'_> {
        let items = self.filtered_collections();
        let visible = area.height.saturating_sub(2).max(1) as usize;
        let start = window_start(self.colls_selection, visible, items.len());
        let db_label = self.current_db.as_deref().unwrap_or("-");
        let lines = items
            .iter()
            .enumerate()
            .skip(start)
            .take(visible)
            .map(|(idx, info)| {
                let marker = if idx == self.colls_selection {
                    ">"
                } else {
                    " "
                };
                let style = if idx == self.colls_selection {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Green)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Line::from(Span::styled(
                    format!("{marker} {:<36} {:>10}", info.name, info.doc_count),
                    style,
                ))
            })
            .collect::<Vec<_>>();
        Paragraph::new(if !lines.is_empty() {
            lines
        } else if self.colls_pending {
            vec![Line::from("loading\u{2026}")]
        } else {
            vec![Line::from(
                "No collections match this filter. Press Esc to clear the filter, r to refresh.",
            )]
        })
        .block(
            Block::default()
                .title(format!(
                    " collections in {db_label} ({}/{}) ",
                    self.colls_selection
                        .saturating_add(1)
                        .min(items.len().max(1)),
                    items.len()
                ))
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false })
    }

    fn collection_details_panel(&self) -> Paragraph<'_> {
        let mut lines = match self.filtered_collections().get(self.colls_selection) {
            Some(info) => vec![
                Line::from(format!("collection: {}", info.name)),
                Line::from(format!(
                    "documents: {} (estimated by the service)",
                    info.doc_count
                )),
                Line::from("i/Enter opens the paged document viewer"),
            ],
            None => vec![Line::from(
                "Select a collection and press Enter to page through its documents.",
            )],
        };

        if let Some(plan) = &self.operation_plan {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "operation plan",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(format!("action: {}", plan.kind.label())));
            lines.push(Line::from(format!(
                "target: {}.{}",
                plan.db, plan.collection
            )));
            lines.push(Line::from(format!("_id: {}", plan.id_label)));
            lines.push(Line::from(format!("risk: {}", plan.risk)));
            lines.push(Line::from(format!(
                "confirmations: {}/{}",
                plan.confirmations, plan.confirmations_required
            )));
        }

        Paragraph::new(lines)
            .block(Block::default().title(" Details ").borders(Borders::ALL))
            .wrap(Wrap { trim: false })
    }

    fn status_panel(&self) -> Paragraph<'_> {
        let hints = if self.mode == Mode::Filter {
            format!(
                "filter: {}\u{258f} Enter apply \u{b7} Esc clear \u{b7} {} match(es)",
                self.browser_filter,
                self.active_len()
            )
        } else {
            match self.view {
                View::Databases => DATABASES_HINTS.to_string(),
                View::Collections => COLLECTIONS_HINTS.to_string(),
            }
        };
        let (db, collection) = self.docs_namespace();
        Paragraph::new(vec![
            Line::from(format!(
                "{} | mode={} | dbs {} match={} | {}.{} docs {}/{} page {} skip={} | viewer {}/{} dropped {} | pending dbs={} colls={} docs={}",
                self.status,
                mode_label(self.mode),
                self.databases.len(),
                self.filtered_databases().len(),
                db,
                collection,
                self.docs.len(),
                self.filtered_docs().len(),
                self.docs_page + 1,
                self.docs_page as u64 * PAGE_SIZE,
                self.doc_lines.len(),
                self.doc_lines.max_lines,
                self.doc_lines.dropped_lines,
                self.dbs_pending,
                self.colls_pending,
                self.docs_pending,
            )),
            Line::from(Span::styled(hints, Style::default().fg(Color::DarkGray))),
        ])
        .block(Block::default().title(" Status ").borders(Borders::ALL))
    }

    fn draw_help(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(78, 84, area);
        frame.render_widget(Clear, popup);
        let help = vec![
            Line::from(Span::styled(
                "Navigation",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("1 databases browser \u{b7} 2 collections browser (Tab cycles)"),
            Line::from("j/k or arrows move \u{b7} PgUp/PgDn page \u{b7} g/G home/end"),
            Line::from("/ client-side filter on the active list (live, Esc clears)"),
            Line::from(Span::styled(
                "Browsing & paging",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("Enter on a database opens its collections browser"),
            Line::from(
                "i/Enter on a collection opens the paged document viewer (Find limit 10, skip page*10, sort {_id:1})",
            ),
            Line::from(
                "n next page (page is replaced; skip history kept) \u{b7} b back \u{b7} r refresh",
            ),
            Line::from(Span::styled(
                "Inspect",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from(
                "Enter on a document: typed full-screen viewer (ObjectId, dates, nested docs, arrays)",
            ),
            Line::from(
                "f server-side JSON query filter (rescans from page 0) \u{b7} / page filter with match count",
            ),
            Line::from(Span::styled(
                "Mutations (staged plans, y confirms)",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("d delete document: type the exact collection name, then y; _id is echoed"),
            Line::from(
                "the delete filter is always the exact viewed _id (at most one document) \u{b7} readonly launches block at confirm",
            ),
            Line::from(Span::styled(
                "Sessions",
                Style::default().add_modifier(Modifier::BOLD),
            )),
            Line::from("q: quit (asks for confirmation while a fetch or plan is active)"),
            Line::from("errors appear in a non-modal banner; any key acknowledges"),
        ];
        frame.render_widget(
            Paragraph::new(help)
                .block(Block::default().title(" Help ").borders(Borders::ALL))
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_docs_viewer(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(88, 84, area);
        frame.render_widget(Clear, popup);
        let visible = popup.height.saturating_sub(2).max(1) as usize;
        let items = self.filtered_docs();
        let start = window_start(self.docs_selection, visible, items.len());
        let skip = self.docs_page as u64 * PAGE_SIZE;
        let mut lines = Vec::new();
        for (idx, document) in items.iter().enumerate().skip(start).take(visible) {
            let marker = if idx == self.docs_selection { ">" } else { " " };
            let style = if idx == self.docs_selection {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Green)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            lines.push(Line::from(Span::styled(
                format!(
                    "{marker} [{}] {}",
                    skip as usize + idx + 1,
                    document_summary(document)
                ),
                style,
            )));
        }
        if lines.is_empty() {
            lines.push(if self.docs_pending {
                Line::from("loading\u{2026}")
            } else {
                Line::from(
                    "No documents on this page. Press n for the next page, f to change the query filter, / to filter this page.",
                )
            });
        }
        if self.docs_filtering {
            lines.push(Line::from(Span::styled(
                format!("filter: {}\u{258f}", self.docs_filter),
                Style::default().fg(Color::Cyan),
            )));
        }
        if let Some(plan) = &self.operation_plan {
            lines.push(Line::from(Span::styled(
                format!(
                    "operation plan: {} on {}.{} _id {} (confirmations {}/{}) \u{b7} y executes",
                    plan.kind.label(),
                    plan.db,
                    plan.collection,
                    plan.id_label,
                    plan.confirmations,
                    plan.confirmations_required
                ),
                Style::default().fg(Color::Yellow),
            )));
        }
        let (rows_start, rows_end) = if self.docs.is_empty() {
            (0, 0)
        } else {
            (skip as usize + 1, skip as usize + self.docs.len())
        };
        frame.render_widget(
            Paragraph::new(lines)
                .block(
                    Block::default()
                        .title(format!(
                            " documents {}.{} \u{b7} page {} \u{b7} rows {}-{}/{} \u{b7} page filter: {} \u{b7} j/k n/b i detail d del f query / filter Esc close ",
                            self.current_db.as_deref().unwrap_or("-"),
                            self.current_collection.as_deref().unwrap_or("-"),
                            self.docs_page + 1,
                            rows_start,
                            rows_end,
                            self.docs_total,
                            self.docs_filter_label(),
                        ))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_doc_viewer(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(88, 84, area);
        frame.render_widget(Clear, popup);
        let visible = popup.height.saturating_sub(2).max(1) as usize;
        let total = self.doc_lines.len();
        let mut body: Vec<Line> = if total == 0 {
            vec![Line::from(
                "nothing inspected yet; press Enter on a document",
            )]
        } else {
            let start = self.doc_scroll.min(total.saturating_sub(1));
            self.doc_lines
                .range(start, start + visible)
                .into_iter()
                .map(Line::from)
                .collect()
        };
        if let Some(plan) = &self.operation_plan {
            body.push(Line::from(Span::styled(
                format!(
                    "operation plan: {} on {}.{} _id {} (confirmations {}/{}) \u{b7} y executes",
                    plan.kind.label(),
                    plan.db,
                    plan.collection,
                    plan.id_label,
                    plan.confirmations,
                    plan.confirmations_required
                ),
                Style::default().fg(Color::Yellow),
            )));
        }
        frame.render_widget(
            Paragraph::new(body)
                .block(
                    Block::default()
                        .title(format!(
                            " document {}.{} \u{b7} line {}/{} \u{b7} j/k PgUp/PgDn scroll \u{b7} d del doc \u{b7} Esc close ",
                            self.current_db.as_deref().unwrap_or("-"),
                            self.current_collection.as_deref().unwrap_or("-"),
                            (self.doc_scroll + 1).min(total.max(1)),
                            total
                        ))
                        .borders(Borders::ALL),
                )
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_query_prompt(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(64, 24, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(vec![
                Line::from("query filter (server-side JSON equality):"),
                Line::from(Span::styled(
                    format!("{}\u{258f}", self.query_input),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from("example: {\"status\": \"failed\"}"),
                Line::from(
                    "empty clears the filter \u{b7} Enter rescans from page 0 \u{b7} Esc cancels",
                ),
            ])
            .block(
                Block::default()
                    .title(" Query Filter ")
                    .borders(Borders::ALL),
            ),
            popup,
        );
    }

    fn draw_confirm_delete(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(68, 30, area);
        frame.render_widget(Clear, popup);
        let collection = self.current_collection.as_deref().unwrap_or("-");
        let db = self.current_db.as_deref().unwrap_or("-");
        let id_label = self
            .delete_target
            .as_ref()
            .and_then(|document| document.get("_id"))
            .map(bson_value_label)
            .unwrap_or_else(|| "(missing _id)".to_string());
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Delete document (destructive)",
                    Style::default()
                        .fg(Color::Red)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(format!("target: {db}.{collection}")),
                Line::from(format!("_id: {id_label}")),
                Line::from("The delete filter is anchored on this _id and matches at most one document."),
                Line::from("Type the exact collection name to confirm:"),
                Line::from(Span::styled(
                    format!("{}\u{258f}", self.typed_collection_input),
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(""),
                Line::from(
                    "Enter stages the plan (typed confirmation) \u{b7} y executes \u{b7} Esc cancels",
                ),
            ])
            .block(
                Block::default()
                    .title(" Confirm Delete ")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_confirm_quit(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(56, 22, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Quit with active work?",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(self.active_work_summary()),
                Line::from("y/Enter: quit \u{b7} any other key: stay"),
            ])
            .block(
                Block::default()
                    .title(" Confirm Quit ")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn active_work_summary(&self) -> String {
        let mut parts = Vec::new();
        if self.dbs_pending {
            parts.push("database fetch in flight".to_string());
        }
        if self.colls_pending {
            parts.push("collection fetch in flight".to_string());
        }
        if self.docs_pending {
            parts.push("document page fetch in flight".to_string());
        }
        if let Some(plan) = &self.operation_plan {
            parts.push(format!(
                "{} plan on {}.{} (_id {})",
                plan.kind.label(),
                plan.db,
                plan.collection,
                plan.id_label
            ));
        }
        if parts.is_empty() {
            "no active fetches or plans".to_string()
        } else {
            parts.join(", ")
        }
    }

    fn draw_error_banner(&self, frame: &mut Frame, area: Rect, error: &str) {
        let banner = Rect {
            x: area.x + 1,
            y: area.height.saturating_sub(6),
            width: area.width.saturating_sub(2),
            height: 3,
        };
        frame.render_widget(Clear, banner);
        frame.render_widget(
            Paragraph::new(vec![Line::from(Span::styled(
                safe_error_summary(error),
                Style::default().fg(Color::Red),
            ))])
            .block(
                Block::default()
                    .title(" Error (any key acknowledges) ")
                    .borders(Borders::ALL)
                    .style(Style::default().fg(Color::Red)),
            )
            .wrap(Wrap { trim: false }),
            banner,
        );
    }
}

impl BoundedLines {
    fn new(max_lines: usize, max_bytes: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            bytes: 0,
            dropped_lines: 0,
            max_lines,
            max_bytes,
        }
    }

    fn len(&self) -> usize {
        self.lines.len()
    }

    fn clear(&mut self) {
        self.lines.clear();
        self.bytes = 0;
        self.dropped_lines = 0;
    }

    fn push_line(&mut self, mut line: String) {
        if line.len() > self.max_bytes {
            line.truncate(self.max_bytes);
        }
        self.bytes = self.bytes.saturating_add(line.len());
        self.lines.push_back(line);
        while self.lines.len() > self.max_lines || self.bytes > self.max_bytes {
            if let Some(removed) = self.lines.pop_front() {
                self.bytes = self.bytes.saturating_sub(removed.len());
                self.dropped_lines = self.dropped_lines.saturating_add(1);
            } else {
                break;
            }
        }
    }

    fn range(&self, start: usize, end: usize) -> Vec<String> {
        self.lines
            .iter()
            .skip(start)
            .take(end.saturating_sub(start))
            .cloned()
            .collect()
    }
}

fn safe_error_summary(message: &str) -> String {
    let mut summary = message.replace('\n', " ");
    if summary.len() > 180 {
        summary.truncate(177);
        summary.push_str("...");
    }
    summary
}

fn secret_leak_markers(text: &str) -> Vec<String> {
    [
        "mongodb_password_value",
        "mongodb_secret_data_value",
        "plaintext_credential_value",
        "raw_connection_url",
    ]
    .into_iter()
    .filter(|marker| text.contains(marker))
    .map(str::to_string)
    .collect()
}

fn mode_label(mode: Mode) -> &'static str {
    match mode {
        Mode::Browser => "browser",
        Mode::Filter => "filter",
    }
}

fn window_start(selected: usize, visible: usize, len: usize) -> usize {
    if visible == 0 || len <= visible {
        0
    } else if selected >= visible {
        (selected + 1).saturating_sub(visible)
    } else {
        0
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}

struct StandaloneMongoTabManager {
    render_tx: tokio::sync::mpsc::UnboundedSender<()>,
    should_quit: Arc<AtomicBool>,
}

impl StandaloneMongoTabManager {
    fn new(should_quit: Arc<AtomicBool>) -> Self {
        let (render_tx, _render_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            render_tx,
            should_quit,
        }
    }

    fn unsupported_tabs_error() -> anyhow::Error {
        anyhow::anyhow!(
            "MongoDB TUI does not host plugin tabs; use plugin-owned CLI commands instead"
        )
    }
}

impl TabManager for StandaloneMongoTabManager {
    fn open(&self, _title: String, _plugin_id: String, _context: Value) -> Result<()> {
        Err(Self::unsupported_tabs_error())
    }

    fn close_current(&self) -> Result<()> {
        self.quit()
    }

    fn set_title(&self, _title: String) -> Result<()> {
        Ok(())
    }

    fn request_render(&self) -> Result<()> {
        let _ = self.render_tx.send(());
        Ok(())
    }

    fn list_tabs(&self) -> Result<Vec<TabInfo>> {
        Ok(vec![TabInfo {
            index: 0,
            title: "MongoDB".to_string(),
            plugin_id: "mongodb".to_string(),
            context: json!({}),
            is_active: true,
        }])
    }

    fn close_tab(&self, index: usize) -> Result<()> {
        if index == 0 {
            self.quit()
        } else {
            Err(anyhow::anyhow!("MongoDB TUI has no tab {index}"))
        }
    }

    fn switch_to(&self, index: usize) -> Result<()> {
        if index == 0 {
            Ok(())
        } else {
            Err(anyhow::anyhow!("MongoDB TUI has no tab {index}"))
        }
    }

    fn active_tab_index(&self) -> Result<usize> {
        Ok(0)
    }

    fn quit(&self) -> Result<()> {
        self.should_quit.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyModifiers;

    fn fixture_launch(readonly: bool) -> MongoTuiLaunch {
        MongoTuiLaunch {
            profile_label: "fixture".to_string(),
            config: None,
            source: MongoTuiSource::Fixture,
            fixture_path: Some(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("fixtures/mongodb_tui_operations.json")
                    .display()
                    .to_string(),
            ),
            purpose: "operations".to_string(),
            readonly,
            restore: true,
            launch_plan: None,
        }
    }

    fn app() -> MongoTuiApp {
        MongoTuiApp::new(fixture_launch(false)).unwrap()
    }

    fn press(app: &mut MongoTuiApp, code: KeyCode) {
        app.handle_key(KeyEvent::new(code, KeyModifiers::NONE));
    }

    fn type_text(app: &mut MongoTuiApp, text: &str) {
        for ch in text.chars() {
            app.handle_key(KeyEvent::new(KeyCode::Char(ch), KeyModifiers::NONE));
        }
    }

    #[test]
    fn fixture_evidence_has_coverage_and_no_secret_markers() {
        let evidence = build_mongodb_tui_evidence(&fixture_launch(false)).unwrap();
        let rendered = serde_json::to_string(&evidence).unwrap();
        assert!(rendered.contains("mongodb_tui_fixture_evidence"));
        assert!(rendered.contains("typed_delete_confirmation"));
        assert!(rendered.contains("collection_paging_skip_history"));
        assert!(rendered.contains("document_viewer_typed_rendering"));
        assert!(rendered.contains("later candidate"));
        assert_eq!(evidence["secret_leak_scan"]["passed"], json!(true));
        assert_eq!(evidence["quality_gate"]["plugin_id"], json!("mongodb"));
        assert_eq!(
            evidence["quality_gate"]["layout"]["minimum_viewport"]["width"],
            json!(80)
        );
    }

    #[test]
    fn preflight_redacts_uri_and_credentials() {
        let launch = MongoTuiLaunch {
            profile_label: "live-cluster".to_string(),
            config: Some(MongoConfig {
                uri: "mongodb://ops:mongodb_password_value@cluster.internal:27017".to_string(),
                default_db: Some("appdb".to_string()),
                auth: Some(crate::config::MongoAuth::Password {
                    username: "ops".to_string(),
                    password: "mongodb_password_value".to_string(),
                    auth_db: Some("admin".to_string()),
                }),
                timeout: 10,
                tls: crate::config::MongoTls::default(),
            }),
            source: MongoTuiSource::Profile,
            fixture_path: None,
            purpose: "triage".to_string(),
            readonly: false,
            restore: true,
            launch_plan: None,
        };
        let rendered = serde_json::to_string(&preflight_value(&launch)).unwrap();
        assert!(rendered.contains("redacted"));
        assert!(!rendered.contains("mongodb_password_value"));
        assert!(!rendered.contains("cluster.internal"), "hosts never render");
        assert!(!rendered.contains("raw_connection_url"));
        assert_eq!(preflight_value(&launch)["ok"], json!(true));
        assert_eq!(preflight_value(&launch)["command"], json!("mongodb tui"));
        assert_eq!(preflight_value(&launch)["default_db"], json!("appdb"));
        assert_eq!(
            preflight_value(&launch)["connection"]["auth"],
            json!("password")
        );
        assert_eq!(
            preflight_value(&launch)["connection"]["kind"],
            json!("mongodb")
        );
    }

    #[test]
    fn startup_target_error_banner_is_non_modal() {
        let mut app = app();
        assert!(app.error_banner.is_some());
        assert!(
            app.error_banner
                .as_deref()
                .unwrap()
                .contains("target error")
        );
        assert_eq!(app.mode, Mode::Browser, "errors must not hijack the mode");
        assert_eq!(
            app.databases.len(),
            4,
            "fixture databases load despite the banner"
        );
        assert_eq!(app.current_db.as_deref(), Some("appdb"));
        assert_eq!(app.collections.len(), 4, "default db collections preselect");
        press(&mut app, KeyCode::Char('j'));
        assert!(app.error_banner.is_none(), "any key acknowledges");
        assert_eq!(app.dbs_selection, 1, "the key keeps its normal meaning");
    }

    #[test]
    fn hotkeys_switch_views_and_tab_cycles() {
        let mut app = app();
        assert_eq!(app.view, View::Databases);
        press(&mut app, KeyCode::Char('2'));
        assert_eq!(app.view, View::Collections);
        assert!(app.status.contains("view: collections"));
        press(&mut app, KeyCode::Tab);
        assert_eq!(app.view, View::Databases);
        press(&mut app, KeyCode::Char('1'));
        assert_eq!(app.view, View::Databases);
    }

    #[test]
    fn database_selection_opens_collections() {
        let mut app = app();
        app.set_view(View::Databases);
        app.dbs_selection = 1; // analytics
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.view, View::Collections);
        assert_eq!(app.current_db.as_deref(), Some("analytics"));
        assert_eq!(app.collections.len(), 1);
        assert_eq!(app.collections[0].name, "events");
        // per-view selection memory: the databases selection survives
        press(&mut app, KeyCode::Char('1'));
        assert_eq!(app.view, View::Databases);
        assert_eq!(app.dbs_selection, 1, "selection memory per view");
    }

    #[test]
    fn collection_paging_tracks_page_total_and_history() {
        let mut app = app();
        app.set_view(View::Collections);
        app.colls_selection = 0; // orders (12 docs)
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::DocsViewer);
        assert_eq!(app.current_collection.as_deref(), Some("orders"));
        assert_eq!(app.docs.len(), PAGE_SIZE as usize);
        assert_eq!(app.docs_total, 12);
        assert_eq!(app.docs_page, 0);
        press(&mut app, KeyCode::Char('n'));
        assert_eq!(app.docs_page, 1);
        assert_eq!(app.docs.len(), 2, "partial final page is visible");
        press(&mut app, KeyCode::Char('n'));
        assert!(app.status.contains("end of collection reached"));
        press(&mut app, KeyCode::Char('b'));
        assert_eq!(app.docs_page, 0);
        assert_eq!(app.docs.len(), PAGE_SIZE as usize);
        press(&mut app, KeyCode::Char('b'));
        assert!(app.status.contains("no previous document page"));
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn query_filter_prompt_rescans_from_page_zero() {
        let mut app = app();
        open_orders(&mut app);
        press(&mut app, KeyCode::Char('f'));
        assert_eq!(app.overlay, Overlay::QueryFilterPrompt);
        type_text(&mut app, "{\"status\": \"failed\"}");
        press(&mut app, KeyCode::Enter);
        assert_eq!(
            app.overlay,
            Overlay::DocsViewer,
            "prompt returns to the viewer"
        );
        assert_eq!(app.docs_page, 0);
        assert_eq!(app.docs_total, 3, "fixture honors equality filters");
        assert_eq!(app.docs.len(), 3);
        assert!(app.status.contains("query filter set to"));
        assert!(app.status.contains("rescanning from page 0"));
        // clear again
        press(&mut app, KeyCode::Char('f'));
        press(&mut app, KeyCode::Enter);
        assert!(app.query_filter.is_empty());
        assert_eq!(app.docs_total, 12);
    }

    #[test]
    fn query_filter_prompt_rejects_invalid_json() {
        let mut app = app();
        open_orders(&mut app);
        press(&mut app, KeyCode::Char('f'));
        type_text(&mut app, "{\"status\": }");
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::QueryFilterPrompt, "prompt stays open");
        assert!(app.status.contains("invalid query filter JSON"));
        assert!(app.query_filter.is_empty());
        press(&mut app, KeyCode::Esc);
        assert_eq!(
            app.overlay,
            Overlay::DocsViewer,
            "prompt returns to the viewer"
        );
    }

    #[test]
    fn page_filter_reports_match_count() {
        let mut app = app();
        open_orders(&mut app);
        press(&mut app, KeyCode::Char('/'));
        assert!(app.docs_filtering);
        type_text(&mut app, "failed");
        press(&mut app, KeyCode::Enter);
        assert!(!app.docs_filtering);
        assert_eq!(app.filtered_docs().len(), 3);
        assert!(app.status.contains("page filter applied: 'failed'"));
        assert!(app.status.contains("3 match(es) on this page"));
        press(&mut app, KeyCode::Char('/'));
        press(&mut app, KeyCode::Esc);
        assert!(app.docs_filter.is_empty());
        assert!(app.status.contains("page filter cleared"));
    }

    #[test]
    fn document_detail_renders_typed_values() {
        let mut app = app();
        open_orders(&mut app);
        app.set_docs_selected(1); // SO-1002: nested customer, items array, $date, null notes absent
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::DocViewer);
        let rendered = app.doc_lines.range(0, app.doc_lines.len()).join("\n");
        assert!(
            rendered.contains("_id: ObjectId(\"665f1a2b3c4d5e6f7a8b9c02\")"),
            "ObjectId renders typed: {rendered}"
        );
        assert!(rendered.contains("customer: {"), "nested docs expand");
        assert!(rendered.contains("name: Nordwind GmbH"));
        assert!(rendered.contains("items: [1]"), "arrays are itemized");
        assert!(rendered.contains("[0]: {"), "array items expand");
        assert!(
            rendered.contains("created_at: 2026-05-17"),
            "dates render RFC-3339"
        );
        // users collection exercises null rendering
        press(&mut app, KeyCode::Esc);
        press(&mut app, KeyCode::Esc);
        app.set_view(View::Collections);
        app.colls_selection = 1; // users
        press(&mut app, KeyCode::Enter);
        app.set_docs_selected(2); // svc-ingest: locale null, last_login null
        press(&mut app, KeyCode::Enter);
        let rendered = app.doc_lines.range(0, app.doc_lines.len()).join("\n");
        assert!(
            rendered.contains("locale: (null)"),
            "null renders literally"
        );
        assert!(rendered.contains("last_login: (null)"));
        press(&mut app, KeyCode::Esc);
    }

    #[test]
    fn typed_delete_requires_exact_collection_name() {
        let mut app = app();
        open_orders(&mut app);
        press(&mut app, KeyCode::Char('d'));
        assert_eq!(app.overlay, Overlay::ConfirmDelete);
        type_text(&mut app, "users");
        press(&mut app, KeyCode::Enter);
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("typed name does not match orders"));
        assert_eq!(app.overlay, Overlay::ConfirmDelete);
        type_text(&mut app, "orders");
        press(&mut app, KeyCode::Enter);
        assert_eq!(
            app.overlay,
            Overlay::DocsViewer,
            "staging returns to the viewer"
        );
        let plan = app.operation_plan.clone().expect("delete plan staged");
        assert_eq!(plan.kind, OperationKind::DeleteDocument);
        assert_eq!(plan.collection, "orders");
        assert_eq!(plan.db, "appdb");
        assert_eq!(plan.confirmations_required, 2);
        assert_eq!(
            plan.confirmations, 1,
            "typed entry is the first confirmation"
        );
        assert!(app.status.contains("typed confirmation recorded"));
        assert!(app.status.contains("press y to execute"));
        assert!(
            plan.id_label.contains("665f1a2b3c4d5e6f7a8b9c01"),
            "the _id echo is recorded in the plan"
        );
    }

    #[test]
    fn delete_executes_after_y_and_refetches_page() {
        let mut app = app();
        open_orders(&mut app);
        press(&mut app, KeyCode::Char('n')); // page 2: SO-1011, SO-1012 (poisoned)
        assert_eq!(app.docs.len(), 2);
        app.set_docs_selected(1); // SO-1012
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "orders");
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('y'));
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("fixture executed delete document"));
        assert!(app.status.contains("1 document(s) deleted"));
        assert_eq!(app.docs_total, 11, "the refetched page reflects the delete");
        assert_eq!(app.docs.len(), 1, "page 2 now holds only SO-1011");
        assert!(
            !app.docs
                .iter()
                .any(|doc| document_summary(doc).contains("SO-1012")),
            "the deleted document is gone from the refetched page"
        );
        // collections doc_count reflects the mutation on reload
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
        app.set_view(View::Collections);
        press(&mut app, KeyCode::Char('r'));
        let orders = app
            .collections
            .iter()
            .find(|info| info.name == "orders")
            .expect("orders still listed");
        assert_eq!(orders.doc_count, 11);
    }

    #[test]
    fn delete_refused_without_document_id() {
        let mut app = app();
        open_orders(&mut app);
        app.docs = vec![doc_like_without_id()];
        app.docs_total = 1;
        app.set_docs_selected(0);
        press(&mut app, KeyCode::Char('d'));
        assert_eq!(app.overlay, Overlay::DocsViewer, "the viewer stays open");
        assert!(
            app.status.contains("document has no _id"),
            "unanchored deletes are refused"
        );
        assert!(app.operation_plan.is_none());
    }

    fn doc_like_without_id() -> Document {
        let value: Value = serde_json::json!({ "payload": "id-less" });
        bson_document_from_json(&value).unwrap()
    }

    #[test]
    fn erroring_collection_stays_responsive() {
        let mut app = app();
        app.set_view(View::Collections);
        app.colls_selection = 2; // appdb.locked_collection
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::None, "the viewer refuses to open");
        assert!(app.error_banner.is_some());
        assert!(app.status.contains("target error"));
        assert_eq!(app.mode, Mode::Browser);
        press(&mut app, KeyCode::Char('j'));
        assert!(app.error_banner.is_none(), "banner stays non-modal");
    }

    #[test]
    fn readonly_launch_blocks_confirmed_plan() {
        let mut app = MongoTuiApp::new(fixture_launch(true)).unwrap();
        open_orders(&mut app);
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "orders");
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Char('y'));
        assert!(app.operation_plan.is_some());
        assert!(app.status.contains("readonly launch blocked"));
        assert_eq!(app.docs_total, 12, "readonly launches must not mutate");
        assert_eq!(app.docs.len(), PAGE_SIZE as usize);
    }

    #[test]
    fn empty_collection_shows_empty_state() {
        let mut app = app();
        app.set_view(View::Collections);
        app.colls_selection = 3; // appdb.empty_collection
        press(&mut app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::DocsViewer);
        assert!(app.docs.is_empty());
        assert_eq!(app.docs_total, 0);
        press(&mut app, KeyCode::Char('d'));
        assert!(
            app.status.contains("select a document to delete"),
            "mutations refuse to stage with nothing selected"
        );
        assert!(app.operation_plan.is_none());
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
    }

    #[test]
    fn quit_with_active_plan_requires_confirmation() {
        let mut app = app();
        open_orders(&mut app);
        press(&mut app, KeyCode::Char('d'));
        type_text(&mut app, "orders");
        press(&mut app, KeyCode::Enter);
        assert!(app.operation_plan.is_some(), "plan staged");
        press(&mut app, KeyCode::Esc); // close the viewer; the plan stays staged
        assert!(app.operation_plan.is_some());
        press(&mut app, KeyCode::Char('q'));
        assert_eq!(app.overlay, Overlay::ConfirmQuit);
        assert!(!app.should_quit);
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.overlay, Overlay::None);
        assert!(!app.should_quit);
        press(&mut app, KeyCode::Char('q'));
        press(&mut app, KeyCode::Char('y'));
        assert!(app.should_quit);
    }

    #[test]
    fn page_keys_move_document_selection() {
        let mut app = app();
        open_orders(&mut app);
        press(&mut app, KeyCode::PageDown);
        assert_eq!(
            app.docs_selection,
            9.min(app.filtered_docs().len().saturating_sub(1))
        );
        press(&mut app, KeyCode::PageUp);
        assert_eq!(app.docs_selection, 0);
    }

    #[test]
    fn fixture_documents_convert_with_typed_bson_values() {
        let data = load_fixture(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/mongodb_tui_operations.json")
                .display()
                .to_string()
                .as_str(),
        )
        .unwrap();
        let orders = data
            .finds
            .iter()
            .find(|find| find.collection == "orders")
            .expect("orders fixture");
        assert_eq!(orders.documents.len(), 12);
        assert!(matches!(
            orders.documents[0].get("_id"),
            Some(Bson::ObjectId(_))
        ));
        assert!(matches!(
            orders.documents[0].get("created_at"),
            Some(Bson::DateTime(_))
        ));
        assert!(matches!(
            orders.documents[0].get("items"),
            Some(Bson::Array(_))
        ));
        assert!(matches!(orders.documents[0].get("notes"), Some(Bson::Null)));
        let access_log = data
            .finds
            .iter()
            .find(|find| find.collection == "access_log")
            .expect("access_log fixture");
        assert!(matches!(
            access_log.documents[0].get("digest"),
            Some(Bson::Binary(_))
        ));
    }

    #[test]
    fn resolve_path_supports_dotted_query_paths() {
        let data = load_fixture(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("fixtures/mongodb_tui_operations.json")
                .display()
                .to_string()
                .as_str(),
        )
        .unwrap();
        let filter = bson_document_from_json(&serde_json::json!({
            "customer.tier": "platinum"
        }))
        .unwrap();
        let (docs, total) = data.resolve_find("appdb", "orders", &filter, 0, PAGE_SIZE);
        assert_eq!(total, 2);
        assert_eq!(docs.len(), 2);
    }

    fn open_orders(app: &mut MongoTuiApp) {
        app.set_view(View::Collections);
        app.colls_selection = 0; // orders
        press(app, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::DocsViewer);
    }
}
