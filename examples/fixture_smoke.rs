//! MongoDB fixture-backed capability smoke driver.
//!
//! This example is script-facing. It exercises the MongoDB plugin capability
//! surface against a disposable local MongoDB fixture using only the generated
//! database and collection from the fixture environment.

#![allow(clippy::result_large_err)]

use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use serde_json::{Value, json};
use voidb_core::{
    ActorRef, ActorType, AgentSessionBinding, AgentSessionCallRequest, AgentSessionOpenContext,
    AgentSessionOpenRequest, AgentSessionRef, CapabilityError, CapabilityErrorCategory,
    CapabilityInvocation, CapabilityInvocationResult, InvocationAcknowledgement,
    InvocationConnectionTarget, InvocationControls, InvocationStatus, Pagination,
    PluginAgentSession, PluginAgentSessionFactory, PluginSessionHealth, PluginSessionPurpose,
    RedactionStatus,
};
use voidb_plugin_mongodb::{MongoAgentSessionFactory, MongoConfig, invoke_mongodb_capability};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let config = config_from_env()?;
    let database = required_env("VOIDB_MONGODB_SMOKE_DATABASE")?;
    let collection = required_env("VOIDB_MONGODB_SMOKE_COLLECTION")?;
    let seed_document_id = required_env("VOIDB_MONGODB_SMOKE_DOCUMENT_ID")?;
    ensure!(
        database.starts_with("voidb_fixture_"),
        "refusing MongoDB smoke outside a generated fixture database"
    );
    ensure!(
        collection == "fixture_accounts",
        "refusing MongoDB smoke outside the generated fixture collection"
    );

    let run_id =
        std::env::var("VOIDB_FIXTURE_RUN_ID").unwrap_or_else(|_| "mongodb-fixture-smoke".into());
    let safe_id = mongodb_safe_id(&run_id);
    let scratch_id = format!("fixture-{safe_id}-scratch");
    let delete_id = format!("fixture-{safe_id}-delete");
    let dry_secret = "voidb-mongodb-fixture-dry-run-secret";

    let diagnostics = invoke_checked(
        &config,
        "diagnostics",
        json!({}),
        false,
        false,
        None,
        "mongodb.diagnostics",
    )
    .await?;
    ensure_succeeded(&diagnostics, "mongodb.diagnostics")?;
    ensure!(
        diagnostics.output["uri_scheme"] == "mongodb"
            && diagnostics.output["default_db_present"] == true
            && diagnostics.output["auth_type"].is_null()
            && diagnostics.output["network_checked"] == false,
        "diagnostics should return shape-only metadata: {}",
        diagnostics.output
    );
    ensure_result_excludes(&diagnostics, secret_samples(&config), "mongodb.diagnostics")?;

    let dry_insert = invoke_checked(
        &unavailable_config(),
        "insert",
        json!({
            "database": database,
            "collection": collection,
            "document": { "_id": format!("fixture-{safe_id}-dry-insert"), "note": dry_secret }
        }),
        true,
        false,
        None,
        "mongodb.insert dry-run should not require a live target",
    )
    .await?;
    ensure_succeeded(&dry_insert, "mongodb.insert dry-run")?;
    ensure!(
        dry_insert.output["dry_run"] == true,
        "insert dry-run output"
    );
    ensure_result_excludes(&dry_insert, dry_secret, "mongodb.insert dry-run")?;

    for (capability, input) in [
        (
            "update",
            json!({
                "database": database,
                "collection": collection,
                "filter": { "_id": scratch_id },
                "update": { "$set": { "note": dry_secret } }
            }),
        ),
        (
            "delete",
            json!({
                "database": database,
                "collection": collection,
                "filter": { "_id": scratch_id }
            }),
        ),
        (
            "create_index",
            json!({
                "database": database,
                "collection": collection,
                "keys": { "fixture_kind": 1 },
                "unique": false
            }),
        ),
        (
            "run_command",
            json!({
                "database": database,
                "command": { "createUser": "fixture-agent", "pwd": dry_secret }
            }),
        ),
    ] {
        let dry_run = invoke_checked(
            &unavailable_config(),
            capability,
            input,
            true,
            false,
            None,
            "mongodb destructive dry-run should not require a live target",
        )
        .await?;
        ensure_succeeded(&dry_run, capability)?;
        ensure!(
            dry_run.output["dry_run"] == true,
            "{capability} dry-run output"
        );
        ensure_result_excludes(&dry_run, dry_secret, capability)?;
    }

    let databases = invoke_checked(
        &config,
        "databases",
        json!({}),
        false,
        false,
        Some(Pagination {
            limit: 50,
            cursor: None,
        }),
        "mongodb.databases",
    )
    .await?;
    ensure_succeeded(&databases, "mongodb.databases")?;
    ensure_database_present(&databases.output, &database)?;

    let collections = invoke_checked(
        &config,
        "collections",
        json!({ "database": database }),
        false,
        false,
        None,
        "mongodb.collections",
    )
    .await?;
    ensure_succeeded(&collections, "mongodb.collections")?;
    ensure_collection_present(&collections.output, &collection)?;

    let paged_find = invoke_checked(
        &config,
        "find",
        json!({
            "database": database,
            "collection": collection,
            "sort": { "name": 1 }
        }),
        false,
        false,
        Some(Pagination {
            limit: 2,
            cursor: None,
        }),
        "mongodb.find paged",
    )
    .await?;
    ensure_succeeded(&paged_find, "mongodb.find paged")?;
    ensure!(
        paged_find.output["item_count"].as_u64() == Some(2)
            && paged_find.output["truncated"] == true
            && paged_find.output["next_cursor"].as_str() == Some("2"),
        "find should honor pagination: {}",
        paged_find.output
    );
    ensure_document_present(&paged_find.output, &seed_document_id)?;
    ensure_result_excludes(&paged_find, secret_samples(&config), "mongodb.find paged")?;

    let next_page = invoke_checked(
        &config,
        "find",
        json!({
            "database": database,
            "collection": collection,
            "sort": { "name": 1 }
        }),
        false,
        false,
        Some(Pagination {
            limit: 2,
            cursor: Some("2".into()),
        }),
        "mongodb.find next page",
    )
    .await?;
    ensure_succeeded(&next_page, "mongodb.find next page")?;
    ensure!(
        next_page.output["item_count"].as_u64() == Some(1)
            && next_page.output["truncated"] == false,
        "find next page should return remaining document: {}",
        next_page.output
    );

    let count_active = invoke_checked(
        &config,
        "count",
        json!({
            "database": database,
            "collection": collection,
            "filter": { "active": true }
        }),
        false,
        false,
        None,
        "mongodb.count active documents",
    )
    .await?;
    ensure_succeeded(&count_active, "mongodb.count")?;
    ensure!(
        count_active.output["count"].as_u64() == Some(2),
        "count should report active seed documents: {}",
        count_active.output
    );

    let aggregate = invoke_checked(
        &config,
        "aggregate",
        json!({
            "database": database,
            "collection": collection,
            "pipeline": [
                { "$match": { "active": true } },
                { "$sort": { "name": 1 } }
            ]
        }),
        false,
        false,
        Some(Pagination {
            limit: 10,
            cursor: None,
        }),
        "mongodb.aggregate active documents",
    )
    .await?;
    ensure_succeeded(&aggregate, "mongodb.aggregate")?;
    ensure!(
        aggregate.output["item_count"].as_u64() == Some(2),
        "aggregate should return active seed documents: {}",
        aggregate.output
    );

    ensure_policy_error(
        invoke(
            &unavailable_config(),
            "aggregate",
            json!({
                "database": database,
                "collection": collection,
                "pipeline": [{ "$match": {} }, { "$out": "fixture_archive" }]
            }),
            false,
            false,
            None,
        )
        .await,
        "validation.mongodb_aggregate_mutating_stage",
        "mongodb.aggregate mutating stage",
    )?;

    let indexes = invoke_checked(
        &config,
        "indexes",
        json!({ "database": database, "collection": collection }),
        false,
        false,
        None,
        "mongodb.indexes",
    )
    .await?;
    ensure_succeeded(&indexes, "mongodb.indexes")?;
    ensure_index_present(&indexes.output, "active_1_name_1")?;

    cleanup_document(&config, &database, &collection, &scratch_id).await;
    cleanup_document(&config, &database, &collection, &delete_id).await;

    let insert = invoke_checked(
        &config,
        "insert",
        json!({
            "database": database,
            "collection": collection,
            "document": {
                "_id": scratch_id,
                "name": "scratch fixture account",
                "balance_cents": 500,
                "active": true,
                "fixture_kind": "scratch"
            }
        }),
        false,
        true,
        None,
        "mongodb.insert scratch document",
    )
    .await?;
    ensure_succeeded(&insert, "mongodb.insert scratch")?;
    ensure!(
        insert.output["operation"] == "insert",
        "insert operation output"
    );

    let update = invoke_checked(
        &config,
        "update",
        json!({
            "database": database,
            "collection": collection,
            "filter": { "_id": scratch_id },
            "update": { "$set": { "balance_cents": 750, "fixture_updated": true } }
        }),
        false,
        true,
        None,
        "mongodb.update scratch document",
    )
    .await?;
    ensure_succeeded(&update, "mongodb.update scratch")?;
    ensure!(
        update.output["details"]["modified_count"].as_u64() == Some(1),
        "update should touch one scratch document: {}",
        update.output
    );

    let insert_delete = invoke_checked(
        &config,
        "insert",
        json!({
            "database": database,
            "collection": collection,
            "document": { "_id": delete_id, "name": "delete fixture account", "fixture_kind": "delete" }
        }),
        false,
        true,
        None,
        "mongodb.insert delete document",
    )
    .await?;
    ensure_succeeded(&insert_delete, "mongodb.insert delete")?;

    let dry_delete = invoke_checked(
        &config,
        "delete",
        json!({
            "database": database,
            "collection": collection,
            "filter": { "_id": delete_id }
        }),
        true,
        false,
        None,
        "mongodb.delete dry-run live target",
    )
    .await?;
    ensure_succeeded(&dry_delete, "mongodb.delete dry-run")?;
    ensure_document_count(&config, &database, &collection, &delete_id, 1).await?;

    let delete = invoke_checked(
        &config,
        "delete",
        json!({
            "database": database,
            "collection": collection,
            "filter": { "_id": delete_id }
        }),
        false,
        true,
        None,
        "mongodb.delete scratch document",
    )
    .await?;
    ensure_succeeded(&delete, "mongodb.delete scratch")?;
    ensure!(
        delete.output["details"]["deleted_count"].as_u64() == Some(1),
        "delete should touch one scratch document: {}",
        delete.output
    );
    ensure_document_count(&config, &database, &collection, &delete_id, 0).await?;

    ensure_bad_auth_redacts(&config, &database, &collection).await?;
    ensure_unavailable_target_redacts(&database).await?;
    cleanup_document(&config, &database, &collection, &scratch_id).await;
    run_live_session_smoke(
        &config,
        &database,
        &collection,
        &seed_document_id,
        &safe_id,
        &config.uri,
    )
    .await?;

    println!("mongodb fixture capability smoke passed");
    println!(
        "capabilities: diagnostics, databases, collections, find, count, aggregate, indexes, insert, update, delete, create_index dry-run, run_command dry-run, cursor_read, change_stream_read, bulk_write"
    );
    println!("fixture_database: generated");
    Ok(())
}

async fn run_live_session_smoke(
    config: &MongoConfig,
    database: &str,
    collection: &str,
    seed_document_id: &str,
    safe_id: &str,
    protected_value: &str,
) -> Result<()> {
    run_cursor_live_smoke(config, database, collection).await?;
    run_change_stream_live_smoke(config, database, collection, safe_id, protected_value).await?;
    run_bulk_partial_failure_smoke(config, database, collection, seed_document_id, safe_id).await?;
    Ok(())
}

async fn run_cursor_live_smoke(
    config: &MongoConfig,
    database: &str,
    collection: &str,
) -> Result<()> {
    let factory = MongoAgentSessionFactory::new(config.clone());
    let session = factory
        .open(mongo_live_context(
            "mongodb.cursor_read",
            PluginSessionPurpose::DatabaseQuery,
            json!({
                "resource": { "database": database, "collection": collection },
                "parameters": {
                    "mode": "find",
                    "filter": { "active": true },
                    "sort": { "name": 1 },
                    "batch_size": 1,
                    "max_time_ms": 2000
                }
            }),
        ))
        .await
        .map_err(|error| anyhow::anyhow!("open MongoDB cursor session: {error}"))?;

    let first = mongo_live_call(
        session.as_ref(),
        "mongodb.cursor_read",
        "mongodb-cursor-first",
        json!({ "max_events": 1, "max_bytes": 65536, "wait_timeout_ms": 2000 }),
    )
    .await?;
    ensure!(
        first["events"]
            .as_array()
            .is_some_and(|events| events.len() == 1),
        "MongoDB cursor did not return its first bounded event: {first}"
    );
    ensure!(
        first["dropped_events"] == 0 && first["coalesced_events"] == 0,
        "MongoDB source-paced cursor reported buffered loss: {first}"
    );
    let after_sequence = first["next_sequence"]
        .as_u64()
        .unwrap_or(1)
        .saturating_sub(1);
    let second = mongo_live_call(
        session.as_ref(),
        "mongodb.cursor_read",
        "mongodb-cursor-second",
        json!({
            "after_sequence": after_sequence,
            "max_events": 1,
            "max_bytes": 65536,
            "wait_timeout_ms": 2000
        }),
    )
    .await?;
    ensure!(
        second["events"]
            .as_array()
            .is_some_and(|events| events.len() == 1),
        "MongoDB cursor did not pace the next document: {second}"
    );
    ensure!(
        first["events"][0]["data"]["document"] != second["events"][0]["data"]["document"],
        "MongoDB cursor repeated a document between pulls"
    );
    session.close("fixture cursor cleanup".into()).await?;
    session
        .close("fixture cursor cleanup repeated".into())
        .await?;
    ensure!(session.health().await? == PluginSessionHealth::Closed);
    Ok(())
}

async fn run_change_stream_live_smoke(
    config: &MongoConfig,
    database: &str,
    collection: &str,
    safe_id: &str,
    protected_value: &str,
) -> Result<()> {
    let document_id = format!("fixture-{safe_id}-change-stream");
    cleanup_document(config, database, collection, &document_id).await;
    let factory = MongoAgentSessionFactory::new(config.clone());
    let session = factory
        .open(mongo_live_context(
            "mongodb.change_stream_read",
            PluginSessionPurpose::WatchStream,
            json!({
                "resource": { "database": database, "collection": collection },
                "parameters": {
                    "pipeline": [{ "$match": { "documentKey._id": document_id } }],
                    "batch_size": 1,
                    "max_await_ms": 5000,
                    "full_document": true
                }
            }),
        ))
        .await
        .map_err(|error| anyhow::anyhow!("open MongoDB change stream: {error}"))?;

    let inserted = invoke_checked(
        config,
        "insert",
        json!({
            "database": database,
            "collection": collection,
            "document": {
                "_id": document_id,
                "name": "change stream fixture",
                "protected_probe": protected_value
            }
        }),
        false,
        true,
        None,
        "MongoDB change-stream fixture insert",
    )
    .await?;
    ensure_succeeded(&inserted, "MongoDB change-stream fixture insert")?;

    let first = read_change_event(session.as_ref(), "mongodb-change-insert", None).await?;
    let encoded = serde_json::to_string(&first)?;
    ensure!(
        !encoded.contains(protected_value),
        "MongoDB change stream exposed configured credentials"
    );
    ensure!(
        first["events"][0]["data"]["resume_token_omitted"] == true
            && first["events"][0]["data"]["session_metadata_omitted"] == true,
        "MongoDB change stream exposed native resume/session metadata: {first}"
    );
    let resume = first["checkpoint"]["cursor"].clone();
    ensure!(
        resume["scope"]
            .as_str()
            .is_some_and(|scope| scope.starts_with("sha256:"))
    );
    session.close("fixture resume transition".into()).await?;

    let resumed = factory
        .open(mongo_live_context(
            "mongodb.change_stream_read",
            PluginSessionPurpose::WatchStream,
            json!({
                "resource": { "database": database, "collection": collection },
                "parameters": {
                    "pipeline": [{ "$match": { "documentKey._id": document_id } }],
                    "batch_size": 1,
                    "max_await_ms": 5000,
                    "full_document": true
                },
                "resume_from": resume
            }),
        ))
        .await
        .map_err(|error| anyhow::anyhow!("resume MongoDB change stream: {error}"))?;
    let updated = invoke_checked(
        config,
        "update",
        json!({
            "database": database,
            "collection": collection,
            "filter": { "_id": document_id },
            "update": { "$set": { "change_stream_updated": true } }
        }),
        false,
        true,
        None,
        "MongoDB change-stream fixture update",
    )
    .await?;
    ensure_succeeded(&updated, "MongoDB change-stream fixture update")?;
    let second = read_change_event(resumed.as_ref(), "mongodb-change-update", None).await?;
    ensure!(
        second["events"][0]["data"]["operation_type"] == "update",
        "MongoDB exact resume did not continue with the update event: {second}"
    );

    let after_sequence = second["next_sequence"]
        .as_u64()
        .unwrap_or(1)
        .saturating_sub(1);
    let idle = mongo_live_call(
        resumed.as_ref(),
        "mongodb.change_stream_read",
        "mongodb-change-idle",
        json!({
            "after_sequence": after_sequence,
            "max_events": 1,
            "max_bytes": 65536,
            "wait_timeout_ms": 1000
        }),
    )
    .await?;
    ensure!(
        idle["timed_out"] == true && idle["events"].as_array().is_some_and(Vec::is_empty),
        "MongoDB idle change-stream pull did not remain bounded: {idle}"
    );
    resumed.cancel("mongodb-change-cancel").await?;
    ensure!(resumed.health().await? == PluginSessionHealth::Closed);
    resumed.close("fixture cleanup".into()).await?;
    cleanup_document(config, database, collection, &document_id).await;
    Ok(())
}

async fn read_change_event(
    session: &dyn PluginAgentSession,
    call_prefix: &str,
    mut after_sequence: Option<u64>,
) -> Result<Value> {
    for attempt in 0..12 {
        let mut input = json!({
            "max_events": 1,
            "max_bytes": 65536,
            "wait_timeout_ms": 1000
        });
        if let Some(after_sequence) = after_sequence {
            input["after_sequence"] = json!(after_sequence);
        }
        let batch = mongo_live_call(
            session,
            "mongodb.change_stream_read",
            &format!("{call_prefix}-{attempt}"),
            input,
        )
        .await?;
        after_sequence = batch["next_sequence"]
            .as_u64()
            .map(|sequence| sequence.saturating_sub(1));
        if batch["events"]
            .as_array()
            .is_some_and(|events| !events.is_empty())
        {
            return Ok(batch);
        }
    }
    bail!("MongoDB change stream did not produce a fixture event")
}

async fn run_bulk_partial_failure_smoke(
    config: &MongoConfig,
    database: &str,
    collection: &str,
    seed_document_id: &str,
    safe_id: &str,
) -> Result<()> {
    let successful_id = format!("fixture-{safe_id}-bulk-success");
    cleanup_document(config, database, collection, &successful_id).await;
    let result = invoke_checked(
        config,
        "bulk_write",
        json!({
            "database": database,
            "collection": collection,
            "ordered": false,
            "operations": [
                {
                    "type": "insert",
                    "document": { "_id": seed_document_id, "fixture_kind": "duplicate" }
                },
                {
                    "type": "insert",
                    "document": { "_id": successful_id, "fixture_kind": "bulk-success" }
                }
            ]
        }),
        false,
        true,
        None,
        "MongoDB unordered bulk partial failure",
    )
    .await?;
    ensure_succeeded(&result, "MongoDB unordered bulk partial failure")?;
    ensure!(
        result.output["details"]["successful"] == 1
            && result.output["details"]["failed"] == 1
            && result.output["details"]["stopped_early"] == false,
        "MongoDB bulk partial failure was not machine-readable: {}",
        result.output
    );
    ensure!(
        result.output["details"]["items"]
            .as_array()
            .is_some_and(|items| {
                items.len() == 2
                    && items.iter().any(|item| item["ok"] == false)
                    && items.iter().any(|item| item["ok"] == true)
            }),
        "MongoDB bulk item results were incomplete: {}",
        result.output
    );
    cleanup_document(config, database, collection, &successful_id).await;
    Ok(())
}

fn mongo_live_context(
    capability: &str,
    purpose: PluginSessionPurpose,
    input: Value,
) -> AgentSessionOpenContext {
    AgentSessionOpenContext {
        binding: AgentSessionBinding {
            grant_id: "mongodb-fixture-live-grant".into(),
            profile_id: "mongodb-fixture-profile".into(),
            plugin_id: "mongodb".into(),
            purpose: purpose.clone(),
            allowed_capabilities: vec![capability.into()],
            host_generation: 1,
        },
        request: AgentSessionOpenRequest {
            purpose,
            capabilities: vec![capability.into()],
            lease_seconds: 60,
            concurrency: Default::default(),
            destructive_acknowledged: false,
            input,
        },
        lease_expires_at: Utc::now() + chrono::Duration::seconds(60),
    }
}

async fn mongo_live_call(
    session: &dyn PluginAgentSession,
    capability: &str,
    call_id: &str,
    input: Value,
) -> Result<Value> {
    session
        .call(AgentSessionCallRequest {
            session: AgentSessionRef::new("mongodb-fixture-live-session", 1),
            call_id: call_id.into(),
            capability: capability.into(),
            input,
            destructive_acknowledged: false,
            timeout_ms: Some(30_000),
            output_limit_bytes: 65536,
        })
        .await
        .map(|result| result.output)
        .map_err(|error| anyhow::anyhow!("MongoDB live-session call failed: {error}"))
}

fn config_from_env() -> Result<MongoConfig> {
    let database = required_env("VOIDB_MONGODB_SMOKE_DATABASE")?;
    Ok(MongoConfig {
        uri: required_env("VOIDB_MONGODB_SMOKE_URI")?,
        default_db: Some(database),
        auth: None,
        timeout: 10,
        tls: Default::default(),
    })
}

fn unavailable_config() -> MongoConfig {
    MongoConfig {
        uri: "mongodb://voidb_mongodb_user:voidbmongodbsecretunavailable@127.0.0.1:1/voidb_fixture_unavailable?authSource=voidb_fixture_unavailable".into(),
        default_db: Some("voidb_fixture_unavailable".into()),
        auth: None,
        timeout: 1,
        tls: Default::default(),
    }
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} is required"))
}

async fn invoke(
    config: &MongoConfig,
    capability_id: &str,
    input: Value,
    dry_run: bool,
    acknowledged: bool,
    page: Option<Pagination>,
) -> std::result::Result<CapabilityInvocationResult, CapabilityError> {
    invoke_mongodb_capability(
        config,
        CapabilityInvocation {
            id: format!("mongodb-fixture-smoke-{capability_id}"),
            plugin_id: "mongodb".into(),
            capability_id: capability_id.into(),
            connection: InvocationConnectionTarget::Stateless,
            input,
            controls: InvocationControls {
                dry_run,
                acknowledgement: acknowledged.then(acknowledgement),
                page,
                ..InvocationControls::default()
            },
            actor: Some(actor()),
            requested_at: Utc::now(),
        },
    )
    .await
}

async fn invoke_checked(
    config: &MongoConfig,
    capability_id: &str,
    input: Value,
    dry_run: bool,
    acknowledged: bool,
    page: Option<Pagination>,
    label: &str,
) -> Result<CapabilityInvocationResult> {
    invoke(config, capability_id, input, dry_run, acknowledged, page)
        .await
        .map_err(|error| {
            let error_json = serde_json::to_string(&error).unwrap_or_else(|_| format!("{error:?}"));
            anyhow::anyhow!("{label}: {error_json}")
        })
}

fn ensure_succeeded(result: &CapabilityInvocationResult, label: &str) -> Result<()> {
    ensure!(
        result.status == InvocationStatus::Succeeded,
        "{label} returned non-success status: {:?}",
        result.status
    );
    Ok(())
}

fn ensure_result_excludes(
    result: &CapabilityInvocationResult,
    sample: impl ProtectedSamples,
    label: &str,
) -> Result<()> {
    let text = serde_json::to_string(result)?;
    for sample in sample.samples() {
        ensure!(
            !text.contains(&sample),
            "{label} exposed protected MongoDB sample {sample}: {text}"
        );
    }
    Ok(())
}

fn ensure_error_excludes_config(
    error: &CapabilityError,
    config: &MongoConfig,
    label: &str,
) -> Result<()> {
    let text = serde_json::to_string(error)?;
    for sample in error_samples(config) {
        ensure!(
            !text.contains(&sample),
            "{label} error exposed MongoDB config material {sample}: {text}"
        );
    }
    ensure!(
        matches!(
            error.redaction,
            RedactionStatus::Applied | RedactionStatus::NotRequired
        ),
        "{label} redaction status should be non-failed: {:?}",
        error.redaction
    );
    Ok(())
}

trait ProtectedSamples {
    fn samples(self) -> Vec<String>;
}

impl ProtectedSamples for &str {
    fn samples(self) -> Vec<String> {
        vec![self.to_string()]
    }
}

impl ProtectedSamples for Vec<String> {
    fn samples(self) -> Vec<String> {
        self
    }
}

fn secret_samples(config: &MongoConfig) -> Vec<String> {
    let mut samples = vec![config.uri.clone()];
    samples.extend(uri_userinfo(&config.uri));
    samples
        .into_iter()
        .filter(|sample| sample.len() >= 4)
        .collect()
}

fn error_samples(config: &MongoConfig) -> Vec<String> {
    let mut samples = secret_samples(config);
    if let Some(default_db) = &config.default_db {
        samples.push(default_db.clone());
    }
    if let Some(host) = uri_authority(&config.uri) {
        samples.push(host);
    }
    if let Some(database) = uri_database(&config.uri) {
        samples.push(database);
    }
    samples
        .into_iter()
        .filter(|sample| sample.len() >= 4)
        .collect()
}

fn uri_authority(uri: &str) -> Option<String> {
    let (_, rest) = uri.split_once("://")?;
    let authority = rest
        .split(['/', '?'])
        .next()
        .filter(|authority| !authority.is_empty())?;
    let host = authority
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(authority);
    (!host.is_empty()).then(|| host.to_string())
}

fn uri_userinfo(uri: &str) -> Vec<String> {
    let Some((_, rest)) = uri.split_once("://") else {
        return Vec::new();
    };
    let Some(authority) = rest.split(['/', '?']).next() else {
        return Vec::new();
    };
    let Some((userinfo, _)) = authority.rsplit_once('@') else {
        return Vec::new();
    };
    userinfo
        .split(':')
        .filter(|value| value.len() >= 4)
        .map(str::to_string)
        .collect()
}

fn uri_database(uri: &str) -> Option<String> {
    let (_, rest) = uri.split_once("://")?;
    let (_, path_and_query) = rest.split_once('/')?;
    path_and_query
        .split('?')
        .next()
        .filter(|database| database.len() >= 4)
        .map(str::to_string)
}

fn ensure_database_present(output: &Value, database: &str) -> Result<()> {
    let databases = output["databases"]
        .as_array()
        .context("mongodb.databases output should include databases array")?;
    ensure!(
        databases.iter().any(|item| item["name"] == database),
        "mongodb.databases did not include expected database {database}: {output}"
    );
    Ok(())
}

fn ensure_collection_present(output: &Value, collection: &str) -> Result<()> {
    let collections = output["collections"]
        .as_array()
        .context("mongodb.collections output should include collections array")?;
    ensure!(
        collections.iter().any(|item| item["name"] == collection),
        "mongodb.collections did not include expected collection {collection}: {output}"
    );
    Ok(())
}

fn ensure_document_present(output: &Value, document_id: &str) -> Result<()> {
    let documents = output["documents"]
        .as_array()
        .context("mongodb.find output should include documents array")?;
    ensure!(
        documents.iter().any(|item| item["_id"] == document_id),
        "mongodb.find did not include expected document {document_id}: {output}"
    );
    Ok(())
}

fn ensure_index_present(output: &Value, index_name: &str) -> Result<()> {
    let indexes = output["indexes"]
        .as_array()
        .context("mongodb.indexes output should include indexes array")?;
    ensure!(
        indexes.iter().any(|item| item["name"] == index_name),
        "mongodb.indexes did not include expected index {index_name}: {output}"
    );
    Ok(())
}

fn ensure_policy_error(
    result: std::result::Result<CapabilityInvocationResult, CapabilityError>,
    expected_code: &str,
    label: &str,
) -> Result<()> {
    match result {
        Ok(result) => bail!(
            "{label}: expected policy/validation error, got {}",
            result.output
        ),
        Err(error) => {
            ensure!(
                matches!(
                    error.category,
                    CapabilityErrorCategory::Validation | CapabilityErrorCategory::Policy
                ),
                "{label}: expected policy/validation error, got {:?}",
                error.category
            );
            ensure!(
                error.code == expected_code,
                "{label}: expected {expected_code}, got {}",
                error.code
            );
        }
    }
    Ok(())
}

async fn ensure_document_count(
    config: &MongoConfig,
    database: &str,
    collection: &str,
    document_id: &str,
    expected: u64,
) -> Result<()> {
    let count = invoke_checked(
        config,
        "count",
        json!({
            "database": database,
            "collection": collection,
            "filter": { "_id": document_id }
        }),
        false,
        false,
        None,
        "mongodb.count scratch document",
    )
    .await?;
    ensure!(
        count.output["count"].as_u64() == Some(expected),
        "scratch document count should be {expected}: {}",
        count.output
    );
    Ok(())
}

async fn ensure_bad_auth_redacts(
    config: &MongoConfig,
    database: &str,
    collection: &str,
) -> Result<()> {
    let mut bad = config.clone();
    let password = required_env("VOIDB_MONGODB_SMOKE_PASSWORD")?;
    bad.uri = bad.uri.replace(&password, &format!("{password}-wrong"));

    match invoke(
        &bad,
        "count",
        json!({
            "database": database,
            "collection": collection,
            "filter": {}
        }),
        false,
        false,
        None,
    )
    .await
    {
        Ok(result) => bail!(
            "expected mongodb.count bad-auth failure, got output: {}",
            result.output
        ),
        Err(error) => {
            ensure!(
                error.category == CapabilityErrorCategory::TargetSystem,
                "bad auth should be a target error: {:?}",
                error.category
            );
            ensure_error_excludes_config(&error, &bad, "mongodb.count bad auth")?;
            ensure_error_excludes_config(&error, config, "mongodb.count bad auth")?;
        }
    }
    Ok(())
}

async fn ensure_unavailable_target_redacts(database: &str) -> Result<()> {
    let unavailable = unavailable_config();
    match invoke(
        &unavailable,
        "databases",
        json!({}),
        false,
        false,
        Some(Pagination {
            limit: 5,
            cursor: None,
        }),
    )
    .await
    {
        Ok(result) => bail!(
            "expected mongodb.databases unavailable failure, got output: {}",
            result.output
        ),
        Err(error) => {
            ensure!(
                error.category == CapabilityErrorCategory::TargetSystem,
                "unavailable target should be a target error: {:?}",
                error.category
            );
            ensure_error_excludes_config(&error, &unavailable, "mongodb.databases unavailable")?;
            let text = serde_json::to_string(&error)?;
            ensure!(
                !text.contains(database),
                "unavailable target error exposed fixture database: {text}"
            );
        }
    }
    Ok(())
}

async fn cleanup_document(
    config: &MongoConfig,
    database: &str,
    collection: &str,
    document_id: &str,
) {
    let _ = invoke(
        config,
        "delete",
        json!({
            "database": database,
            "collection": collection,
            "filter": { "_id": document_id }
        }),
        false,
        true,
        None,
    )
    .await;
}

fn mongodb_safe_id(value: &str) -> String {
    let mut safe = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    while safe.contains("--") {
        safe = safe.replace("--", "-");
    }
    safe.trim_matches('-').chars().take(32).collect()
}

fn actor() -> ActorRef {
    ActorRef {
        id: "agent:mongodb-fixture-smoke".into(),
        actor_type: ActorType::Agent,
    }
}

fn acknowledgement() -> InvocationAcknowledgement {
    InvocationAcknowledgement {
        actor: actor(),
        acknowledged_at: Utc::now(),
        reason: Some("fixture smoke mutation scoped to generated MongoDB database".into()),
        approval_id: Some("mongodb-fixture-smoke".into()),
    }
}
