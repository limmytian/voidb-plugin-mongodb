#![allow(clippy::result_large_err)]

use bson::{Bson, Document};
use serde_json::{Value, json};
use voidb_core::{
    CapabilityDefinition, CapabilityError, CapabilityErrorCategory, CapabilityInvocation,
    CapabilityInvocationResult, CapabilityRiskLevel, CredentialClass, InvocationOutputPage,
    InvocationStatus, Pagination, RedactionStatus, TargetSystemFailure, audit_json_summary,
};

use crate::agent_session::{
    CHANGE_STREAM_READ_CAPABILITY, CURSOR_READ_CAPABILITY, mongodb_live_session_contract,
};
use crate::config::{MongoAuth, MongoConfig};
use crate::service::agent_live::{MongoBulkOperation, execute_bulk};
use crate::service::{CollectionInfo, DbInfo, IndexInfo, MongoService};

const PLUGIN_ID: &str = "mongodb";
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_PAGE_LIMIT: usize = 100;
const MAX_PAGE_LIMIT: usize = 500;

pub fn mongodb_capabilities() -> Vec<CapabilityDefinition> {
    vec![
        capability(
            "diagnostics",
            "Return agent-safe MongoDB profile diagnostics without opening a cluster connection.",
            empty_input_schema(),
            json!({
                "type": "object",
                "required": [
                    "uri_scheme",
                    "default_db_present",
                    "auth_type",
                    "timeout_secs",
                    "tls_enabled",
                    "network_checked"
                ],
                "properties": {
                    "uri_scheme": { "type": ["string", "null"] },
                    "default_db_present": { "type": "boolean" },
                    "auth_type": { "type": ["string", "null"] },
                    "timeout_secs": { "type": "integer", "minimum": 0 },
                    "tls_enabled": { "type": "boolean" },
                    "network_checked": { "type": "boolean" }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "mongodb.diagnostics"],
            false,
            false,
            false,
        ),
        capability(
            "databases",
            "List MongoDB databases with bounded, cursor-based output.",
            empty_input_schema(),
            list_schema("databases", database_schema()),
            vec!["connection.read", "mongodb.databases.list"],
            false,
            false,
            false,
        ),
        capability(
            "collections",
            "List MongoDB collections in one database with bounded output.",
            json!({
                "type": "object",
                "required": ["database"],
                "properties": {
                    "database": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            list_schema("collections", collection_schema()),
            vec!["connection.read", "mongodb.collections.list"],
            false,
            false,
            false,
        ),
        capability(
            "find",
            "Find MongoDB documents with bounded, cursor-based output.",
            json!({
                "type": "object",
                "required": ["database", "collection"],
                "properties": {
                    "database": { "type": "string", "minLength": 1 },
                    "collection": { "type": "string", "minLength": 1 },
                    "filter": { "type": "object", "additionalProperties": true },
                    "sort": { "type": "object", "additionalProperties": true }
                },
                "additionalProperties": false
            }),
            documents_output_schema(),
            vec!["connection.read", "mongodb.documents.find"],
            false,
            false,
            false,
        ),
        capability(
            "count",
            "Count MongoDB documents matching a filter.",
            json!({
                "type": "object",
                "required": ["database", "collection"],
                "properties": {
                    "database": { "type": "string", "minLength": 1 },
                    "collection": { "type": "string", "minLength": 1 },
                    "filter": { "type": "object", "additionalProperties": true }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["database", "collection", "count", "filter_summary"],
                "properties": {
                    "database": { "type": "string" },
                    "collection": { "type": "string" },
                    "count": { "type": "integer", "minimum": 0 },
                    "filter_summary": { "type": "object" }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "mongodb.documents.count"],
            false,
            false,
            false,
        ),
        capability(
            "aggregate",
            "Run a read-only MongoDB aggregation pipeline with bounded output.",
            json!({
                "type": "object",
                "required": ["database", "collection", "pipeline"],
                "properties": {
                    "database": { "type": "string", "minLength": 1 },
                    "collection": { "type": "string", "minLength": 1 },
                    "pipeline": {
                        "type": "array",
                        "items": { "type": "object", "additionalProperties": true }
                    }
                },
                "additionalProperties": false
            }),
            documents_output_schema(),
            vec!["connection.read", "mongodb.documents.aggregate"],
            false,
            false,
            false,
        ),
        live_capability(
            CURSOR_READ_CAPABILITY,
            "Read a persistent MongoDB find or aggregation cursor with source-paced batches.",
            vec!["connection.read", "mongodb.documents.find"],
        ),
        live_capability(
            CHANGE_STREAM_READ_CAPABILITY,
            "Read a MongoDB collection change stream with scoped resume tokens.",
            vec!["connection.read", "mongodb.change_streams.read"],
        ),
        capability(
            "indexes",
            "List MongoDB indexes for one collection with bounded output.",
            json!({
                "type": "object",
                "required": ["database", "collection"],
                "properties": {
                    "database": { "type": "string", "minLength": 1 },
                    "collection": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            list_schema("indexes", index_schema()),
            vec!["connection.read", "mongodb.indexes.list"],
            false,
            false,
            false,
        ),
        capability(
            "insert",
            "Insert one MongoDB document.",
            json!({
                "type": "object",
                "required": ["database", "collection", "document"],
                "properties": {
                    "database": { "type": "string", "minLength": 1 },
                    "collection": { "type": "string", "minLength": 1 },
                    "document": { "type": "object", "additionalProperties": true }
                },
                "additionalProperties": false
            }),
            mutation_output_schema(),
            vec!["connection.write", "mongodb.documents.insert"],
            true,
            false,
            true,
        ),
        capability(
            "update",
            "Update MongoDB documents matching a filter.",
            json!({
                "type": "object",
                "required": ["database", "collection", "filter", "update"],
                "properties": {
                    "database": { "type": "string", "minLength": 1 },
                    "collection": { "type": "string", "minLength": 1 },
                    "filter": { "type": "object", "additionalProperties": true },
                    "update": { "type": "object", "additionalProperties": true }
                },
                "additionalProperties": false
            }),
            mutation_output_schema(),
            vec!["connection.write", "mongodb.documents.update"],
            true,
            false,
            true,
        ),
        capability(
            "delete",
            "Delete MongoDB documents matching a filter.",
            json!({
                "type": "object",
                "required": ["database", "collection", "filter"],
                "properties": {
                    "database": { "type": "string", "minLength": 1 },
                    "collection": { "type": "string", "minLength": 1 },
                    "filter": { "type": "object", "additionalProperties": true }
                },
                "additionalProperties": false
            }),
            mutation_output_schema(),
            vec!["connection.write", "mongodb.documents.delete"],
            true,
            false,
            true,
        ),
        capability(
            "bulk_write",
            "Execute a bounded ordered or unordered MongoDB write batch with item-level results.",
            json!({
                "type": "object",
                "required": ["database", "collection", "operations"],
                "properties": {
                    "database": { "type": "string", "minLength": 1 },
                    "collection": { "type": "string", "minLength": 1 },
                    "ordered": { "type": "boolean", "default": true },
                    "operations": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 100,
                        "items": {
                            "type": "object",
                            "required": ["type"],
                            "properties": {
                                "type": { "type": "string", "enum": ["insert", "update", "delete"] },
                                "document": { "type": "object", "additionalProperties": true },
                                "filter": { "type": "object", "additionalProperties": true },
                                "update": { "type": "object", "additionalProperties": true }
                            },
                            "additionalProperties": false
                        }
                    }
                },
                "additionalProperties": false
            }),
            mutation_output_schema(),
            vec!["connection.write", "mongodb.documents.bulk_write"],
            true,
            false,
            true,
        ),
        capability(
            "create_index",
            "Create one MongoDB index.",
            json!({
                "type": "object",
                "required": ["database", "collection", "keys"],
                "properties": {
                    "database": { "type": "string", "minLength": 1 },
                    "collection": { "type": "string", "minLength": 1 },
                    "keys": { "type": "object", "additionalProperties": true },
                    "unique": { "type": "boolean", "default": false }
                },
                "additionalProperties": false
            }),
            mutation_output_schema(),
            vec!["connection.write", "mongodb.indexes.create"],
            true,
            false,
            true,
        ),
        capability(
            "run_command",
            "Run a raw MongoDB database command; all raw commands are destructive-gated.",
            json!({
                "type": "object",
                "required": ["database", "command"],
                "properties": {
                    "database": { "type": "string", "minLength": 1 },
                    "command": { "type": "object", "additionalProperties": true }
                },
                "additionalProperties": false
            }),
            json!({
                "type": "object",
                "required": ["ok", "operation", "dry_run", "destructive", "details"],
                "properties": {
                    "ok": { "type": "boolean" },
                    "operation": { "type": "string" },
                    "dry_run": { "type": "boolean" },
                    "would_execute": { "type": "boolean" },
                    "destructive": { "type": "boolean" },
                    "details": { "type": "object" },
                    "result": { "type": ["object", "null"] },
                    "result_summary": { "type": ["object", "null"] }
                },
                "additionalProperties": false
            }),
            vec!["connection.write", "mongodb.raw_command"],
            true,
            false,
            true,
        ),
    ]
}

pub async fn invoke_mongodb_capability(
    config: &MongoConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    if invocation.plugin_id != PLUGIN_ID {
        return Err(validation_error(
            "validation.plugin_mismatch",
            "Invocation plugin_id does not match MongoDB.",
            json!({ "expected": PLUGIN_ID, "actual": invocation.plugin_id }),
        ));
    }

    match invocation.capability_id.as_str() {
        "diagnostics" => Ok(diagnostics_result(config, invocation.id)),
        "databases" => invoke_databases(config, invocation).await,
        "collections" => invoke_collections(config, invocation).await,
        "find" => invoke_find(config, invocation).await,
        "count" => invoke_count(config, invocation).await,
        "aggregate" => invoke_aggregate(config, invocation).await,
        "cursor_read" | "change_stream_read" => Err(unavailable_error(
            "unavailable.session_required",
            "This MongoDB live workflow requires a persistent agent session.",
            json!({ "capability_id": invocation.capability_id }),
        )),
        "indexes" => invoke_indexes(config, invocation).await,
        "insert" => invoke_insert(config, invocation).await,
        "update" => invoke_update(config, invocation).await,
        "delete" => invoke_delete(config, invocation).await,
        "bulk_write" => invoke_bulk_write(config, invocation).await,
        "create_index" => invoke_create_index(config, invocation).await,
        "run_command" => invoke_run_command(config, invocation).await,
        other => Err(unavailable_error(
            "unavailable.capability_not_found",
            "MongoDB capability was not found.",
            json!({ "capability_id": other }),
        )),
    }
}

fn live_capability(
    qualified_id: &str,
    description: &str,
    permissions: Vec<&str>,
) -> CapabilityDefinition {
    let id = qualified_id
        .strip_prefix("mongodb.")
        .expect("MongoDB live capability ID");
    let (purpose, contract) =
        mongodb_live_session_contract(qualified_id).expect("MongoDB live-session contract");
    let handoff_capabilities = contract
        .operations
        .capabilities()
        .cloned()
        .collect::<Vec<_>>();
    CapabilityDefinition {
        plugin_id: PLUGIN_ID.to_string(),
        id: id.to_string(),
        description: description.to_string(),
        input_schema: live_read_schema(),
        output_schema: live_batch_schema(),
        permissions: permissions.into_iter().map(str::to_string).collect(),
        authorization: mongodb_live_authorization(qualified_id, purpose.clone()),
        risk: CapabilityRiskLevel::ReadOnly,
        destructive: false,
        streaming: true,
        execution_mode: voidb_core::CapabilityExecutionMode::SessionOnly,
        session_handoff: Some(
            voidb_core::CapabilitySessionHandoff::new(purpose, handoff_capabilities)
                .with_live_session(contract),
        ),
        connection_required: true,
        required_secret_classes: Vec::<CredentialClass>::new(),
        supports_dry_run: false,
        default_timeout_ms: Some(DEFAULT_TIMEOUT_MS),
    }
}

fn mongodb_live_authorization(
    capability: &str,
    purpose: voidb_core::PluginSessionPurpose,
) -> voidb_core::CapabilityAuthorizationMetadata {
    let mut fields = vec![
        voidb_core::CapabilityApprovalField::new(
            "/resource/database",
            "Database",
            voidb_core::CapabilityApprovalValueType::ResourceId,
        )
        .required(),
        voidb_core::CapabilityApprovalField::new(
            "/resource/collection",
            "Collection",
            voidb_core::CapabilityApprovalValueType::ResourceId,
        )
        .required()
        .with_constraint(voidb_core::CapabilityConstraintKind::Prefix),
    ];
    fields.push(
        voidb_core::CapabilityApprovalField::new(
            if capability == CURSOR_READ_CAPABILITY {
                "/parameters/filter"
            } else {
                "/parameters/pipeline"
            },
            "Query or change filter",
            voidb_core::CapabilityApprovalValueType::Json,
        )
        .with_constraint(voidb_core::CapabilityConstraintKind::Subset),
    );
    voidb_core::CapabilityAuthorizationMetadata::declared()
        .with_session_purposes(vec![purpose])
        .with_note(
            "MongoDB live cursors are isolated from run_command transaction sessions; resource scope and resume tokens are revalidated at open.",
        )
        .with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(fields))
}

fn live_read_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "after_sequence": { "type": "integer", "minimum": 0 },
            "max_events": { "type": "integer", "minimum": 1, "maximum": 1000 },
            "max_bytes": { "type": "integer", "minimum": 1, "maximum": 1048576 },
            "wait_timeout_ms": { "type": "integer", "minimum": 0, "maximum": 30000 }
        },
        "additionalProperties": false
    })
}

fn live_batch_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "protocol_version", "events", "next_sequence", "timed_out", "source_closed",
            "dropped_events", "dropped_bytes", "coalesced_events", "reconnect_attempts"
        ],
        "properties": {
            "protocol_version": { "type": "integer", "const": 1 },
            "events": { "type": "array", "maxItems": 1000 },
            "next_sequence": { "type": "integer", "minimum": 1 },
            "resume_cursor": { "type": "object" },
            "checkpoint": { "type": "object" },
            "oldest_available_sequence": { "type": "integer", "minimum": 1 },
            "truncated": { "type": "boolean" },
            "timed_out": { "type": "boolean" },
            "source_closed": { "type": "boolean" },
            "dropped_events": { "type": "integer", "minimum": 0 },
            "dropped_bytes": { "type": "integer", "minimum": 0 },
            "coalesced_events": { "type": "integer", "minimum": 0 },
            "reconnect_attempts": { "type": "integer", "minimum": 0 }
        },
        "additionalProperties": false
    })
}

#[allow(clippy::too_many_arguments)]
fn capability(
    id: &str,
    description: &str,
    input_schema: Value,
    output_schema: Value,
    permissions: Vec<&str>,
    destructive: bool,
    streaming: bool,
    supports_dry_run: bool,
) -> CapabilityDefinition {
    let session_handoff = (id == "run_command").then(|| {
        voidb_core::CapabilitySessionHandoff::new(
            voidb_core::PluginSessionPurpose::DatabaseTransaction,
            ["mongodb.run_command"],
        )
    });
    CapabilityDefinition {
        plugin_id: PLUGIN_ID.to_string(),
        id: id.to_string(),
        description: description.to_string(),
        input_schema,
        output_schema,
        permissions: permissions.into_iter().map(str::to_string).collect(),
        authorization: mongodb_authorization_metadata(id),
        risk: CapabilityRiskLevel::from_destructive(destructive),
        destructive,
        streaming,
        execution_mode: if session_handoff.is_some() {
            voidb_core::CapabilityExecutionMode::Both
        } else {
            voidb_core::CapabilityExecutionMode::Stateless
        },
        session_handoff,
        connection_required: true,
        required_secret_classes: Vec::<CredentialClass>::new(),
        supports_dry_run,
        default_timeout_ms: Some(DEFAULT_TIMEOUT_MS),
    }
}

fn mongodb_authorization_metadata(id: &str) -> voidb_core::CapabilityAuthorizationMetadata {
    let metadata = match id {
        "run_command" => voidb_core::CapabilityAuthorizationMetadata::declared()
            .with_session_purposes(vec![
                voidb_core::PluginSessionPurpose::DatabaseQuery,
                voidb_core::PluginSessionPurpose::DatabaseTransaction,
            ])
            .with_note("Raw command scope stays Custom; transaction state is session-bound."),
        "find" | "aggregate" | "count" => voidb_core::CapabilityAuthorizationMetadata::declared()
            .with_session_purposes(vec![voidb_core::PluginSessionPurpose::DatabaseQuery]),
        _ => voidb_core::CapabilityAuthorizationMetadata::declared(),
    };
    match id {
        "collections" => {
            metadata.with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(vec![
                voidb_core::CapabilityApprovalField::new(
                    "/database",
                    "Database",
                    voidb_core::CapabilityApprovalValueType::ResourceId,
                )
                .required(),
            ]))
        }
        "bulk_write" => {
            let mut fields = mongodb_collection_schema(true).fields;
            fields.push(
                voidb_core::CapabilityApprovalField::new(
                    "/operations",
                    "Bulk operations",
                    voidb_core::CapabilityApprovalValueType::Json,
                )
                .required()
                .with_constraint(voidb_core::CapabilityConstraintKind::Subset)
                .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive),
            );
            metadata.with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(fields))
        }
        "find" | "count" | "aggregate" | "indexes" | "insert" | "update" | "delete"
        | "create_index" => metadata.with_approval_schema(mongodb_collection_schema(matches!(
            id,
            "insert" | "update" | "delete" | "create_index"
        ))),
        "run_command" => {
            metadata.with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(vec![
                voidb_core::CapabilityApprovalField::new(
                    "/database",
                    "Database",
                    voidb_core::CapabilityApprovalValueType::ResourceId,
                )
                .required(),
                voidb_core::CapabilityApprovalField::new(
                    "/command",
                    "MongoDB command",
                    voidb_core::CapabilityApprovalValueType::Json,
                )
                .required()
                .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive),
            ]))
        }
        _ => metadata,
    }
}

fn mongodb_collection_schema(destructive: bool) -> voidb_core::CapabilityApprovalSchema {
    let mut collection = voidb_core::CapabilityApprovalField::new(
        "/collection",
        "Collection",
        voidb_core::CapabilityApprovalValueType::ResourceId,
    )
    .required();
    if destructive {
        collection =
            collection.with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::Destructive);
    }
    voidb_core::CapabilityApprovalSchema::v1(vec![
        voidb_core::CapabilityApprovalField::new(
            "/database",
            "Database",
            voidb_core::CapabilityApprovalValueType::ResourceId,
        )
        .required(),
        collection,
    ])
}

fn diagnostics_result(config: &MongoConfig, invocation_id: String) -> CapabilityInvocationResult {
    let output = json!({
        "uri_scheme": uri_scheme(&config.uri),
        "default_db_present": config.default_db.is_some(),
        "auth_type": mongo_auth_type(config),
        "timeout_secs": config.timeout,
        "tls_enabled": config.tls.enabled,
        "network_checked": false,
    });
    result(invocation_id, output.clone(), output, None)
}

async fn invoke_databases(
    config: &MongoConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let page = page_request(invocation.controls.page.as_ref())?;
    let service = service(config).await?;
    let mut databases = service
        .direct_list_databases()
        .await
        .map_err(|error| target_error(config, "mongodb.databases_failed", error.to_string()))?;
    databases.sort_by(|left, right| left.name.cmp(&right.name));
    let items = databases.iter().map(database_json).collect::<Vec<_>>();
    Ok(paged_result(
        invocation.id,
        "databases",
        items,
        page,
        Value::Null,
    ))
}

async fn invoke_collections(
    config: &MongoConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let database = required_string(&invocation.input, "database")?;
    let page = page_request(invocation.controls.page.as_ref())?;
    let service = service(config).await?;
    let mut collections = service
        .direct_list_collections(&database)
        .await
        .map_err(|error| target_error(config, "mongodb.collections_failed", error.to_string()))?;
    collections.sort_by(|left, right| left.name.cmp(&right.name));
    let items = collections.iter().map(collection_json).collect::<Vec<_>>();
    Ok(paged_result(
        invocation.id,
        "collections",
        items,
        page,
        json!({ "database": database }),
    ))
}

async fn invoke_find(
    config: &MongoConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let database = required_string(&invocation.input, "database")?;
    let collection = required_string(&invocation.input, "collection")?;
    let filter = optional_document(&invocation.input, "filter")?.unwrap_or_default();
    let sort = optional_document(&invocation.input, "sort")?;
    let page = page_request(invocation.controls.page.as_ref())?;
    let service = service(config).await?;
    let result = service
        .direct_find(
            &database,
            &collection,
            filter.clone(),
            Some((page.limit + 1) as i64),
            Some(page.offset as u64),
            sort,
        )
        .await
        .map_err(|error| target_error(config, "mongodb.find_failed", error.to_string()))?;

    let documents = result
        .documents
        .iter()
        .take(page.limit)
        .map(document_output_json)
        .collect::<Vec<_>>();
    let next_offset = page.offset.saturating_add(documents.len());
    let next_cursor = (next_offset < result.total as usize).then(|| next_offset.to_string());
    Ok(document_result(
        invocation.id,
        database,
        collection,
        documents,
        page,
        next_cursor,
        json!({
            "total": result.total,
            "columns": result.columns,
            "filter_summary": document_summary(&filter),
        }),
    ))
}

async fn invoke_count(
    config: &MongoConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let database = required_string(&invocation.input, "database")?;
    let collection = required_string(&invocation.input, "collection")?;
    let filter = optional_document(&invocation.input, "filter")?.unwrap_or_default();
    let service = service(config).await?;
    let count = service
        .direct_count(&database, &collection, filter.clone())
        .await
        .map_err(|error| target_error(config, "mongodb.count_failed", error.to_string()))?;
    let output = json!({
        "database": database,
        "collection": collection,
        "count": count,
        "filter_summary": document_summary(&filter),
    });
    let summary = output.clone();
    Ok(result(invocation.id, output, summary, None))
}

async fn invoke_aggregate(
    config: &MongoConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let database = required_string(&invocation.input, "database")?;
    let collection = required_string(&invocation.input, "collection")?;
    let pipeline = required_pipeline(&invocation.input, "pipeline")?;
    reject_mutating_pipeline(&pipeline)?;
    let page = page_request(invocation.controls.page.as_ref())?;
    let bounded_pipeline = bounded_aggregate_pipeline(pipeline.clone(), &page);
    let service = service(config).await?;
    let result = service
        .direct_aggregate(&database, &collection, bounded_pipeline)
        .await
        .map_err(|error| target_error(config, "mongodb.aggregate_failed", error.to_string()))?;

    let fetched = result.documents.len();
    let documents = result
        .documents
        .iter()
        .take(page.limit)
        .map(document_output_json)
        .collect::<Vec<_>>();
    let next_cursor =
        (fetched > page.limit).then(|| page.offset.saturating_add(documents.len()).to_string());
    Ok(document_result(
        invocation.id,
        database,
        collection,
        documents,
        page,
        next_cursor,
        json!({
            "pipeline_stage_count": pipeline.len(),
            "pipeline_summary": audit_json_summary(&pipeline_json(&pipeline)),
        }),
    ))
}

async fn invoke_indexes(
    config: &MongoConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let database = required_string(&invocation.input, "database")?;
    let collection = required_string(&invocation.input, "collection")?;
    let page = page_request(invocation.controls.page.as_ref())?;
    let service = service(config).await?;
    let mut indexes = service
        .direct_list_indexes(&database, &collection)
        .await
        .map_err(|error| target_error(config, "mongodb.indexes_failed", error.to_string()))?;
    indexes.sort_by(|left, right| left.name.cmp(&right.name));
    let items = indexes.iter().map(index_json).collect::<Vec<_>>();
    Ok(paged_result(
        invocation.id,
        "indexes",
        items,
        page,
        json!({ "database": database, "collection": collection }),
    ))
}

async fn invoke_insert(
    config: &MongoConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let database = required_string(&invocation.input, "database")?;
    let collection = required_string(&invocation.input, "collection")?;
    let document_value = required_value(&invocation.input, "document")?.clone();
    let document = json_document(&document_value, "document")?;
    let details = json!({
        "database": database,
        "collection": collection,
        "document_summary": audit_json_summary(&document_value),
    });

    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "insert", details));
    }

    let service = service(config).await?;
    let result = service
        .direct_insert(&database, &collection, document)
        .await
        .map_err(|error| target_error(config, "mongodb.insert_failed", error.to_string()))?;
    let mut details = details;
    details["inserted_id"] = json!(result.inserted_id);
    Ok(mutation_result(invocation.id, "insert", details))
}

async fn invoke_update(
    config: &MongoConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let database = required_string(&invocation.input, "database")?;
    let collection = required_string(&invocation.input, "collection")?;
    let filter_value = required_value(&invocation.input, "filter")?.clone();
    let update_value = required_value(&invocation.input, "update")?.clone();
    let filter = json_document(&filter_value, "filter")?;
    let update = json_document(&update_value, "update")?;
    let details = json!({
        "database": database,
        "collection": collection,
        "filter_summary": audit_json_summary(&filter_value),
        "update_summary": audit_json_summary(&update_value),
    });

    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "update", details));
    }

    let service = service(config).await?;
    let result = service
        .direct_update(&database, &collection, filter, update)
        .await
        .map_err(|error| target_error(config, "mongodb.update_failed", error.to_string()))?;
    let mut details = details;
    details["modified_count"] = json!(result.modified_count);
    Ok(mutation_result(invocation.id, "update", details))
}

async fn invoke_delete(
    config: &MongoConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let database = required_string(&invocation.input, "database")?;
    let collection = required_string(&invocation.input, "collection")?;
    let filter_value = required_value(&invocation.input, "filter")?.clone();
    let filter = json_document(&filter_value, "filter")?;
    let details = json!({
        "database": database,
        "collection": collection,
        "filter_summary": audit_json_summary(&filter_value),
    });

    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "delete", details));
    }

    let service = service(config).await?;
    let result = service
        .direct_delete(&database, &collection, filter)
        .await
        .map_err(|error| target_error(config, "mongodb.delete_failed", error.to_string()))?;
    let mut details = details;
    details["deleted_count"] = json!(result.deleted_count);
    Ok(mutation_result(invocation.id, "delete", details))
}

async fn invoke_bulk_write(
    config: &MongoConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let database = required_string(&invocation.input, "database")?;
    let collection = required_string(&invocation.input, "collection")?;
    let ordered = optional_bool(&invocation.input, "ordered")?.unwrap_or(true);
    let operations = bulk_operations(&invocation.input)?;
    let operation_types = operations
        .iter()
        .map(MongoBulkOperation::name)
        .collect::<Vec<_>>();
    let preview = json!({
        "database": database,
        "collection": collection,
        "ordered": ordered,
        "operation_count": operations.len(),
        "operation_types": operation_types,
        "payloads_omitted": true
    });
    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "bulk_write", preview));
    }
    let bulk = execute_bulk(config, &database, &collection, operations, ordered)
        .await
        .map_err(|error| target_error(config, "mongodb.bulk_write_failed", error))?;
    let items = bulk
        .items
        .iter()
        .map(|item| {
            json!({
                "index": item.index,
                "operation": item.operation,
                "ok": item.ok,
                "affected": item.affected,
                "inserted_id": item.inserted_id,
                "error_code": item.error_code,
                "retryable": item.retryable
            })
        })
        .collect::<Vec<_>>();
    let details = json!({
        "database": database,
        "collection": collection,
        "ordered": ordered,
        "successful": bulk.successful,
        "failed": bulk.failed,
        "stopped_early": bulk.stopped_early,
        "items": items
    });
    let output = json!({
        "ok": bulk.failed == 0,
        "operation": "bulk_write",
        "dry_run": false,
        "destructive": true,
        "details": details
    });
    Ok(result(
        invocation.id,
        output,
        json!({
            "operation": "bulk_write",
            "successful": bulk.successful,
            "failed": bulk.failed,
            "stopped_early": bulk.stopped_early
        }),
        None,
    ))
}

async fn invoke_create_index(
    config: &MongoConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let database = required_string(&invocation.input, "database")?;
    let collection = required_string(&invocation.input, "collection")?;
    let keys_value = required_value(&invocation.input, "keys")?.clone();
    let keys = json_document(&keys_value, "keys")?;
    let unique = optional_bool(&invocation.input, "unique")?.unwrap_or(false);
    let details = json!({
        "database": database,
        "collection": collection,
        "keys": keys_value,
        "unique": unique,
    });

    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "create_index", details));
    }

    let service = service(config).await?;
    let result = service
        .direct_create_index(&database, &collection, keys, unique)
        .await
        .map_err(|error| target_error(config, "mongodb.create_index_failed", error.to_string()))?;
    let mut details = details;
    details["index_name"] = json!(result.index_name);
    Ok(mutation_result(invocation.id, "create_index", details))
}

async fn invoke_run_command(
    config: &MongoConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let database = required_string(&invocation.input, "database")?;
    let command_value = required_value(&invocation.input, "command")?.clone();
    let command = json_document(&command_value, "command")?;
    let details = json!({
        "database": database,
        "command_summary": audit_json_summary(&command_value),
    });

    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "run_command", details));
    }

    let service = service(config).await?;
    let result_document = service
        .direct_run_command(&database, command)
        .await
        .map_err(|error| target_error(config, "mongodb.run_command_failed", error.to_string()))?;
    let result_value = document_output_json(&result_document);
    let output = json!({
        "ok": true,
        "operation": "run_command",
        "dry_run": false,
        "destructive": true,
        "details": details,
        "result": result_value,
        "result_summary": audit_json_summary(&result_value),
    });
    let summary = json!({
        "operation": "run_command",
        "dry_run": false,
        "result_summary": output["result_summary"],
    });
    Ok(result(invocation.id, output, summary, None))
}

async fn service(config: &MongoConfig) -> Result<MongoService, CapabilityError> {
    MongoService::new_direct(config)
        .await
        .map_err(|error| target_error(config, "mongodb.connect_failed", error.to_string()))
}

fn paged_result(
    invocation_id: String,
    item_key: &str,
    items: Vec<Value>,
    page: PageRequest,
    metadata: Value,
) -> CapabilityInvocationResult {
    let source_count = items.len();
    let end = page.offset.saturating_add(page.limit).min(source_count);
    let page_items = if page.offset >= source_count {
        Vec::new()
    } else {
        items[page.offset..end].to_vec()
    };
    let next_cursor = (end < source_count).then(|| end.to_string());
    let item_count = page_items.len();
    let truncated = next_cursor.is_some();
    let output = json!({
        item_key: page_items,
        "item_count": item_count,
        "source_item_count": source_count,
        "limit": page.limit,
        "cursor": page.cursor,
        "next_cursor": next_cursor,
        "truncated": truncated,
        "metadata": metadata,
    });
    let summary = json!({
        "item_key": item_key,
        "item_count": item_count,
        "source_item_count": source_count,
        "truncated": truncated,
        "next_cursor": output["next_cursor"],
    });
    let output_page = truncated.then(|| InvocationOutputPage {
        next_cursor: output["next_cursor"].as_str().map(str::to_string),
    });
    result(invocation_id, output, summary, output_page)
}

fn document_result(
    invocation_id: String,
    database: String,
    collection: String,
    documents: Vec<Value>,
    page: PageRequest,
    next_cursor: Option<String>,
    metadata: Value,
) -> CapabilityInvocationResult {
    let item_count = documents.len();
    let truncated = next_cursor.is_some();
    let document_summaries = documents.iter().map(audit_json_summary).collect::<Vec<_>>();
    let output = json!({
        "database": database,
        "collection": collection,
        "documents": documents,
        "document_summaries": document_summaries,
        "item_count": item_count,
        "limit": page.limit,
        "cursor": page.cursor,
        "next_cursor": next_cursor,
        "truncated": truncated,
        "metadata": metadata,
    });
    let summary = json!({
        "database": output["database"],
        "collection": output["collection"],
        "item_count": item_count,
        "truncated": truncated,
        "next_cursor": output["next_cursor"],
        "document_summaries": output["document_summaries"],
        "metadata": output["metadata"],
    });
    let output_page = truncated.then(|| InvocationOutputPage {
        next_cursor: output["next_cursor"].as_str().map(str::to_string),
    });
    result(invocation_id, output, summary, output_page)
}

fn dry_run_result(
    invocation_id: String,
    operation: &str,
    details: Value,
) -> CapabilityInvocationResult {
    let output = json!({
        "ok": true,
        "operation": operation,
        "dry_run": true,
        "would_execute": true,
        "destructive": true,
        "details": details,
    });
    result(
        invocation_id,
        output,
        json!({ "operation": operation, "dry_run": true }),
        None,
    )
}

fn mutation_result(
    invocation_id: String,
    operation: &str,
    details: Value,
) -> CapabilityInvocationResult {
    let output = json!({
        "ok": true,
        "operation": operation,
        "dry_run": false,
        "destructive": true,
        "details": details,
    });
    result(
        invocation_id,
        output,
        json!({ "operation": operation, "dry_run": false }),
        None,
    )
}

fn result(
    invocation_id: String,
    output: Value,
    output_summary: Value,
    page: Option<InvocationOutputPage>,
) -> CapabilityInvocationResult {
    CapabilityInvocationResult {
        invocation_id,
        status: InvocationStatus::Succeeded,
        output,
        output_summary,
        page,
    }
}

fn database_json(database: &DbInfo) -> Value {
    json!({
        "name": database.name,
        "size_on_disk": database.size_on_disk,
        "empty": database.empty,
    })
}

fn collection_json(collection: &CollectionInfo) -> Value {
    json!({
        "name": collection.name,
        "doc_count": collection.doc_count,
        "size": collection.size,
        "index_count": collection.index_count,
    })
}

fn index_json(index: &IndexInfo) -> Value {
    let keys = document_output_json(&index.keys);
    json!({
        "name": index.name,
        "keys": keys,
        "unique": index.unique,
        "sparse": index.sparse,
    })
}

fn document_output_json(document: &Document) -> Value {
    let bson = bson::to_bson(document).unwrap_or(Bson::Null);
    serde_json::to_value(bson).unwrap_or(Value::Null)
}

fn document_summary(document: &Document) -> Value {
    audit_json_summary(&document_output_json(document))
}

fn pipeline_json(pipeline: &[Document]) -> Value {
    Value::Array(pipeline.iter().map(document_output_json).collect())
}

fn empty_input_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false
    })
}

fn list_schema(item_key: &str, item_schema: Value) -> Value {
    json!({
        "type": "object",
        "required": [
            item_key,
            "item_count",
            "source_item_count",
            "limit",
            "cursor",
            "next_cursor",
            "truncated",
            "metadata"
        ],
        "properties": {
            item_key: { "type": "array", "items": item_schema },
            "item_count": { "type": "integer", "minimum": 0 },
            "source_item_count": { "type": "integer", "minimum": 0 },
            "limit": { "type": "integer", "minimum": 1, "maximum": MAX_PAGE_LIMIT },
            "cursor": { "type": ["string", "null"] },
            "next_cursor": { "type": ["string", "null"] },
            "truncated": { "type": "boolean" },
            "metadata": { "type": ["object", "null"] }
        },
        "additionalProperties": false
    })
}

fn database_schema() -> Value {
    json!({
        "type": "object",
        "required": ["name", "size_on_disk", "empty"],
        "properties": {
            "name": { "type": "string" },
            "size_on_disk": { "type": "integer", "minimum": 0 },
            "empty": { "type": "boolean" }
        },
        "additionalProperties": false
    })
}

fn collection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["name", "doc_count", "size", "index_count"],
        "properties": {
            "name": { "type": "string" },
            "doc_count": { "type": "integer", "minimum": 0 },
            "size": { "type": "integer", "minimum": 0 },
            "index_count": { "type": "integer", "minimum": 0 }
        },
        "additionalProperties": false
    })
}

fn index_schema() -> Value {
    json!({
        "type": "object",
        "required": ["name", "keys", "unique", "sparse"],
        "properties": {
            "name": { "type": "string" },
            "keys": { "type": "object" },
            "unique": { "type": "boolean" },
            "sparse": { "type": "boolean" }
        },
        "additionalProperties": false
    })
}

fn documents_output_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "database",
            "collection",
            "documents",
            "document_summaries",
            "item_count",
            "limit",
            "cursor",
            "next_cursor",
            "truncated",
            "metadata"
        ],
        "properties": {
            "database": { "type": "string" },
            "collection": { "type": "string" },
            "documents": { "type": "array", "items": { "type": "object" } },
            "document_summaries": { "type": "array", "items": { "type": "object" } },
            "item_count": { "type": "integer", "minimum": 0 },
            "limit": { "type": "integer", "minimum": 1, "maximum": MAX_PAGE_LIMIT },
            "cursor": { "type": ["string", "null"] },
            "next_cursor": { "type": ["string", "null"] },
            "truncated": { "type": "boolean" },
            "metadata": { "type": "object" }
        },
        "additionalProperties": false
    })
}

fn mutation_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["ok", "operation", "dry_run", "destructive", "details"],
        "properties": {
            "ok": { "type": "boolean" },
            "operation": { "type": "string" },
            "dry_run": { "type": "boolean" },
            "would_execute": { "type": "boolean" },
            "destructive": { "type": "boolean" },
            "details": { "type": "object" }
        },
        "additionalProperties": false
    })
}

fn required_value<'a>(input: &'a Value, field: &str) -> Result<&'a Value, CapabilityError> {
    input.get(field).ok_or_else(|| {
        validation_error(
            "validation.input_field_required",
            "Required input field is missing.",
            json!({ "field": field }),
        )
    })
}

fn required_string(input: &Value, field: &str) -> Result<String, CapabilityError> {
    optional_string(input, field)?.ok_or_else(|| {
        validation_error(
            "validation.input_field_required",
            "Required string input field is missing.",
            json!({ "field": field }),
        )
    })
}

fn optional_string(input: &Value, field: &str) -> Result<Option<String>, CapabilityError> {
    match input.get(field) {
        Some(value) if !value.is_string() => Err(validation_error(
            "validation.input_field_invalid",
            "Input field must be a string.",
            json!({ "field": field }),
        )),
        Some(value) => value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| {
                validation_error(
                    "validation.input_field_required",
                    "String input field cannot be empty.",
                    json!({ "field": field }),
                )
            }),
        None => Ok(None),
    }
}

fn optional_bool(input: &Value, field: &str) -> Result<Option<bool>, CapabilityError> {
    match input.get(field) {
        Some(value) if !value.is_boolean() => Err(validation_error(
            "validation.input_field_invalid",
            "Input field must be a boolean.",
            json!({ "field": field }),
        )),
        Some(value) => Ok(value.as_bool()),
        None => Ok(None),
    }
}

fn optional_document(input: &Value, field: &str) -> Result<Option<Document>, CapabilityError> {
    input
        .get(field)
        .map(|value| json_document(value, field))
        .transpose()
}

fn json_document(value: &Value, field: &str) -> Result<Document, CapabilityError> {
    if !value.is_object() {
        return Err(validation_error(
            "validation.input_field_invalid",
            "Input field must be a JSON object.",
            json!({ "field": field }),
        ));
    }
    bson::to_document(value).map_err(|error| {
        validation_error(
            "validation.mongodb_document_invalid",
            "JSON object could not be converted to a BSON document.",
            json!({ "field": field, "message": error.to_string() }),
        )
    })
}

fn bulk_operations(input: &Value) -> Result<Vec<MongoBulkOperation>, CapabilityError> {
    let values = required_value(input, "operations").and_then(|value| {
        value.as_array().ok_or_else(|| {
            validation_error(
                "validation.input_field_invalid",
                "MongoDB bulk operations must be an array.",
                json!({ "field": "operations" }),
            )
        })
    })?;
    if values.is_empty() || values.len() > 100 {
        return Err(validation_error(
            "validation.bulk_operation_count_invalid",
            "MongoDB bulk writes require 1 to 100 operations.",
            json!({ "minimum": 1, "maximum": 100, "actual": values.len() }),
        ));
    }
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            let operation = value.as_object().ok_or_else(|| {
                validation_error(
                    "validation.bulk_operation_invalid",
                    "MongoDB bulk operation must be an object.",
                    json!({ "index": index }),
                )
            })?;
            let operation_type =
                operation
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        validation_error(
                            "validation.bulk_operation_invalid",
                            "MongoDB bulk operation type is required.",
                            json!({ "index": index }),
                        )
                    })?;
            match operation_type {
                "insert" => Ok(MongoBulkOperation::Insert {
                    document: operation
                        .get("document")
                        .ok_or_else(|| {
                            validation_error(
                                "validation.bulk_operation_invalid",
                                "MongoDB bulk insert requires document.",
                                json!({ "index": index }),
                            )
                        })
                        .and_then(|value| json_document(value, "document"))?,
                }),
                "update" => Ok(MongoBulkOperation::Update {
                    filter: operation
                        .get("filter")
                        .ok_or_else(|| {
                            validation_error(
                                "validation.bulk_operation_invalid",
                                "MongoDB bulk update requires filter.",
                                json!({ "index": index }),
                            )
                        })
                        .and_then(|value| json_document(value, "filter"))?,
                    update: operation
                        .get("update")
                        .ok_or_else(|| {
                            validation_error(
                                "validation.bulk_operation_invalid",
                                "MongoDB bulk update requires update.",
                                json!({ "index": index }),
                            )
                        })
                        .and_then(|value| json_document(value, "update"))?,
                }),
                "delete" => Ok(MongoBulkOperation::Delete {
                    filter: operation
                        .get("filter")
                        .ok_or_else(|| {
                            validation_error(
                                "validation.bulk_operation_invalid",
                                "MongoDB bulk delete requires filter.",
                                json!({ "index": index }),
                            )
                        })
                        .and_then(|value| json_document(value, "filter"))?,
                }),
                _ => Err(validation_error(
                    "validation.bulk_operation_invalid",
                    "MongoDB bulk operation type is unsupported.",
                    json!({ "index": index, "type": operation_type }),
                )),
            }
        })
        .collect()
}

fn required_pipeline(input: &Value, field: &str) -> Result<Vec<Document>, CapabilityError> {
    let value = required_value(input, field)?;
    let Some(stages) = value.as_array() else {
        return Err(validation_error(
            "validation.input_field_invalid",
            "Aggregation pipeline must be a JSON array.",
            json!({ "field": field }),
        ));
    };
    stages
        .iter()
        .enumerate()
        .map(|(index, stage)| {
            json_document(stage, field).map_err(|mut error| {
                error.details["stage_index"] = json!(index);
                error
            })
        })
        .collect()
}

fn reject_mutating_pipeline(pipeline: &[Document]) -> Result<(), CapabilityError> {
    for (index, stage) in pipeline.iter().enumerate() {
        if stage.contains_key("$out") || stage.contains_key("$merge") {
            return Err(validation_error(
                "validation.mongodb_aggregate_mutating_stage",
                "Read-only aggregate rejects $out and $merge stages; use a destructive raw command when intentional.",
                json!({ "stage_index": index }),
            ));
        }
    }
    Ok(())
}

fn bounded_aggregate_pipeline(mut pipeline: Vec<Document>, page: &PageRequest) -> Vec<Document> {
    if page.offset > 0 {
        pipeline.push(bson::doc! { "$skip": page.offset as i64 });
    }
    pipeline.push(bson::doc! { "$limit": (page.limit + 1) as i64 });
    pipeline
}

#[derive(Debug)]
struct PageRequest {
    limit: usize,
    offset: usize,
    cursor: Option<String>,
}

fn page_request(page: Option<&Pagination>) -> Result<PageRequest, CapabilityError> {
    let Some(page) = page else {
        return Ok(PageRequest {
            limit: DEFAULT_PAGE_LIMIT,
            offset: 0,
            cursor: None,
        });
    };
    let limit = (page.limit as usize).clamp(1, MAX_PAGE_LIMIT);
    let offset = match &page.cursor {
        Some(cursor) => cursor.parse::<usize>().map_err(|_| {
            validation_error(
                "validation.cursor_invalid",
                "MongoDB cursor must be a numeric offset.",
                json!({ "cursor": cursor }),
            )
        })?,
        None => 0,
    };
    Ok(PageRequest {
        limit,
        offset,
        cursor: page.cursor.clone(),
    })
}

fn uri_scheme(uri: &str) -> Option<String> {
    uri.split_once("://").map(|(scheme, _)| scheme.to_string())
}

fn mongo_auth_type(config: &MongoConfig) -> Option<&'static str> {
    match config.auth.as_ref() {
        Some(MongoAuth::Password { .. }) => Some("password"),
        Some(MongoAuth::X509 { .. }) => Some("x509"),
        Some(MongoAuth::AwsIam { .. }) => Some("aws_iam"),
        None => None,
    }
}

fn validation_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Validation,
        code,
        message,
        details,
        None,
        false,
    )
}

fn unavailable_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Unavailable,
        code,
        message,
        details,
        None,
        true,
    )
}

fn target_error(config: &MongoConfig, code: &str, message: String) -> CapabilityError {
    let (message, redaction) = redact_mongodb_target_message(message, config);
    CapabilityError {
        category: CapabilityErrorCategory::TargetSystem,
        code: code.to_string(),
        message: "MongoDB target operation failed.".to_string(),
        details: json!({ "message": message.clone() }),
        target: Some(TargetSystemFailure {
            system: Some("mongodb".into()),
            code: Some(code.into()),
            message: Some(message),
        }),
        retryable: false,
        redaction,
    }
}

fn redact_mongodb_target_message(
    message: String,
    config: &MongoConfig,
) -> (String, RedactionStatus) {
    let original = message.clone();
    let mut redacted = redact_mongodb_url_auth(
        redact_mongodb_url_auth(message, "mongodb://"),
        "mongodb+srv://",
    );

    redact_value(&mut redacted, &config.uri);
    if let Some(authority) = mongodb_uri_authority(&config.uri) {
        redact_value(&mut redacted, &authority);
    }
    for userinfo in mongodb_uri_userinfo(&config.uri) {
        redact_value(&mut redacted, &userinfo);
    }
    if let Some(database) = mongodb_uri_database(&config.uri) {
        redact_value(&mut redacted, &database);
    }
    if let Some(default_db) = &config.default_db {
        redact_value(&mut redacted, default_db);
    }

    match &config.auth {
        Some(MongoAuth::Password {
            username,
            password,
            auth_db,
        }) => {
            redact_value(&mut redacted, username);
            redact_value(&mut redacted, password);
            if let Some(auth_db) = auth_db {
                redact_value(&mut redacted, auth_db);
            }
        }
        Some(MongoAuth::X509 {
            cert_path,
            key_path,
        }) => {
            redact_value(&mut redacted, cert_path);
            if let Some(key_path) = key_path {
                redact_value(&mut redacted, key_path);
            }
        }
        Some(MongoAuth::AwsIam {
            access_key,
            secret_key,
            session_token,
        }) => {
            redact_value(&mut redacted, access_key);
            redact_value(&mut redacted, secret_key);
            if let Some(session_token) = session_token {
                redact_value(&mut redacted, session_token);
            }
        }
        None => {}
    }

    let redaction = if redacted != original {
        RedactionStatus::Applied
    } else {
        RedactionStatus::NotRequired
    };
    (redacted, redaction)
}

fn redact_mongodb_url_auth(mut message: String, scheme: &str) -> String {
    let mut search_from = 0usize;
    while let Some(relative_start) = message[search_from..].find(scheme) {
        let scheme_start = search_from + relative_start;
        let auth_start = scheme_start + scheme.len();
        let tail = &message[auth_start..];
        let end = tail
            .find(|ch: char| ch.is_whitespace() || matches!(ch, '"' | '\'' | ')' | '(' | ',' | ';'))
            .map(|relative_end| auth_start + relative_end)
            .unwrap_or(message.len());
        let Some(relative_at) = message[auth_start..end].find('@') else {
            search_from = auth_start;
            continue;
        };
        let auth_end = auth_start + relative_at;
        message.replace_range(auth_start..auth_end, "<redacted>");
        search_from = auth_start + "<redacted>@".len();
    }
    message
}

fn mongodb_uri_authority(uri: &str) -> Option<String> {
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

fn mongodb_uri_userinfo(uri: &str) -> Vec<String> {
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

fn mongodb_uri_database(uri: &str) -> Option<String> {
    let (_, rest) = uri.split_once("://")?;
    let (_, path_and_query) = rest.split_once('/')?;
    path_and_query
        .split('?')
        .next()
        .filter(|database| database.len() >= 4)
        .map(str::to_string)
}

fn redact_value(message: &mut String, sensitive: &str) {
    if sensitive.len() >= 4 && message.contains(sensitive) {
        *message = message.replace(sensitive, "<redacted>");
    }
}

fn capability_error(
    category: CapabilityErrorCategory,
    code: &str,
    message: &str,
    details: Value,
    target: Option<TargetSystemFailure>,
    retryable: bool,
) -> CapabilityError {
    CapabilityError {
        category,
        code: code.to_string(),
        message: message.to_string(),
        details,
        target,
        retryable,
        redaction: RedactionStatus::NotRequired,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use voidb_core::{
        ActorRef, ActorType, ConnectionInstancePurpose, ConnectionProfileRef, InstanceReusePolicy,
        InvocationConnectionTarget, InvocationControls, RedactionStatus,
    };

    use super::*;

    #[test]
    fn catalog_marks_mutations_and_raw_commands_as_destructive_dry_run() {
        let capabilities = mongodb_capabilities();
        for id in [
            "insert",
            "update",
            "delete",
            "bulk_write",
            "create_index",
            "run_command",
        ] {
            let capability = capabilities
                .iter()
                .find(|capability| capability.id == id)
                .expect("capability");
            assert!(capability.destructive);
            assert!(capability.supports_dry_run);
            assert_eq!(
                capability.effective_risk(),
                CapabilityRiskLevel::Destructive
            );
        }

        let find = capabilities
            .iter()
            .find(|capability| capability.id == "find")
            .expect("find capability");
        assert!(!find.destructive);
        assert!(!find.supports_dry_run);
    }

    #[tokio::test]
    async fn insert_dry_run_does_not_open_mongodb_connection() {
        let config = MongoConfig {
            uri: "mongodb://127.0.0.1:1".into(),
            timeout: 1,
            ..Default::default()
        };
        let mut invocation = invocation(
            "insert",
            json!({
                "database": "app",
                "collection": "users",
                "document": { "email": "ada@example.com", "role": "admin" }
            }),
        );
        invocation.controls.dry_run = true;

        let result = invoke_mongodb_capability(&config, invocation)
            .await
            .expect("dry-run");
        let encoded = serde_json::to_string(&result).expect("serialize");

        assert_eq!(result.output["dry_run"], true);
        assert_eq!(
            result.output["details"]["document_summary"]["kind"],
            "object"
        );
        assert!(!encoded.contains("ada@example.com"));
    }

    #[tokio::test]
    async fn bulk_write_dry_run_previews_shape_without_payload_or_connection() {
        let config = MongoConfig {
            uri: "mongodb://127.0.0.1:1".into(),
            timeout: 1,
            ..Default::default()
        };
        let mut invocation = invocation(
            "bulk_write",
            json!({
                "database": "app",
                "collection": "users",
                "ordered": false,
                "operations": [
                    { "type": "insert", "document": { "secret": "do-not-return" } },
                    { "type": "delete", "filter": { "disabled": true } }
                ]
            }),
        );
        invocation.controls.dry_run = true;

        let result = invoke_mongodb_capability(&config, invocation)
            .await
            .expect("dry-run");
        let encoded = serde_json::to_string(&result).expect("serialize");
        assert_eq!(result.output["operation"], "bulk_write");
        assert_eq!(result.output["details"]["operation_count"], 2);
        assert_eq!(result.output["details"]["payloads_omitted"], true);
        assert!(!encoded.contains("do-not-return"));
    }

    #[tokio::test]
    async fn raw_command_dry_run_summarizes_command_without_connection() {
        let config = MongoConfig {
            uri: "mongodb://127.0.0.1:1".into(),
            timeout: 1,
            ..Default::default()
        };
        let mut invocation = invocation(
            "run_command",
            json!({
                "database": "admin",
                "command": { "createUser": "agent", "pwd": "secret" }
            }),
        );
        invocation.controls.dry_run = true;

        let result = invoke_mongodb_capability(&config, invocation)
            .await
            .expect("dry-run");
        let encoded = serde_json::to_string(&result).expect("serialize");

        assert_eq!(result.output["dry_run"], true);
        assert_eq!(result.output["operation"], "run_command");
        assert!(encoded.contains("createUser"));
        assert!(!encoded.contains("secret"));
    }

    #[tokio::test]
    async fn diagnostics_do_not_open_mongodb_connection_or_expose_secrets() {
        let config = MongoConfig {
            uri: "mongodb://user:secret@127.0.0.1:1/app".into(),
            auth: Some(MongoAuth::Password {
                username: "user".into(),
                password: "secret".into(),
                auth_db: Some("admin".into()),
            }),
            timeout: 7,
            ..Default::default()
        };

        let result = invoke_mongodb_capability(&config, invocation("diagnostics", json!({})))
            .await
            .expect("diagnostics");
        let encoded = serde_json::to_string(&result).expect("serialize");

        assert_eq!(result.output["uri_scheme"], "mongodb");
        assert_eq!(result.output["auth_type"], "password");
        assert_eq!(result.output["timeout_secs"], 7);
        assert_eq!(result.output["network_checked"], false);
        assert!(!encoded.contains("secret"));
        assert!(!encoded.contains("127.0.0.1"));
    }

    #[test]
    fn aggregate_rejects_mutating_stages() {
        let pipeline = required_pipeline(
            &json!({ "pipeline": [{ "$match": {} }, { "$merge": "archive" }] }),
            "pipeline",
        )
        .expect("pipeline");

        let error = reject_mutating_pipeline(&pipeline).expect_err("mutating stage");

        assert_eq!(error.code, "validation.mongodb_aggregate_mutating_stage");
    }

    #[test]
    fn target_error_redacts_profile_values() {
        let config = MongoConfig {
            uri: "mongodb://voidb_mongodb_user:mongodb-secret@db.internal.example:27017/voidb_fixture_private?authSource=admin_private".into(),
            default_db: Some("voidb_fixture_private".into()),
            auth: Some(MongoAuth::Password {
                username: "voidb_mongodb_user".into(),
                password: "mongodb-secret".into(),
                auth_db: Some("admin_private".into()),
            }),
            timeout: 1,
            ..Default::default()
        };

        let error = target_error(
            &config,
            "mongodb.test_failed",
            "failed mongodb://voidb_mongodb_user:mongodb-secret@db.internal.example:27017/voidb_fixture_private?authSource=admin_private for voidb_mongodb_user with mongodb-secret".into(),
        );
        let encoded = serde_json::to_string(&error).expect("serialize");

        assert_eq!(error.redaction, RedactionStatus::Applied);
        assert!(encoded.contains("<redacted>"));
        for sample in [
            "mongodb-secret",
            "db.internal.example",
            "voidb_mongodb_user",
            "voidb_fixture_private",
            "admin_private",
        ] {
            assert!(
                !encoded.contains(sample),
                "target error exposed MongoDB profile value {sample}: {encoded}"
            );
        }
    }

    #[tokio::test]
    async fn rejects_wrong_plugin_id() {
        let config = MongoConfig::default();
        let mut invocation = invocation("diagnostics", json!({}));
        invocation.plugin_id = "elasticsearch".into();

        let error = invoke_mongodb_capability(&config, invocation)
            .await
            .expect_err("plugin mismatch");

        assert_eq!(error.category, CapabilityErrorCategory::Validation);
        assert_eq!(error.code, "validation.plugin_mismatch");
    }

    fn invocation(capability_id: &str, input: Value) -> CapabilityInvocation {
        CapabilityInvocation {
            id: format!("invoke-{}", capability_id),
            plugin_id: PLUGIN_ID.into(),
            capability_id: capability_id.into(),
            connection: InvocationConnectionTarget::FromProfile {
                profile: ConnectionProfileRef::Name("mongo".into()),
                purpose: ConnectionInstancePurpose::CapabilityInvocation,
                reuse: InstanceReusePolicy::Allow,
                options: Value::Null,
            },
            input,
            controls: InvocationControls::default(),
            actor: Some(ActorRef {
                id: "test-agent".into(),
                actor_type: ActorType::Agent,
            }),
            requested_at: Utc::now(),
        }
    }
}
