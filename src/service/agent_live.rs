//! Driver-owned MongoDB cursors, change streams, and bounded bulk execution.

use std::time::Duration;

use base64::Engine;
use bson::{Bson, Document, doc};
use mongodb::Cursor;
use mongodb::change_stream::ChangeStream;
use mongodb::change_stream::event::{ChangeStreamEvent, ResumeToken};
use mongodb::options::{AggregateOptions, FindOptions, FullDocumentType};
use serde_json::{Value, json};

use crate::config::MongoConfig;
use crate::mongo_ops;

const MAX_DOCUMENT_BYTES: usize = 64 * 1024;
const MAX_RESUME_TOKEN_BYTES: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MongoCursorKind {
    Find,
    Aggregate,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MongoCursorItem {
    pub document: Value,
    pub document_omitted: bool,
}

pub struct MongoCursorSource {
    cursor: Cursor<Document>,
    exhausted: bool,
}

impl MongoCursorSource {
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        config: &MongoConfig,
        database: &str,
        collection: &str,
        kind: MongoCursorKind,
        filter: Document,
        pipeline: Vec<Document>,
        sort: Option<Document>,
        batch_size: u32,
        max_time_ms: u64,
    ) -> Result<Self, MongoLiveError> {
        let client = mongo_ops::create_client(config)
            .await
            .map_err(|_| MongoLiveError::fatal("MongoDB cursor connection failed"))?;
        let collection = client.database(database).collection::<Document>(collection);
        let cursor = match kind {
            MongoCursorKind::Find => {
                let options = FindOptions::builder()
                    .batch_size(batch_size)
                    .sort(sort)
                    .max_time(Duration::from_millis(max_time_ms))
                    .build();
                collection
                    .find(filter)
                    .with_options(options)
                    .await
                    .map_err(MongoLiveError::from_driver)?
            }
            MongoCursorKind::Aggregate => {
                let options = AggregateOptions::builder()
                    .batch_size(batch_size)
                    .max_time(Duration::from_millis(max_time_ms))
                    .build();
                collection
                    .aggregate(pipeline)
                    .with_options(options)
                    .await
                    .map_err(MongoLiveError::from_driver)?
            }
        };
        Ok(Self {
            cursor,
            exhausted: false,
        })
    }

    pub async fn read(
        &mut self,
        count: usize,
        timeout_ms: u64,
    ) -> Result<Vec<MongoCursorItem>, MongoLiveError> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms.max(1));
        let mut items = Vec::new();
        while items.len() < count && !self.exhausted {
            let advanced = tokio::time::timeout_at(deadline, self.cursor.advance())
                .await
                .map_err(|_| MongoLiveError::retryable("MongoDB cursor read timed out"))?
                .map_err(MongoLiveError::from_driver)?;
            if !advanced {
                self.exhausted = true;
                break;
            }
            let document = self
                .cursor
                .deserialize_current()
                .map_err(MongoLiveError::from_driver)?;
            items.push(bounded_document(document));
        }
        Ok(items)
    }

    pub fn exhausted(&self) -> bool {
        self.exhausted
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MongoChangeEvent {
    pub value: Value,
    pub resume_token: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MongoChangeRead {
    pub events: Vec<MongoChangeEvent>,
    pub latest_resume_token: Option<String>,
    pub exhausted: bool,
}

pub struct MongoChangeStreamSource {
    settings: MongoChangeStreamSettings,
    stream: ChangeStream<ChangeStreamEvent<Document>>,
}

#[derive(Clone)]
struct MongoChangeStreamSettings {
    config: MongoConfig,
    database: String,
    collection: String,
    pipeline: Vec<Document>,
    batch_size: u32,
    max_await_ms: u64,
    full_document: bool,
}

impl MongoChangeStreamSource {
    #[allow(clippy::too_many_arguments)]
    pub async fn open(
        config: &MongoConfig,
        database: String,
        collection: String,
        pipeline: Vec<Document>,
        batch_size: u32,
        max_await_ms: u64,
        full_document: bool,
        resume_token: Option<&str>,
    ) -> Result<Self, MongoLiveError> {
        let settings = MongoChangeStreamSettings {
            config: config.clone(),
            database,
            collection,
            pipeline,
            batch_size,
            max_await_ms,
            full_document,
        };
        let stream = open_change_stream(&settings, resume_token).await?;
        Ok(Self { settings, stream })
    }

    pub async fn reconnect(&mut self, resume_token: Option<&str>) -> Result<(), MongoLiveError> {
        self.stream = open_change_stream(&self.settings, resume_token).await?;
        Ok(())
    }

    pub async fn read(
        &mut self,
        count: usize,
        wait_ms: u64,
    ) -> Result<MongoChangeRead, MongoLiveError> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms.max(1));
        let mut events = Vec::new();
        while events.len() < count && self.stream.is_alive() {
            let next = tokio::time::timeout_at(deadline, self.stream.next_if_any()).await;
            let next = match next {
                Ok(result) => result.map_err(MongoLiveError::from_driver)?,
                Err(_) => break,
            };
            let Some(event) = next else {
                break;
            };
            let resume_token = encode_resume_token(&event.id)?;
            events.push(MongoChangeEvent {
                value: change_event_value(event),
                resume_token,
            });
        }
        let latest_resume_token = self
            .stream
            .resume_token()
            .as_ref()
            .map(encode_resume_token)
            .transpose()?;
        Ok(MongoChangeRead {
            events,
            latest_resume_token,
            exhausted: !self.stream.is_alive(),
        })
    }
}

async fn open_change_stream(
    settings: &MongoChangeStreamSettings,
    resume_token: Option<&str>,
) -> Result<ChangeStream<ChangeStreamEvent<Document>>, MongoLiveError> {
    let client = mongo_ops::create_client(&settings.config)
        .await
        .map_err(|_| MongoLiveError::fatal("MongoDB change-stream connection failed"))?;
    let collection = client
        .database(&settings.database)
        .collection::<Document>(&settings.collection);
    let mut watch = collection
        .watch()
        .pipeline(settings.pipeline.clone())
        .batch_size(settings.batch_size)
        .max_await_time(Duration::from_millis(settings.max_await_ms));
    if settings.full_document {
        watch = watch.full_document(FullDocumentType::UpdateLookup);
    }
    if let Some(resume_token) = resume_token {
        watch = watch.resume_after(decode_resume_token(resume_token)?);
    }
    watch.await.map_err(MongoLiveError::from_driver)
}

#[derive(Debug, Clone)]
pub enum MongoBulkOperation {
    Insert { document: Document },
    Update { filter: Document, update: Document },
    Delete { filter: Document },
}

impl MongoBulkOperation {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Insert { .. } => "insert",
            Self::Update { .. } => "update",
            Self::Delete { .. } => "delete",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MongoBulkItemResult {
    pub index: usize,
    pub operation: &'static str,
    pub ok: bool,
    pub affected: u64,
    pub inserted_id: Option<String>,
    pub error_code: Option<&'static str>,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MongoBulkResult {
    pub items: Vec<MongoBulkItemResult>,
    pub successful: usize,
    pub failed: usize,
    pub stopped_early: bool,
}

pub async fn execute_bulk(
    config: &MongoConfig,
    database: &str,
    collection: &str,
    operations: Vec<MongoBulkOperation>,
    ordered: bool,
) -> Result<MongoBulkResult, String> {
    let client = mongo_ops::create_client(config).await?;
    let collection = client.database(database).collection::<Document>(collection);
    let total = operations.len();
    let mut items = Vec::with_capacity(total);
    for (index, operation) in operations.into_iter().enumerate() {
        let operation_name = operation.name();
        let outcome = match operation {
            MongoBulkOperation::Insert { document } => collection
                .insert_one(document)
                .await
                .map(|result| (1, Some(result.inserted_id.to_string()))),
            MongoBulkOperation::Update { filter, update } => collection
                .update_many(filter, update)
                .await
                .map(|result| (result.modified_count, None)),
            MongoBulkOperation::Delete { filter } => collection
                .delete_many(filter)
                .await
                .map(|result| (result.deleted_count, None)),
        };
        match outcome {
            Ok((affected, inserted_id)) => items.push(MongoBulkItemResult {
                index,
                operation: operation_name,
                ok: true,
                affected,
                inserted_id,
                error_code: None,
                retryable: false,
            }),
            Err(error) => {
                let retryable = error.contains_label("RetryableWriteError")
                    || error.contains_label("TransientTransactionError");
                items.push(MongoBulkItemResult {
                    index,
                    operation: operation_name,
                    ok: false,
                    affected: 0,
                    inserted_id: None,
                    error_code: Some("target_write_failed"),
                    retryable,
                });
                if ordered {
                    break;
                }
            }
        }
    }
    let successful = items.iter().filter(|item| item.ok).count();
    let failed = items.len().saturating_sub(successful);
    Ok(MongoBulkResult {
        stopped_early: ordered && items.len() < total,
        items,
        successful,
        failed,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MongoLiveErrorKind {
    Retryable,
    Fatal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MongoLiveError {
    pub kind: MongoLiveErrorKind,
    pub code: &'static str,
}

impl MongoLiveError {
    fn retryable(code: &'static str) -> Self {
        Self {
            kind: MongoLiveErrorKind::Retryable,
            code,
        }
    }

    fn fatal(code: &'static str) -> Self {
        Self {
            kind: MongoLiveErrorKind::Fatal,
            code,
        }
    }

    fn from_driver(error: mongodb::error::Error) -> Self {
        if error.contains_label("ResumableChangeStreamError")
            || error.contains_label("RetryableReadError")
        {
            Self::retryable("target_read_retryable")
        } else {
            Self::fatal("target_read_failed")
        }
    }
}

fn bounded_document(document: Document) -> MongoCursorItem {
    let value = Bson::Document(document).into_relaxed_extjson();
    if serde_json::to_vec(&value)
        .map(|encoded| encoded.len() <= MAX_DOCUMENT_BYTES)
        .unwrap_or(false)
    {
        MongoCursorItem {
            document: value,
            document_omitted: false,
        }
    } else {
        MongoCursorItem {
            document: Value::Null,
            document_omitted: true,
        }
    }
}

fn change_event_value(event: ChangeStreamEvent<Document>) -> Value {
    let full_document = event.full_document.map(|document| {
        let item = bounded_document(document);
        json!({
            "value": item.document,
            "omitted": item.document_omitted
        })
    });
    let (document_key, document_key_omitted) = event
        .document_key
        .map(|document| Bson::Document(document).into_relaxed_extjson())
        .map(bounded_value)
        .unwrap_or((Value::Null, false));
    let (update_description, update_description_omitted) = event
        .update_description
        .map(|description| {
            let updated_fields = Bson::Document(description.updated_fields).into_relaxed_extjson();
            json!({
                "updated_fields": updated_fields,
                "removed_fields": description.removed_fields,
                "truncated_arrays_present": description.truncated_arrays.is_some()
            })
        })
        .map(bounded_value)
        .unwrap_or((Value::Null, false));
    json!({
        "operation_type": serde_json::to_value(event.operation_type).unwrap_or(Value::String("other".into())),
        "namespace": event.ns.map(|namespace| json!({ "database": namespace.db, "collection": namespace.coll })),
        "document_key": document_key,
        "document_key_omitted": document_key_omitted,
        "full_document": full_document,
        "update_description": update_description,
        "update_description_omitted": update_description_omitted,
        "cluster_time": event.cluster_time.map(|time| format!("{}:{}", time.time, time.increment)),
        "wall_time": event.wall_time.map(|time| time.to_string()),
        "resume_token_omitted": true,
        "session_metadata_omitted": true
    })
}

fn bounded_value(value: Value) -> (Value, bool) {
    if serde_json::to_vec(&value)
        .map(|encoded| encoded.len() <= MAX_DOCUMENT_BYTES)
        .unwrap_or(false)
    {
        (value, false)
    } else {
        (Value::Null, true)
    }
}

fn encode_resume_token(token: &ResumeToken) -> Result<String, MongoLiveError> {
    let token =
        bson::to_bson(token).map_err(|_| MongoLiveError::fatal("resume_token_encode_failed"))?;
    let bytes = bson::to_vec(&doc! { "token": token })
        .map_err(|_| MongoLiveError::fatal("resume_token_encode_failed"))?;
    let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
    if encoded.len() > MAX_RESUME_TOKEN_BYTES {
        return Err(MongoLiveError::fatal("resume_token_too_large"));
    }
    Ok(encoded)
}

fn decode_resume_token(value: &str) -> Result<ResumeToken, MongoLiveError> {
    if value.is_empty() || value.len() > MAX_RESUME_TOKEN_BYTES {
        return Err(MongoLiveError::fatal("resume_token_invalid"));
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| MongoLiveError::fatal("resume_token_invalid"))?;
    let mut document: Document =
        bson::from_slice(&bytes).map_err(|_| MongoLiveError::fatal("resume_token_invalid"))?;
    let token = document
        .remove("token")
        .ok_or_else(|| MongoLiveError::fatal("resume_token_invalid"))?;
    bson::from_bson(token).map_err(|_| MongoLiveError::fatal("resume_token_invalid"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_documents_are_omitted_instead_of_partially_serialized() {
        let item = bounded_document(doc! { "large": "x".repeat(MAX_DOCUMENT_BYTES) });
        assert!(item.document_omitted);
        assert!(item.document.is_null());
    }

    #[test]
    fn bulk_operation_names_are_machine_stable() {
        let operation = MongoBulkOperation::Delete {
            filter: Document::new(),
        };
        assert_eq!(operation.name(), "delete");
    }

    #[test]
    fn oversized_change_event_components_are_omitted() {
        let (value, omitted) = bounded_value(json!({ "large": "x".repeat(MAX_DOCUMENT_BYTES) }));
        assert!(omitted);
        assert!(value.is_null());
    }

    #[test]
    fn malformed_resume_tokens_fail_closed_without_driver_state() {
        let oversized = "x".repeat(MAX_RESUME_TOKEN_BYTES + 1);
        for malformed in ["", "not-base64", oversized.as_str()] {
            let error = decode_resume_token(malformed).expect_err("invalid resume token");
            assert_eq!(error.kind, MongoLiveErrorKind::Fatal);
            assert_eq!(error.code, "resume_token_invalid");
        }
    }
}
