//! MongoDB operations wrapping the official mongodb crate.

use bson::Document;
use futures::TryStreamExt;
use mongodb::options::{ClientOptions, FindOptions};
use mongodb::{Client, Collection, Database};
use std::time::Duration;

use crate::config::{MongoAuth, MongoConfig};
use crate::types::{CollectionInfo, DbInfo, IndexInfo};

/// Create a MongoDB client from config.
pub async fn create_client(config: &MongoConfig) -> Result<Client, String> {
    let mut uri = config.uri.clone();

    // Inject auth credentials into URI if not already embedded
    if let Some(MongoAuth::Password {
        username,
        password,
        auth_db,
    }) = &config.auth
        && !uri.contains('@') {
            let scheme_end = uri.find("://").map(|i| i + 3).unwrap_or(0);
            let scheme = &uri[..scheme_end];
            let rest = &uri[scheme_end..];
            uri = format!(
                "{}{}:{}@{}",
                scheme,
                urlencoded(username),
                urlencoded(password),
                rest
            );
            if let Some(db) = auth_db
                && !uri.contains("authSource=") {
                    let sep = if uri.contains('?') { '&' } else { '?' };
                    uri = format!("{}{}authSource={}", uri, sep, db);
                }
        }

    let mut opts = ClientOptions::parse(&uri)
        .await
        .map_err(|e| format!("Failed to parse URI: {}", e))?;

    opts.connect_timeout = Some(Duration::from_secs(config.timeout));
    opts.server_selection_timeout = Some(Duration::from_secs(config.timeout));

    if config.tls.enabled {
        use mongodb::options::TlsOptions;
        let mut tls = TlsOptions::default();
        if let Some(ca) = &config.tls.ca_file {
            tls.ca_file_path = Some(ca.into());
        }
        tls.allow_invalid_certificates = Some(config.tls.allow_invalid_certs);
        opts.tls = Some(mongodb::options::Tls::Enabled(tls));
    }

    Client::with_options(opts).map_err(|e| format!("Failed to create client: {}", e))
}

/// List all databases.
pub async fn list_databases(client: &Client) -> Result<Vec<DbInfo>, String> {
    let dbs = client
        .list_databases()
        .await
        .map_err(|e| format!("Failed to list databases: {}", e))?;

    Ok(dbs
        .into_iter()
        .map(|db| DbInfo {
            name: db.name,
            size_on_disk: db.size_on_disk,
            empty: db.size_on_disk == 0,
        })
        .collect())
}

/// List collections in a database.
pub async fn list_collections(db: &Database) -> Result<Vec<CollectionInfo>, String> {
    let names = db
        .list_collection_names()
        .await
        .map_err(|e| format!("Failed to list collections: {}", e))?;

    let mut infos = Vec::new();
    for name in names {
        let coll: Collection<Document> = db.collection(&name);
        let doc_count = coll
            .estimated_document_count()
            .await
            .unwrap_or(0);
        infos.push(CollectionInfo {
            name,
            doc_count,
            size: 0,
            index_count: 0,
        });
    }
    Ok(infos)
}

/// Find documents with optional filter, sort, limit.
pub async fn find_documents(
    coll: &Collection<Document>,
    filter: Document,
    limit: Option<i64>,
    skip: Option<u64>,
    sort: Option<Document>,
) -> Result<Vec<Document>, String> {
    let mut opts = FindOptions::default();
    opts.limit = limit;
    opts.skip = skip;
    opts.sort = sort;

    let cursor = coll
        .find(filter)
        .with_options(opts)
        .await
        .map_err(|e| format!("Find failed: {}", e))?;

    cursor
        .try_collect()
        .await
        .map_err(|e| format!("Cursor error: {}", e))
}

/// Count documents matching a filter.
pub async fn count_documents(
    coll: &Collection<Document>,
    filter: Document,
) -> Result<u64, String> {
    coll.count_documents(filter)
        .await
        .map_err(|e| format!("Count failed: {}", e))
}

/// Insert a single document, returning the inserted ID as string.
pub async fn insert_document(
    coll: &Collection<Document>,
    doc: Document,
) -> Result<String, String> {
    let result = coll
        .insert_one(doc)
        .await
        .map_err(|e| format!("Insert failed: {}", e))?;

    Ok(format!("{}", result.inserted_id))
}

/// Update documents matching a filter.
pub async fn update_documents(
    coll: &Collection<Document>,
    filter: Document,
    update: Document,
) -> Result<u64, String> {
    let result = coll
        .update_many(filter, update)
        .await
        .map_err(|e| format!("Update failed: {}", e))?;

    Ok(result.modified_count)
}

/// Delete documents matching a filter.
pub async fn delete_documents(
    coll: &Collection<Document>,
    filter: Document,
) -> Result<u64, String> {
    let result = coll
        .delete_many(filter)
        .await
        .map_err(|e| format!("Delete failed: {}", e))?;

    Ok(result.deleted_count)
}

/// Run an aggregation pipeline.
pub async fn aggregate(
    coll: &Collection<Document>,
    pipeline: Vec<Document>,
) -> Result<Vec<Document>, String> {
    let cursor = coll
        .aggregate(pipeline)
        .await
        .map_err(|e| format!("Aggregate failed: {}", e))?;

    cursor
        .try_collect()
        .await
        .map_err(|e| format!("Cursor error: {}", e))
}

/// List indexes on a collection.
pub async fn list_indexes(coll: &Collection<Document>) -> Result<Vec<IndexInfo>, String> {
    let cursor = coll
        .list_indexes()
        .await
        .map_err(|e| format!("List indexes failed: {}", e))?;

    let models: Vec<_> = cursor
        .try_collect()
        .await
        .map_err(|e| format!("Cursor error: {}", e))?;

    Ok(models
        .into_iter()
        .map(|m| {
            let opts = m.options.as_ref();
            IndexInfo {
                name: opts
                    .and_then(|o| o.name.clone())
                    .unwrap_or_default(),
                keys: m.keys,
                unique: opts.and_then(|o| o.unique).unwrap_or(false),
                sparse: opts.and_then(|o| o.sparse).unwrap_or(false),
            }
        })
        .collect())
}

/// Create an index on a collection.
pub async fn create_index(
    coll: &Collection<Document>,
    keys: Document,
    unique: bool,
) -> Result<String, String> {
    let mut opts = mongodb::options::IndexOptions::default();
    if unique {
        opts.unique = Some(true);
    }

    let model = mongodb::IndexModel::builder()
        .keys(keys)
        .options(opts)
        .build();

    let result = coll
        .create_index(model)
        .await
        .map_err(|e| format!("Create index failed: {}", e))?;

    Ok(result.index_name)
}

/// Drop an index by name.
pub async fn drop_index(coll: &Collection<Document>, name: &str) -> Result<(), String> {
    coll.drop_index(name)
        .await
        .map_err(|e| format!("Drop index failed: {}", e))
}

/// Run a database command.
pub async fn run_command(db: &Database, command: Document) -> Result<Document, String> {
    db.run_command(command)
        .await
        .map_err(|e| format!("Command failed: {}", e))
}

/// Infer column names from a sample of documents.
pub fn infer_columns(docs: &[Document]) -> Vec<String> {
    let mut columns = vec!["_id".to_string()];
    let mut seen = std::collections::HashSet::new();
    seen.insert("_id".to_string());

    for doc in docs {
        for key in doc.keys() {
            if seen.insert(key.to_string()) {
                columns.push(key.to_string());
            }
        }
    }
    columns
}

/// Flatten a document's top-level fields into string values for grid display.
pub fn flatten_document(doc: &Document, columns: &[String]) -> Vec<String> {
    columns
        .iter()
        .map(|col| {
            doc.get(col)
                .map(format_bson_value)
                .unwrap_or_default()
        })
        .collect()
}

/// Format a BSON value for display.
fn format_bson_value(v: &bson::Bson) -> String {
    match v {
        bson::Bson::Null => String::new(),
        bson::Bson::Boolean(b) => b.to_string(),
        bson::Bson::Int32(n) => n.to_string(),
        bson::Bson::Int64(n) => n.to_string(),
        bson::Bson::Double(n) => n.to_string(),
        bson::Bson::String(s) => s.clone(),
        bson::Bson::ObjectId(oid) => oid.to_hex(),
        bson::Bson::DateTime(dt) => {
            dt.try_to_rfc3339_string().unwrap_or_else(|_| dt.to_string())
        }
        bson::Bson::Array(a) => format!("[{} items]", a.len()),
        bson::Bson::Document(_) => "{...}".to_string(),
        bson::Bson::Binary(b) => format!("Binary({} bytes)", b.bytes.len()),
        bson::Bson::RegularExpression(r) => format!("/{}/{}", r.pattern, r.options),
        bson::Bson::Timestamp(ts) => format!("Timestamp({}, {})", ts.time, ts.increment),
        bson::Bson::Decimal128(d) => d.to_string(),
        _ => format!("{}", v),
    }
}

/// URL-encode a string for URI embedding.
fn urlencoded(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => result.push(c),
            _ => {
                for b in c.to_string().as_bytes() {
                    result.push_str(&format!("%{:02X}", b));
                }
            }
        }
    }
    result
}
