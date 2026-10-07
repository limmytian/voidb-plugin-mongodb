//! MongoDB document operations inner service.

use bson::Document;
use futures::TryStreamExt;
use mongodb::options::FindOptions;
use mongodb::Client;

use crate::mongo_ops;
use super::types::{AggregateResult, DeleteResult, FindResult, InsertResult, UpdateResult};

/// Inner service for document-level operations.
pub struct MongoDocumentService {
    client: Client,
}

impl MongoDocumentService {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Find documents with optional filter, sort, limit.
    pub async fn find(
        &self,
        db: &str,
        collection: &str,
        filter: Document,
        limit: Option<i64>,
        skip: Option<u64>,
        sort: Option<Document>,
    ) -> Result<FindResult, String> {
        let coll = self
            .client
            .database(db)
            .collection::<Document>(collection);

        // Count total matching documents
        let total = coll
            .count_documents(filter.clone())
            .await
            .map_err(|e| format!("Count failed: {}", e))?;

        // Execute find
        let mut opts = FindOptions::default();
        opts.limit = limit;
        opts.skip = skip;
        opts.sort = sort;

        let cursor = coll
            .find(filter)
            .with_options(opts)
            .await
            .map_err(|e| format!("Find failed: {}", e))?;

        let documents: Vec<Document> = cursor
            .try_collect()
            .await
            .map_err(|e| format!("Cursor error: {}", e))?;

        let columns = mongo_ops::infer_columns(&documents);

        Ok(FindResult {
            documents,
            total,
            columns,
        })
    }

    /// Count documents matching a filter.
    pub async fn count(
        &self,
        db: &str,
        collection: &str,
        filter: Document,
    ) -> Result<u64, String> {
        let coll = self
            .client
            .database(db)
            .collection::<Document>(collection);
        coll.count_documents(filter)
            .await
            .map_err(|e| format!("Count failed: {}", e))
    }

    /// Insert a single document.
    pub async fn insert(
        &self,
        db: &str,
        collection: &str,
        doc: Document,
    ) -> Result<InsertResult, String> {
        let coll = self
            .client
            .database(db)
            .collection::<Document>(collection);
        let result = coll
            .insert_one(doc)
            .await
            .map_err(|e| format!("Insert failed: {}", e))?;
        Ok(InsertResult {
            inserted_id: format!("{}", result.inserted_id),
        })
    }

    /// Update documents matching a filter.
    pub async fn update(
        &self,
        db: &str,
        collection: &str,
        filter: Document,
        update: Document,
    ) -> Result<UpdateResult, String> {
        let coll = self
            .client
            .database(db)
            .collection::<Document>(collection);
        let result = coll
            .update_many(filter, update)
            .await
            .map_err(|e| format!("Update failed: {}", e))?;
        Ok(UpdateResult {
            modified_count: result.modified_count,
        })
    }

    /// Delete documents matching a filter.
    pub async fn delete(
        &self,
        db: &str,
        collection: &str,
        filter: Document,
    ) -> Result<DeleteResult, String> {
        let coll = self
            .client
            .database(db)
            .collection::<Document>(collection);
        let result = coll
            .delete_many(filter)
            .await
            .map_err(|e| format!("Delete failed: {}", e))?;
        Ok(DeleteResult {
            deleted_count: result.deleted_count,
        })
    }

    /// Run an aggregation pipeline.
    pub async fn aggregate(
        &self,
        db: &str,
        collection: &str,
        pipeline: Vec<Document>,
    ) -> Result<AggregateResult, String> {
        let coll = self
            .client
            .database(db)
            .collection::<Document>(collection);
        let cursor = coll
            .aggregate(pipeline)
            .await
            .map_err(|e| format!("Aggregate failed: {}", e))?;
        let documents: Vec<Document> = cursor
            .try_collect()
            .await
            .map_err(|e| format!("Cursor error: {}", e))?;
        Ok(AggregateResult { documents })
    }
}
