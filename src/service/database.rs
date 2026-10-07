//! MongoDB database operations inner service.

use bson::Document;
use mongodb::Client;

use super::types::{CollectionInfo, DbInfo};

/// Inner service for database-level operations.
pub struct MongoDatabaseService {
    client: Client,
}

impl MongoDatabaseService {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// List all databases.
    pub async fn list_databases(&self) -> Result<Vec<DbInfo>, String> {
        let dbs = self
            .client
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
    pub async fn list_collections(&self, db_name: &str) -> Result<Vec<CollectionInfo>, String> {
        let db = self.client.database(db_name);
        let names = db
            .list_collection_names()
            .await
            .map_err(|e| format!("Failed to list collections: {}", e))?;

        let mut infos = Vec::new();
        for name in names {
            let coll: mongodb::Collection<Document> = db.collection(&name);
            let doc_count = coll.estimated_document_count().await.unwrap_or(0);
            infos.push(CollectionInfo {
                name,
                doc_count,
                size: 0,
                index_count: 0,
            });
        }
        Ok(infos)
    }
}
