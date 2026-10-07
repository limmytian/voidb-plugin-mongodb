//! MongoDB index operations inner service.

use bson::Document;
use futures::TryStreamExt;
use mongodb::Client;

use super::types::{CommandResult, CreateIndexResult, IndexInfo};

/// Inner service for index-level operations.
pub struct MongoIndexService {
    client: Client,
}

impl MongoIndexService {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// List indexes on a collection.
    pub async fn list(&self, db: &str, collection: &str) -> Result<Vec<IndexInfo>, String> {
        let coll = self
            .client
            .database(db)
            .collection::<Document>(collection);
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
                    name: opts.and_then(|o| o.name.clone()).unwrap_or_default(),
                    keys: m.keys,
                    unique: opts.and_then(|o| o.unique).unwrap_or(false),
                    sparse: opts.and_then(|o| o.sparse).unwrap_or(false),
                }
            })
            .collect())
    }

    /// Create an index on a collection.
    pub async fn create(
        &self,
        db: &str,
        collection: &str,
        keys: Document,
        unique: bool,
    ) -> Result<CreateIndexResult, String> {
        let coll = self
            .client
            .database(db)
            .collection::<Document>(collection);

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

        Ok(CreateIndexResult {
            index_name: result.index_name,
        })
    }

    /// Drop an index by name.
    pub async fn drop(&self, db: &str, collection: &str, name: &str) -> Result<(), String> {
        let coll = self
            .client
            .database(db)
            .collection::<Document>(collection);
        coll.drop_index(name)
            .await
            .map_err(|e| format!("Drop index failed: {}", e))
    }

    /// Run a database command.
    pub async fn run_command(&self, db: &str, command: Document) -> Result<CommandResult, String> {
        let database = self.client.database(db);
        let document = database
            .run_command(command)
            .await
            .map_err(|e| format!("Command failed: {}", e))?;
        Ok(CommandResult { document })
    }
}
