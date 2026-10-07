//! MongoDB service layer.
//!
//! This module provides `MongoService`, the service facade for the MongoDB plugin.
//! Client creation happens inside the background task to avoid block_on deadlock
//! with the shared runtime.
//!
//! Two modes are supported:
//! - `Channel` (default): background task + mpsc channels, used by the TUI plugin.
//! - `Direct`: async methods called inline, used by the CLI plugin.

pub mod agent_live;
pub mod commands;
pub mod events;
pub mod types;

mod database;
mod document;
mod index;

pub use commands::MongoCommand;
pub use events::MongoEvent;
pub use types::{
    AggregateResult, CollectionInfo, CommandResult, CreateIndexResult, DbInfo, DeleteResult,
    FindResult, IndexInfo, InsertResult, UpdateResult,
};

use std::sync::Arc;

use bson::Document;
use tokio::sync::mpsc;

use crate::config::MongoConfig;
use crate::mongo_ops;
use commands::{DatabaseCommand, DocumentCommand, IndexCommand};
use events::{DatabaseEvent, DocumentEvent, IndexEvent};
use voidb_core::TabManager;

use database::MongoDatabaseService;
use document::MongoDocumentService;
use index::MongoIndexService;

/// Internal mode of operation for `MongoService`.
enum ServiceMode {
    /// TUI mode: channel-based background task.
    Channel {
        cmd_tx: mpsc::UnboundedSender<MongoCommand>,
        event_rx: mpsc::UnboundedReceiver<MongoEvent>,
        _task: tokio::task::JoinHandle<()>,
    },
    /// CLI / direct mode: inline async calls, no background task.
    Direct {
        db_svc: MongoDatabaseService,
        doc_svc: MongoDocumentService,
        idx_svc: MongoIndexService,
    },
}

/// MongoDB service facade.
///
/// Owns the command sender and event receiver channels. The background
/// task runs on the shared tokio runtime via `runtime.spawn()`.
///
/// `MongoService` is `Send` but NOT `Sync` (because `UnboundedReceiver`
/// is `!Sync`). Plugin structs must wrap it in `std::sync::Mutex` to
/// satisfy `Plugin: Send + Sync`.
pub struct MongoService {
    mode: ServiceMode,
}

/// One driver session retained for transactions and causal consistency across
/// serialized agent calls.
pub struct PersistentMongoSession {
    client: mongodb::Client,
    session: mongodb::ClientSession,
    default_database: Option<String>,
}

impl PersistentMongoSession {
    pub async fn open(config: &MongoConfig) -> Result<Self, String> {
        let client = mongo_ops::create_client(config).await?;
        let session = client
            .start_session()
            .await
            .map_err(|_| "MongoDB driver session could not be started".to_string())?;
        Ok(Self {
            client,
            session,
            default_database: config.default_db.clone(),
        })
    }

    pub async fn begin(&mut self) -> Result<(), String> {
        self.session
            .start_transaction()
            .await
            .map_err(|_| "MongoDB transaction could not be started".to_string())
    }

    pub async fn commit(&mut self) -> Result<(), String> {
        self.session
            .commit_transaction()
            .await
            .map_err(|_| "MongoDB transaction commit failed".to_string())
    }

    pub async fn abort(&mut self) -> Result<(), String> {
        self.session
            .abort_transaction()
            .await
            .map_err(|_| "MongoDB transaction abort failed".to_string())
    }

    pub async fn run_command(
        &mut self,
        database: Option<&str>,
        command: Document,
    ) -> Result<Document, String> {
        let database = database
            .or(self.default_database.as_deref())
            .ok_or_else(|| "MongoDB session database is required".to_string())?;
        self.client
            .database(database)
            .run_command(command)
            .session(&mut self.session)
            .await
            .map_err(|_| "MongoDB session command failed".to_string())
    }

    pub async fn close(mut self) {
        let _ = self.session.abort_transaction().await;
    }
}

impl MongoService {
    /// Create a new MongoService with a background processing task (TUI mode).
    ///
    /// The service starts in a disconnected state. Send `MongoCommand::Connect`
    /// to create the client inside the background task (avoids block_on deadlock).
    pub fn new(
        tabs: Arc<dyn TabManager>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<MongoCommand>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<MongoEvent>();

        let task = runtime.spawn(Self::background_task(cmd_rx, event_tx, tabs));

        Self {
            mode: ServiceMode::Channel {
                cmd_tx,
                event_rx,
                _task: task,
            },
        }
    }

    /// Create a new MongoService in direct (CLI) mode.
    ///
    /// Connects immediately using the provided config. No background task is
    /// spawned. Use the `direct_*` methods to perform operations.
    pub async fn new_direct(config: &MongoConfig) -> Result<Self, String> {
        let client = mongo_ops::create_client(config).await?;
        Ok(Self {
            mode: ServiceMode::Direct {
                db_svc: MongoDatabaseService::new(client.clone()),
                doc_svc: MongoDocumentService::new(client.clone()),
                idx_svc: MongoIndexService::new(client),
            },
        })
    }

    /// Send a command to the background service task (Channel mode only).
    pub fn send(&self, cmd: MongoCommand) {
        if let ServiceMode::Channel { cmd_tx, .. } = &self.mode {
            let _ = cmd_tx.send(cmd);
        }
    }

    /// Send a command, returning `Err` if the background task has exited.
    pub fn send_checked(&self, cmd: MongoCommand) -> Result<(), String> {
        match &self.mode {
            ServiceMode::Channel { cmd_tx, .. } => cmd_tx
                .send(cmd)
                .map_err(|_| "Service task has exited".to_string()),
            ServiceMode::Direct { .. } => {
                Err("send_checked() is not supported in Direct mode".to_string())
            }
        }
    }

    /// Poll for the next event from the service (Channel mode only).
    pub fn poll_event(&mut self) -> Option<MongoEvent> {
        if let ServiceMode::Channel { event_rx, .. } = &mut self.mode {
            event_rx.try_recv().ok()
        } else {
            None
        }
    }

    // =========================================================================
    // Direct-mode async methods (CLI use only)
    // =========================================================================

    /// List all databases (Direct mode).
    pub async fn direct_list_databases(&self) -> Result<Vec<DbInfo>, String> {
        match &self.mode {
            ServiceMode::Direct { db_svc, .. } => db_svc.list_databases().await,
            ServiceMode::Channel { .. } => {
                Err("direct_list_databases() requires Direct mode".to_string())
            }
        }
    }

    /// List collections in a database (Direct mode).
    pub async fn direct_list_collections(&self, db: &str) -> Result<Vec<CollectionInfo>, String> {
        match &self.mode {
            ServiceMode::Direct { db_svc, .. } => db_svc.list_collections(db).await,
            ServiceMode::Channel { .. } => {
                Err("direct_list_collections() requires Direct mode".to_string())
            }
        }
    }

    /// Run a raw database command (Direct mode).
    pub async fn direct_run_command(&self, db: &str, command: Document) -> Result<Document, String> {
        match &self.mode {
            ServiceMode::Direct { idx_svc, .. } => {
                idx_svc
                    .run_command(db, command)
                    .await
                    .map(|r| r.document)
            }
            ServiceMode::Channel { .. } => {
                Err("direct_run_command() requires Direct mode".to_string())
            }
        }
    }

    /// Find documents (Direct mode).
    pub async fn direct_find(
        &self,
        db: &str,
        collection: &str,
        filter: Document,
        limit: Option<i64>,
        skip: Option<u64>,
        sort: Option<Document>,
    ) -> Result<FindResult, String> {
        match &self.mode {
            ServiceMode::Direct { doc_svc, .. } => {
                doc_svc.find(db, collection, filter, limit, skip, sort).await
            }
            ServiceMode::Channel { .. } => {
                Err("direct_find() requires Direct mode".to_string())
            }
        }
    }

    /// Count documents (Direct mode).
    pub async fn direct_count(
        &self,
        db: &str,
        collection: &str,
        filter: Document,
    ) -> Result<u64, String> {
        match &self.mode {
            ServiceMode::Direct { doc_svc, .. } => doc_svc.count(db, collection, filter).await,
            ServiceMode::Channel { .. } => {
                Err("direct_count() requires Direct mode".to_string())
            }
        }
    }

    /// Insert a document (Direct mode).
    pub async fn direct_insert(
        &self,
        db: &str,
        collection: &str,
        doc: Document,
    ) -> Result<InsertResult, String> {
        match &self.mode {
            ServiceMode::Direct { doc_svc, .. } => doc_svc.insert(db, collection, doc).await,
            ServiceMode::Channel { .. } => {
                Err("direct_insert() requires Direct mode".to_string())
            }
        }
    }

    /// Update documents (Direct mode).
    pub async fn direct_update(
        &self,
        db: &str,
        collection: &str,
        filter: Document,
        update: Document,
    ) -> Result<UpdateResult, String> {
        match &self.mode {
            ServiceMode::Direct { doc_svc, .. } => {
                doc_svc.update(db, collection, filter, update).await
            }
            ServiceMode::Channel { .. } => {
                Err("direct_update() requires Direct mode".to_string())
            }
        }
    }

    /// Delete documents (Direct mode).
    pub async fn direct_delete(
        &self,
        db: &str,
        collection: &str,
        filter: Document,
    ) -> Result<DeleteResult, String> {
        match &self.mode {
            ServiceMode::Direct { doc_svc, .. } => doc_svc.delete(db, collection, filter).await,
            ServiceMode::Channel { .. } => {
                Err("direct_delete() requires Direct mode".to_string())
            }
        }
    }

    /// Run aggregation pipeline (Direct mode).
    pub async fn direct_aggregate(
        &self,
        db: &str,
        collection: &str,
        pipeline: Vec<Document>,
    ) -> Result<AggregateResult, String> {
        match &self.mode {
            ServiceMode::Direct { doc_svc, .. } => {
                doc_svc.aggregate(db, collection, pipeline).await
            }
            ServiceMode::Channel { .. } => {
                Err("direct_aggregate() requires Direct mode".to_string())
            }
        }
    }

    /// List indexes on a collection (Direct mode).
    pub async fn direct_list_indexes(
        &self,
        db: &str,
        collection: &str,
    ) -> Result<Vec<IndexInfo>, String> {
        match &self.mode {
            ServiceMode::Direct { idx_svc, .. } => idx_svc.list(db, collection).await,
            ServiceMode::Channel { .. } => {
                Err("direct_list_indexes() requires Direct mode".to_string())
            }
        }
    }

    /// Create an index (Direct mode).
    pub async fn direct_create_index(
        &self,
        db: &str,
        collection: &str,
        keys: Document,
        unique: bool,
    ) -> Result<CreateIndexResult, String> {
        match &self.mode {
            ServiceMode::Direct { idx_svc, .. } => {
                idx_svc.create(db, collection, keys, unique).await
            }
            ServiceMode::Channel { .. } => {
                Err("direct_create_index() requires Direct mode".to_string())
            }
        }
    }

    /// Background task that processes commands.
    ///
    /// Client creation happens INSIDE this async task to avoid block_on deadlock
    /// with the shared runtime.
    async fn background_task(
        mut cmd_rx: mpsc::UnboundedReceiver<MongoCommand>,
        event_tx: mpsc::UnboundedSender<MongoEvent>,
        tabs: Arc<dyn TabManager>,
    ) {
        let mut db_svc: Option<MongoDatabaseService> = None;
        let mut doc_svc: Option<MongoDocumentService> = None;
        let mut idx_svc: Option<MongoIndexService> = None;

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                MongoCommand::Connect { config } => {
                    match mongo_ops::create_client(&config).await {
                        Ok(client) => {
                            db_svc = Some(MongoDatabaseService::new(client.clone()));
                            doc_svc = Some(MongoDocumentService::new(client.clone()));
                            idx_svc = Some(MongoIndexService::new(client));
                            let _ = event_tx.send(MongoEvent::Connected);
                        }
                        Err(e) => {
                            let _ = event_tx.send(MongoEvent::Error(e));
                        }
                    }
                }

                MongoCommand::Disconnect => break,

                MongoCommand::Database(sub) => {
                    let Some(ref svc) = db_svc else {
                        let _ = event_tx.send(MongoEvent::Error("Not connected".to_string()));
                        let _ = tabs.request_render();
                        continue;
                    };
                    match sub {
                        DatabaseCommand::ListDatabases => match svc.list_databases().await {
                            Ok(dbs) => {
                                let _ = event_tx.send(MongoEvent::Database(
                                    DatabaseEvent::DatabasesListed(dbs),
                                ));
                            }
                            Err(e) => {
                                let _ = event_tx.send(MongoEvent::Error(e));
                            }
                        },
                        DatabaseCommand::ListCollections { db } => {
                            match svc.list_collections(&db).await {
                                Ok(colls) => {
                                    let _ = event_tx.send(MongoEvent::Database(
                                        DatabaseEvent::CollectionsListed {
                                            db,
                                            collections: colls,
                                        },
                                    ));
                                }
                                Err(e) => {
                                    let _ = event_tx.send(MongoEvent::Error(e));
                                }
                            }
                        }
                    }
                }

                MongoCommand::Document(sub) => {
                    let Some(ref svc) = doc_svc else {
                        let _ = event_tx.send(MongoEvent::Error("Not connected".to_string()));
                        let _ = tabs.request_render();
                        continue;
                    };
                    match sub {
                        DocumentCommand::Find {
                            db,
                            collection,
                            filter,
                            limit,
                            skip,
                            sort,
                        } => match svc.find(&db, &collection, filter, limit, skip, sort).await {
                            Ok(r) => {
                                let _ = event_tx
                                    .send(MongoEvent::Document(DocumentEvent::Found(r)));
                            }
                            Err(e) => {
                                let _ = event_tx.send(MongoEvent::Error(e));
                            }
                        },
                        DocumentCommand::Count {
                            db,
                            collection,
                            filter,
                        } => match svc.count(&db, &collection, filter).await {
                            Ok(c) => {
                                let _ = event_tx
                                    .send(MongoEvent::Document(DocumentEvent::Counted(c)));
                            }
                            Err(e) => {
                                let _ = event_tx.send(MongoEvent::Error(e));
                            }
                        },
                        DocumentCommand::Insert {
                            db,
                            collection,
                            doc,
                        } => match svc.insert(&db, &collection, doc).await {
                            Ok(r) => {
                                let _ = event_tx
                                    .send(MongoEvent::Document(DocumentEvent::Inserted(r)));
                            }
                            Err(e) => {
                                let _ = event_tx.send(MongoEvent::Error(e));
                            }
                        },
                        DocumentCommand::Update {
                            db,
                            collection,
                            filter,
                            update,
                        } => match svc.update(&db, &collection, filter, update).await {
                            Ok(r) => {
                                let _ = event_tx
                                    .send(MongoEvent::Document(DocumentEvent::Updated(r)));
                            }
                            Err(e) => {
                                let _ = event_tx.send(MongoEvent::Error(e));
                            }
                        },
                        DocumentCommand::Delete {
                            db,
                            collection,
                            filter,
                        } => match svc.delete(&db, &collection, filter).await {
                            Ok(r) => {
                                let _ = event_tx
                                    .send(MongoEvent::Document(DocumentEvent::Deleted(r)));
                            }
                            Err(e) => {
                                let _ = event_tx.send(MongoEvent::Error(e));
                            }
                        },
                        DocumentCommand::Aggregate {
                            db,
                            collection,
                            pipeline,
                        } => match svc.aggregate(&db, &collection, pipeline).await {
                            Ok(r) => {
                                let _ = event_tx
                                    .send(MongoEvent::Document(DocumentEvent::Aggregated(r)));
                            }
                            Err(e) => {
                                let _ = event_tx.send(MongoEvent::Error(e));
                            }
                        },
                    }
                }

                MongoCommand::Index(sub) => {
                    let Some(ref svc) = idx_svc else {
                        let _ = event_tx.send(MongoEvent::Error("Not connected".to_string()));
                        let _ = tabs.request_render();
                        continue;
                    };
                    match sub {
                        IndexCommand::List { db, collection } => {
                            match svc.list(&db, &collection).await {
                                Ok(indexes) => {
                                    let _ = event_tx
                                        .send(MongoEvent::Index(IndexEvent::Listed(indexes)));
                                }
                                Err(e) => {
                                    let _ = event_tx.send(MongoEvent::Error(e));
                                }
                            }
                        }
                        IndexCommand::Create {
                            db,
                            collection,
                            keys,
                            unique,
                        } => match svc.create(&db, &collection, keys, unique).await {
                            Ok(r) => {
                                let _ =
                                    event_tx.send(MongoEvent::Index(IndexEvent::Created(r)));
                            }
                            Err(e) => {
                                let _ = event_tx.send(MongoEvent::Error(e));
                            }
                        },
                        IndexCommand::Drop {
                            db,
                            collection,
                            name,
                        } => match svc.drop(&db, &collection, &name).await {
                            Ok(()) => {
                                let _ = event_tx
                                    .send(MongoEvent::Index(IndexEvent::Dropped { name }));
                            }
                            Err(e) => {
                                let _ = event_tx.send(MongoEvent::Error(e));
                            }
                        },
                        IndexCommand::RunCommand { db, command } => {
                            match svc.run_command(&db, command).await {
                                Ok(r) => {
                                    let _ = event_tx
                                        .send(MongoEvent::Index(IndexEvent::CommandResult(r)));
                                }
                                Err(e) => {
                                    let _ = event_tx.send(MongoEvent::Error(e));
                                }
                            }
                        }
                    }
                }
            }

            let _ = tabs.request_render();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>() {}

    #[allow(dead_code)]
    fn assert_sync<T: Sync>() {}

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn service_is_send() {
        assert_send::<MongoService>();
    }

    #[test]
    fn command_is_send() {
        assert_send::<MongoCommand>();
    }

    #[test]
    fn event_is_send() {
        assert_send::<MongoEvent>();
    }

    #[test]
    fn mutex_service_is_send_sync() {
        assert_send_sync::<std::sync::Mutex<MongoService>>();
    }
}
