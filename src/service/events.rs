//! MongoDB service events.
//!
//! Events sent from the MongoService background task back to the UI layer.

use super::types::*;

/// Top-level event enum for the MongoDB service.
pub enum MongoEvent {
    /// Connection established successfully.
    Connected,
    /// An error occurred.
    Error(String),
    /// Database-level events.
    Database(DatabaseEvent),
    /// Document-level events.
    Document(DocumentEvent),
    /// Index-level events.
    Index(IndexEvent),
}

/// Database sub-events.
pub enum DatabaseEvent {
    DatabasesListed(Vec<DbInfo>),
    CollectionsListed { db: String, collections: Vec<CollectionInfo> },
}

/// Document sub-events.
pub enum DocumentEvent {
    Found(FindResult),
    Counted(u64),
    Inserted(InsertResult),
    Updated(UpdateResult),
    Deleted(DeleteResult),
    Aggregated(AggregateResult),
}

/// Index sub-events.
pub enum IndexEvent {
    Listed(Vec<IndexInfo>),
    Created(CreateIndexResult),
    Dropped { name: String },
    CommandResult(CommandResult),
}
