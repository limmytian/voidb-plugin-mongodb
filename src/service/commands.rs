//! MongoDB service commands.
//!
//! Commands sent from the UI layer to the MongoService background task.

use crate::config::MongoConfig;

/// Top-level command enum for the MongoDB service.
pub enum MongoCommand {
    /// Connect with the given config (client created inside background task).
    Connect { config: MongoConfig },
    /// Disconnect and stop the background task.
    Disconnect,
    /// Database-level operations.
    Database(DatabaseCommand),
    /// Document-level operations.
    Document(DocumentCommand),
    /// Index-level operations.
    Index(IndexCommand),
}

/// Database sub-commands.
pub enum DatabaseCommand {
    ListDatabases,
    ListCollections { db: String },
}

/// Document sub-commands.
pub enum DocumentCommand {
    Find {
        db: String,
        collection: String,
        filter: bson::Document,
        limit: Option<i64>,
        skip: Option<u64>,
        sort: Option<bson::Document>,
    },
    Count {
        db: String,
        collection: String,
        filter: bson::Document,
    },
    Insert {
        db: String,
        collection: String,
        doc: bson::Document,
    },
    Update {
        db: String,
        collection: String,
        filter: bson::Document,
        update: bson::Document,
    },
    Delete {
        db: String,
        collection: String,
        filter: bson::Document,
    },
    Aggregate {
        db: String,
        collection: String,
        pipeline: Vec<bson::Document>,
    },
}

/// Index sub-commands.
pub enum IndexCommand {
    List {
        db: String,
        collection: String,
    },
    Create {
        db: String,
        collection: String,
        keys: bson::Document,
        unique: bool,
    },
    Drop {
        db: String,
        collection: String,
        name: String,
    },
    RunCommand {
        db: String,
        command: bson::Document,
    },
}
