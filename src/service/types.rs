//! Typed response wrappers for MongoDB service operations.
//!
//! All types in this module are UI-framework-independent (no TUI dependency).

/// Information about a MongoDB database.
#[derive(Debug, Clone)]
pub struct DbInfo {
    pub name: String,
    pub size_on_disk: u64,
    pub empty: bool,
}

/// Information about a MongoDB collection.
#[derive(Debug, Clone)]
pub struct CollectionInfo {
    pub name: String,
    pub doc_count: u64,
    pub size: u64,
    pub index_count: u32,
}

/// Information about a MongoDB index.
#[derive(Debug, Clone)]
pub struct IndexInfo {
    pub name: String,
    pub keys: bson::Document,
    pub unique: bool,
    pub sparse: bool,
}

/// Typed find result from MongoDB.
#[derive(Debug, Clone)]
pub struct FindResult {
    pub documents: Vec<bson::Document>,
    pub total: u64,
    pub columns: Vec<String>,
}

/// Typed aggregation result.
#[derive(Debug, Clone)]
pub struct AggregateResult {
    pub documents: Vec<bson::Document>,
}

/// Typed insert result.
#[derive(Debug, Clone)]
pub struct InsertResult {
    pub inserted_id: String,
}

/// Typed update result.
#[derive(Debug, Clone)]
pub struct UpdateResult {
    pub modified_count: u64,
}

/// Typed delete result.
#[derive(Debug, Clone)]
pub struct DeleteResult {
    pub deleted_count: u64,
}

/// Typed command result.
#[derive(Debug, Clone)]
pub struct CommandResult {
    pub document: bson::Document,
}

/// Typed create index result.
#[derive(Debug, Clone)]
pub struct CreateIndexResult {
    pub index_name: String,
}
