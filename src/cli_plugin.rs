use async_trait::async_trait;
use bson::Document;
use clap::{Arg, ArgMatches, Command};
use voidb_core::plugin::cli::{CliContext, CliPlugin};
use voidb_core::VoidbError;

use crate::config::MongoConfig;
use crate::service::MongoService;

pub struct MongoCliPlugin;

pub fn create_mongo_cli_plugin() -> Box<dyn CliPlugin> {
    Box::new(MongoCliPlugin)
}

#[async_trait]
impl CliPlugin for MongoCliPlugin {
    fn plugin_id(&self) -> &str {
        "mongodb"
    }

    fn name(&self) -> &str {
        "MongoDB"
    }

    fn commands(&self) -> Vec<Command> {
        let conn_arg = Arg::new("connection")
            .short('c')
            .long("connection")
            .required(true)
            .help("Connection name");

        vec![
            Command::new("dbs")
                .about("List databases")
                .arg(conn_arg.clone()),
            Command::new("collections")
                .about("List collections in a database")
                .arg(conn_arg.clone())
                .arg(Arg::new("database").required(true).help("Database name")),
            Command::new("stats")
                .about("Show database statistics")
                .arg(conn_arg.clone())
                .arg(Arg::new("database").required(true).help("Database name")),
            Command::new("find")
                .about("Find documents in a collection")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("namespace")
                        .required(true)
                        .help("Database.collection (e.g. mydb.users)"),
                )
                .arg(
                    Arg::new("filter")
                        .help("JSON filter (omit for all documents)"),
                )
                .arg(
                    Arg::new("limit")
                        .short('n')
                        .long("limit")
                        .default_value("20")
                        .help("Max documents to return"),
                )
                .arg(
                    Arg::new("skip")
                        .long("skip")
                        .default_value("0")
                        .help("Number of documents to skip"),
                )
                .arg(
                    Arg::new("sort")
                        .short('s')
                        .long("sort")
                        .help("JSON sort document (e.g. '{\"name\": 1}')"),
                ),
            Command::new("count")
                .about("Count documents in a collection")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("namespace")
                        .required(true)
                        .help("Database.collection"),
                )
                .arg(Arg::new("filter").help("JSON filter (omit for total count)")),
            Command::new("insert")
                .about("Insert a document")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("namespace")
                        .required(true)
                        .help("Database.collection"),
                )
                .arg(
                    Arg::new("document")
                        .required(true)
                        .help("JSON document to insert"),
                ),
            Command::new("update")
                .about("Update documents")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("namespace")
                        .required(true)
                        .help("Database.collection"),
                )
                .arg(
                    Arg::new("filter")
                        .required(true)
                        .help("JSON filter for matching documents"),
                )
                .arg(
                    Arg::new("update")
                        .required(true)
                        .help("JSON update document (e.g. '{\"$set\": {\"age\": 30}}')"),
                ),
            Command::new("delete")
                .about("Delete documents")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("namespace")
                        .required(true)
                        .help("Database.collection"),
                )
                .arg(
                    Arg::new("filter")
                        .required(true)
                        .help("JSON filter for documents to delete"),
                ),
            Command::new("aggregate")
                .about("Run an aggregation pipeline")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("namespace")
                        .required(true)
                        .help("Database.collection"),
                )
                .arg(
                    Arg::new("pipeline")
                        .required(true)
                        .help("JSON array of pipeline stages"),
                ),
            Command::new("indexes")
                .about("List indexes on a collection")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("namespace")
                        .required(true)
                        .help("Database.collection"),
                ),
            Command::new("create-index")
                .about("Create an index")
                .arg(conn_arg.clone())
                .arg(
                    Arg::new("namespace")
                        .required(true)
                        .help("Database.collection"),
                )
                .arg(
                    Arg::new("keys")
                        .required(true)
                        .help("JSON key specification (e.g. '{\"email\": 1}')"),
                )
                .arg(
                    Arg::new("unique")
                        .long("unique")
                        .action(clap::ArgAction::SetTrue)
                        .help("Create a unique index"),
                ),
            Command::new("exec")
                .about("Execute a database command")
                .arg(conn_arg.clone())
                .arg(Arg::new("database").required(true).help("Database name"))
                .arg(
                    Arg::new("command")
                        .required(true)
                        .help("JSON command document"),
                ),
            Command::new("test")
                .about("Test connection")
                .arg(conn_arg),
        ]
    }

    async fn execute(
        &self,
        command: &str,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        match command {
            "dbs" => self.handle_dbs(matches, ctx).await,
            "collections" => self.handle_collections(matches, ctx).await,
            "stats" => self.handle_stats(matches, ctx).await,
            "find" => self.handle_find(matches, ctx).await,
            "count" => self.handle_count(matches, ctx).await,
            "insert" => self.handle_insert(matches, ctx).await,
            "update" => self.handle_update(matches, ctx).await,
            "delete" => self.handle_delete(matches, ctx).await,
            "aggregate" => self.handle_aggregate(matches, ctx).await,
            "indexes" => self.handle_indexes(matches, ctx).await,
            "create-index" => self.handle_create_index(matches, ctx).await,
            "exec" => self.handle_exec(matches, ctx).await,
            "test" => self.handle_test(matches, ctx).await,
            _ => Err(VoidbError::Plugin(format!("Unknown command: {}", command))),
        }
    }
}

impl MongoCliPlugin {
    fn parse_config(conn_name: &str, ctx: &CliContext) -> Result<MongoConfig, VoidbError> {
        let config = ctx.find_connection(conn_name).ok_or_else(|| {
            VoidbError::Plugin(format!("Connection '{}' not found", conn_name))
        })?;

        if config.effective_plugin_id() != "mongodb" {
            return Err(VoidbError::Plugin(format!(
                "Connection '{}' is not a MongoDB connection (plugin: {})",
                conn_name,
                config.effective_plugin_id()
            )));
        }

        config
            .plugin_config
            .as_ref()
            .ok_or_else(|| VoidbError::Connection("Missing plugin_config".to_string()))
            .and_then(|pc| {
                serde_json::from_value(pc.clone())
                    .map_err(|e| VoidbError::Connection(format!("Invalid MongoDB config: {}", e)))
            })
    }

    async fn connect(
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<MongoService, VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let mongo_config = Self::parse_config(conn_name, ctx)?;
        MongoService::new_direct(&mongo_config)
            .await
            .map_err(VoidbError::Plugin)
    }

    fn parse_namespace(ns: &str) -> Result<(&str, &str), VoidbError> {
        let parts: Vec<&str> = ns.splitn(2, '.').collect();
        if parts.len() != 2 {
            return Err(VoidbError::Plugin(
                "Namespace must be 'database.collection'".to_string(),
            ));
        }
        Ok((parts[0], parts[1]))
    }

    fn parse_json_doc(s: &str) -> Result<Document, VoidbError> {
        let value: serde_json::Value = serde_json::from_str(s)
            .map_err(|e| VoidbError::Plugin(format!("Invalid JSON: {}", e)))?;
        bson::to_document(&value)
            .map_err(|e| VoidbError::Plugin(format!("Failed to convert to BSON: {}", e)))
    }

    async fn handle_dbs(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::connect(matches, ctx).await?;
        let dbs = svc
            .direct_list_databases()
            .await
            .map_err(VoidbError::Plugin)?;

        println!("{:<30} {:<15} EMPTY", "NAME", "SIZE");
        for db in &dbs {
            let size = format_bytes(db.size_on_disk);
            println!("{:<30} {:<15} {}", db.name, size, db.empty);
        }
        eprintln!("({} databases)", dbs.len());
        Ok(())
    }

    async fn handle_collections(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::connect(matches, ctx).await?;
        let db_name = matches.get_one::<String>("database").unwrap();

        let colls = svc
            .direct_list_collections(db_name)
            .await
            .map_err(VoidbError::Plugin)?;

        println!("{:<40} {:<12}", "COLLECTION", "DOCUMENTS");
        for c in &colls {
            println!("{:<40} {:<12}", c.name, c.doc_count);
        }
        eprintln!("({} collections)", colls.len());
        Ok(())
    }

    async fn handle_stats(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::connect(matches, ctx).await?;
        let db_name = matches.get_one::<String>("database").unwrap();

        let result = svc
            .direct_run_command(db_name, bson::doc! { "dbStats": 1 })
            .await
            .map_err(VoidbError::Plugin)?;

        println!(
            "{}",
            serde_json::to_string_pretty(&bson::to_bson(&result).unwrap_or_default())
                .unwrap_or_default()
        );
        Ok(())
    }

    async fn handle_find(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::connect(matches, ctx).await?;
        let ns = matches.get_one::<String>("namespace").unwrap();
        let (db_name, coll_name) = Self::parse_namespace(ns)?;

        let filter = if let Some(f) = matches.get_one::<String>("filter") {
            Self::parse_json_doc(f)?
        } else {
            Document::new()
        };

        let limit: i64 = matches
            .get_one::<String>("limit")
            .unwrap()
            .parse()
            .map_err(|_| VoidbError::Plugin("Invalid limit".to_string()))?;

        let skip: u64 = matches
            .get_one::<String>("skip")
            .unwrap()
            .parse()
            .map_err(|_| VoidbError::Plugin("Invalid skip".to_string()))?;

        let sort = if let Some(s) = matches.get_one::<String>("sort") {
            Some(Self::parse_json_doc(s)?)
        } else {
            None
        };

        let result = svc
            .direct_find(db_name, coll_name, filter, Some(limit), Some(skip), sort)
            .await
            .map_err(VoidbError::Plugin)?;

        for doc in &result.documents {
            let json = bson::to_bson(doc).unwrap_or_default();
            println!(
                "{}",
                serde_json::to_string_pretty(&json).unwrap_or_default()
            );
            println!("---");
        }
        eprintln!("({} documents)", result.documents.len());
        Ok(())
    }

    async fn handle_count(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::connect(matches, ctx).await?;
        let ns = matches.get_one::<String>("namespace").unwrap();
        let (db_name, coll_name) = Self::parse_namespace(ns)?;

        let filter = if let Some(f) = matches.get_one::<String>("filter") {
            Self::parse_json_doc(f)?
        } else {
            Document::new()
        };

        let count = svc
            .direct_count(db_name, coll_name, filter)
            .await
            .map_err(VoidbError::Plugin)?;

        println!("{}", count);
        Ok(())
    }

    async fn handle_insert(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::connect(matches, ctx).await?;
        let ns = matches.get_one::<String>("namespace").unwrap();
        let (db_name, coll_name) = Self::parse_namespace(ns)?;
        let doc_str = matches.get_one::<String>("document").unwrap();

        let doc = Self::parse_json_doc(doc_str)?;
        let result = svc
            .direct_insert(db_name, coll_name, doc)
            .await
            .map_err(VoidbError::Plugin)?;

        println!("Inserted: {}", result.inserted_id);
        Ok(())
    }

    async fn handle_update(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::connect(matches, ctx).await?;
        let ns = matches.get_one::<String>("namespace").unwrap();
        let (db_name, coll_name) = Self::parse_namespace(ns)?;
        let filter_str = matches.get_one::<String>("filter").unwrap();
        let update_str = matches.get_one::<String>("update").unwrap();

        let filter = Self::parse_json_doc(filter_str)?;
        let update = Self::parse_json_doc(update_str)?;
        let result = svc
            .direct_update(db_name, coll_name, filter, update)
            .await
            .map_err(VoidbError::Plugin)?;

        println!("{} document(s) updated", result.modified_count);
        Ok(())
    }

    async fn handle_delete(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::connect(matches, ctx).await?;
        let ns = matches.get_one::<String>("namespace").unwrap();
        let (db_name, coll_name) = Self::parse_namespace(ns)?;
        let filter_str = matches.get_one::<String>("filter").unwrap();

        let filter = Self::parse_json_doc(filter_str)?;
        let result = svc
            .direct_delete(db_name, coll_name, filter)
            .await
            .map_err(VoidbError::Plugin)?;

        println!("{} document(s) deleted", result.deleted_count);
        Ok(())
    }

    async fn handle_aggregate(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::connect(matches, ctx).await?;
        let ns = matches.get_one::<String>("namespace").unwrap();
        let (db_name, coll_name) = Self::parse_namespace(ns)?;
        let pipeline_str = matches.get_one::<String>("pipeline").unwrap();

        let pipeline_value: serde_json::Value = serde_json::from_str(pipeline_str)
            .map_err(|e| VoidbError::Plugin(format!("Invalid JSON pipeline: {}", e)))?;

        let pipeline: Vec<Document> = pipeline_value
            .as_array()
            .ok_or_else(|| VoidbError::Plugin("Pipeline must be a JSON array".to_string()))?
            .iter()
            .map(|v| bson::to_document(v).map_err(|e| VoidbError::Plugin(e.to_string())))
            .collect::<Result<Vec<_>, _>>()?;

        let result = svc
            .direct_aggregate(db_name, coll_name, pipeline)
            .await
            .map_err(VoidbError::Plugin)?;

        for doc in &result.documents {
            let json = bson::to_bson(doc).unwrap_or_default();
            println!(
                "{}",
                serde_json::to_string_pretty(&json).unwrap_or_default()
            );
        }
        eprintln!("({} results)", result.documents.len());
        Ok(())
    }

    async fn handle_indexes(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::connect(matches, ctx).await?;
        let ns = matches.get_one::<String>("namespace").unwrap();
        let (db_name, coll_name) = Self::parse_namespace(ns)?;

        let indexes = svc
            .direct_list_indexes(db_name, coll_name)
            .await
            .map_err(VoidbError::Plugin)?;

        println!("{:<30} {:<30} {:<8} SPARSE", "NAME", "KEYS", "UNIQUE");
        for idx in &indexes {
            let keys_str = format!("{}", idx.keys);
            println!(
                "{:<30} {:<30} {:<8} {}",
                idx.name, keys_str, idx.unique, idx.sparse
            );
        }
        eprintln!("({} indexes)", indexes.len());
        Ok(())
    }

    async fn handle_create_index(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = Self::connect(matches, ctx).await?;
        let ns = matches.get_one::<String>("namespace").unwrap();
        let (db_name, coll_name) = Self::parse_namespace(ns)?;
        let keys_str = matches.get_one::<String>("keys").unwrap();
        let unique = matches.get_flag("unique");

        let keys = Self::parse_json_doc(keys_str)?;
        let result = svc
            .direct_create_index(db_name, coll_name, keys, unique)
            .await
            .map_err(VoidbError::Plugin)?;

        println!("Created index: {}", result.index_name);
        Ok(())
    }

    async fn handle_exec(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = Self::connect(matches, ctx).await?;
        let db_name = matches.get_one::<String>("database").unwrap();
        let cmd_str = matches.get_one::<String>("command").unwrap();

        let cmd = Self::parse_json_doc(cmd_str)?;
        let result = svc
            .direct_run_command(db_name, cmd)
            .await
            .map_err(VoidbError::Plugin)?;

        let json = bson::to_bson(&result).unwrap_or_default();
        println!(
            "{}",
            serde_json::to_string_pretty(&json).unwrap_or_default()
        );
        Ok(())
    }

    async fn handle_test(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        let config = ctx.find_connection(conn_name).ok_or_else(|| {
            VoidbError::Plugin(format!("Connection '{}' not found", conn_name))
        })?;

        let result = crate::test_connection(config)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("{}", result);
        Ok(())
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
