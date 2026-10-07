//! VoidB MongoDB Plugin - Database and collection capability surface.

mod config;
mod cli_plugin;
mod agent_session;
mod capabilities;
pub mod mongo_ops;
pub mod service;
mod types;

pub use capabilities::{invoke_mongodb_capability, mongodb_capabilities};
pub use agent_session::MongoAgentSessionFactory;
pub use cli_plugin::create_mongo_cli_plugin;
pub use config::MongoConfig;

/// Test a MongoDB connection by pinging the cluster.
pub async fn test_connection(
    conn: &voidb_core::connection::ConnectionConfig,
) -> anyhow::Result<String> {
    let mongo_config: MongoConfig = conn
        .plugin_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing plugin_config"))
        .and_then(|pc| serde_json::from_value(pc.clone()).map_err(Into::into))?;

    let client = mongo_ops::create_client(&mongo_config)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    // Ping the server
    let db = client.database("admin");
    db.run_command(bson::doc! { "ping": 1 })
        .await
        .map_err(|e| anyhow::anyhow!("Ping failed: {}", e))?;

    // Get server info
    let dbs = mongo_ops::list_databases(&client)
        .await
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok(format!("OK: {} databases at {}", dbs.len(), mongo_config.uri))
}
