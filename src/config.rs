use serde::{Deserialize, Serialize};

/// MongoDB connection configuration, stored as JSON in ConnectionConfig.plugin_config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongoConfig {
    /// Connection URI (mongodb:// or mongodb+srv://)
    pub uri: String,

    /// Default database (optional)
    #[serde(default)]
    pub default_db: Option<String>,

    /// Authentication (can also be embedded in URI)
    #[serde(default)]
    pub auth: Option<MongoAuth>,

    /// Connection timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,

    /// TLS settings
    #[serde(default)]
    pub tls: MongoTls,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum MongoAuth {
    /// Username/password (SCRAM-SHA-256)
    Password {
        username: String,
        password: String,
        auth_db: Option<String>,
    },
    /// X.509 certificate
    X509 {
        cert_path: String,
        key_path: Option<String>,
    },
    /// AWS IAM
    AwsIam {
        access_key: String,
        secret_key: String,
        session_token: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MongoTls {
    pub enabled: bool,
    pub ca_file: Option<String>,
    pub allow_invalid_certs: bool,
}

fn default_timeout() -> u64 {
    10
}

impl Default for MongoConfig {
    fn default() -> Self {
        Self {
            uri: "mongodb://localhost:27017".to_string(),
            default_db: None,
            auth: None,
            timeout: 10,
            tls: MongoTls::default(),
        }
    }
}
