use std::fs;
use std::path::Path;
use voidb_plugin_mongodb::mongodb_capabilities;

fn main() -> anyhow::Result<()> {
    let schemas_dir = Path::new("schemas");
    fs::create_dir_all(schemas_dir)?;

    let capabilities = mongodb_capabilities();

    // Export capability schemas
    for cap in &capabilities {
        let input_path = schemas_dir.join(format!("{}-input.schema.json", cap.id));
        let output_path = schemas_dir.join(format!("{}-output.schema.json", cap.id));

        fs::write(&input_path, serde_json::to_string_pretty(&cap.input_schema)? + "\n")?;
        fs::write(&output_path, serde_json::to_string_pretty(&cap.output_schema)? + "\n")?;
        println!("Exported schemas for capability: {}", cap.id);
    }

    // Export profile schema matching MongoConfig
    let profile_schema = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "MongoConnectionProfile",
        "type": "object",
        "required": ["uri"],
        "properties": {
            "uri": {
                "type": "string",
                "description": "MongoDB connection URI, e.g. mongodb://localhost:27017 or mongodb+srv://cluster..."
            },
            "default_db": {
                "type": ["string", "null"],
                "description": "Default database name"
            },
            "auth": {
                "type": "object",
                "required": ["type"],
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["Password", "X509", "AwsIam"]
                    },
                    "username": {
                        "type": "string",
                        "description": "Username for Password auth"
                    },
                    "password": {
                        "type": "string",
                        "description": "Password for Password auth"
                    },
                    "auth_db": {
                        "type": ["string", "null"],
                        "description": "Authentication source database"
                    },
                    "cert_path": {
                        "type": "string",
                        "description": "Certificate path for X509"
                    },
                    "key_path": {
                        "type": ["string", "null"],
                        "description": "Private key path for X509"
                    },
                    "access_key": {
                        "type": "string",
                        "description": "AWS access key for AwsIam"
                    },
                    "secret_key": {
                        "type": "string",
                        "description": "AWS secret key for AwsIam"
                    },
                    "session_token": {
                        "type": ["string", "null"],
                        "description": "AWS session token"
                    }
                }
            },
            "timeout": {
                "type": "integer",
                "minimum": 1,
                "default": 10,
                "description": "Connection timeout in seconds"
            },
            "tls": {
                "type": "object",
                "properties": {
                    "enabled": { "type": "boolean", "default": false },
                    "ca_file": { "type": ["string", "null"] },
                    "allow_invalid_certs": { "type": "boolean", "default": false }
                },
                "additionalProperties": false
            }
        },
        "additionalProperties": false
    });

    let profile_path = schemas_dir.join("profile.schema.json");
    fs::write(&profile_path, serde_json::to_string_pretty(&profile_schema)? + "\n")?;
    println!("Exported profile schema to {}", profile_path.display());

    Ok(())
}
