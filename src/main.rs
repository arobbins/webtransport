mod connections;
mod models;
mod server;

use anyhow::Result;
use base64::{Engine, engine::general_purpose::STANDARD};
use tracing::Level;
use wtransport::Identity;
use wtransport::ServerConfig;
use wtransport::tls::Sha256Digest;

struct Config {
    ticker_interval: u64,
    log_level: Level,
    cert_hash: Sha256Digest,
    server_config: ServerConfig,
}

impl Config {
    fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let port: u16 = std::env::var("WT_PORT")
            .unwrap_or_else(|_| "4433".to_string())
            .parse()?;

        let ticker_interval: u64 = std::env::var("TICKER_INTERVAL")
            .unwrap_or_else(|_| "3".to_string())
            .parse()?;

        let log_level: Level = std::env::var("LOG_LEVEL")
            .unwrap_or_else(|_| "info".to_string())
            .parse()?;

        let hosts: Vec<String> = std::env::var("WT_HOSTS")
            .unwrap_or_else(|_| "localhost,127.0.0.1,::1".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let identity = Identity::self_signed(hosts)?;
        let cert_hash = identity.certificate_chain().as_slice()[0].hash();
        let server_config = ServerConfig::builder()
            .with_bind_default(port)
            .with_identity(identity)
            .build();

        Ok(Self {
            ticker_interval,
            log_level,
            cert_hash,
            server_config,
        })
    }
}

fn create_dev_cert_hash_file(config: &Config) -> Result<()> {
    let cert_hash_str = STANDARD.encode(config.cert_hash.as_ref());
    std::fs::write("cert-hash.txt", cert_hash_str)?;
    Ok(())
}

fn init_tracing(config: &Config) {
    tracing_subscriber::fmt()
        .with_max_level(config.log_level)
        .init();
}

async fn boot() -> Result<()> {
    let config = Config::from_env()?;

    init_tracing(&config);

    if let Err(e) = create_dev_cert_hash_file(&config) {
        tracing::warn!("Failed to create dev cert hash file: {}", e);
    }

    server::start_server(config).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    boot().await
}
