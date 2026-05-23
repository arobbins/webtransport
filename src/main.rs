use anyhow::Result;
use base64::{Engine, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, Sender};
use tokio::time::{Duration, interval};
use tracing_subscriber::fmt;
use wtransport::Endpoint;
use wtransport::Identity;
use wtransport::ServerConfig;
use wtransport::datagram::Datagram;
use wtransport::tls::Sha256Digest;

const TICKER_INTERVAL: u64 = 3;
const WT_PORT: u16 = 4433;

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClientMessage {
    x_pos: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
struct ServerMessage {
    connection_id: usize,
    connected_on: DateTime<Utc>,
    x_pos: u64,
}

type MessageSender = Sender<serde_json::Value>;

#[derive(Debug, Clone)]
struct Connection {
    connection_id: usize,
    connected_on: DateTime<Utc>,
    latest_message: ClientMessage,
    tx: MessageSender,
}

type ConnectionList = Arc<Mutex<HashMap<usize, Connection>>>;

fn create_connections_registry() -> ConnectionList {
    Arc::new(Mutex::new(HashMap::new()))
}

async fn remove_connection(connection_list: &ConnectionList, connection_id: usize) {
    let mut map = connection_list.lock().await;
    map.remove(&connection_id);
    tracing::info!("Total current connections: {:?}", map.len());
}

async fn add_connection(
    connection_list: &ConnectionList,
    connection: &wtransport::Connection,
    tx: MessageSender,
) {
    let mut map = connection_list.lock().await;
    let connection_id = connection.stable_id();

    map.insert(
        connection_id,
        Connection {
            connection_id,
            connected_on: Utc::now(),
            latest_message: ClientMessage::default(),
            tx,
        },
    );

    tracing::info!("Total current connections: {:?}", map.len());
}

async fn update_connection(
    connection_list: &ConnectionList,
    connection_id: usize,
    message: ClientMessage,
) {
    let mut map = connection_list.lock().await;

    if let Some(connection) = map.get_mut(&connection_id) {
        connection.latest_message = message;
    }
}

async fn create_transmitters_list(
    connection_list: &ConnectionList,
    connection_id: usize,
) -> Vec<MessageSender> {
    let map = connection_list.lock().await;

    map.iter()
        .filter(|(id, _)| **id != connection_id)
        .map(|(_, conn)| conn.tx.clone())
        .collect()
}

async fn broadcast_to_all(transmitters: Vec<MessageSender>, server_message: ServerMessage) {
    let value = match serde_json::to_value(&server_message) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("Serialization failed: {}", e);
            return;
        }
    };

    for tx in transmitters {
        if let Err(e) = tx.send(value.clone()).await {
            tracing::warn!("Broadcast failed: {}", e);
        }
    }
}

async fn handle_connection(
    incoming_session: wtransport::endpoint::IncomingSession,
    connection_list: ConnectionList,
) {
    if let Err(e) = run_connection(incoming_session, &connection_list).await {
        tracing::error!("Connection error: {}", e);
    }
}

fn get_client_message(datagram: &[u8]) -> Result<ClientMessage> {
    let message = str::from_utf8(datagram)?;
    Ok(serde_json::from_str(message)?)
}

fn get_server_message(connection: &Connection, client_message: &ClientMessage) -> ServerMessage {
    ServerMessage {
        connection_id: connection.connection_id,
        connected_on: connection.connected_on,
        x_pos: client_message.x_pos,
    }
}

async fn get_connection(
    connection_list: &ConnectionList,
    connection_id: usize,
) -> Result<Connection> {
    let map = connection_list.lock().await;
    map.get(&connection_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Connection {} not found", connection_id))
}

async fn handle_datagram(
    datagram: Datagram,
    connection_id: usize,
    connection_list: &ConnectionList,
) -> Result<()> {
    if datagram.is_empty() {
        return Ok(());
    }

    let client_message = get_client_message(&datagram)?;
    let connection = get_connection(connection_list, connection_id).await?;
    let server_message = get_server_message(&connection, &client_message);
    let transmitters = create_transmitters_list(connection_list, connection_id).await;

    broadcast_to_all(transmitters, server_message).await;
    update_connection(connection_list, connection_id, client_message).await;

    Ok(())
}

async fn run_connection(
    incoming_session: wtransport::endpoint::IncomingSession,
    connection_list: &ConnectionList,
) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<serde_json::Value>(32);

    let incoming_request = incoming_session.await?;
    let connection = incoming_request.accept().await?;
    let connection_id = connection.stable_id();

    tracing::info!("Connection established: {}", connection_id);
    add_connection(connection_list, &connection, tx).await;

    let result = async {
        loop {
            tokio::select! {
                datagram = connection.receive_datagram() => {
                    if let Err(e) = handle_datagram(datagram?, connection_id, connection_list).await {
                        tracing::warn!("Datagram error: {}", e);
                    }
                }
                msg = rx.recv() => {
                    if let Some(msg) = msg {
                        connection.send_datagram(msg.to_string().as_bytes())?;
                    }
                }
                _ = connection.closed() => {
                        tracing::info!("Connection {} closed.", connection_id);
                        break;
                }
            }
        }
        Ok(())
    }
    .await;

    remove_connection(connection_list, connection_id).await;
    result
}

fn spawn_ticker(connection_list: ConnectionList) {
    tokio::spawn({
        async move {
            let mut interval = interval(Duration::from_secs(TICKER_INTERVAL));
            loop {
                interval.tick().await;
                let map = connection_list.lock().await;
                tracing::info!("Tick — {} active connections", map.len());
            }
        }
    });
}

async fn start_server(config: ServerConfig) -> Result<()> {
    let server = Endpoint::server(config)?;

    let connection_list = create_connections_registry();

    spawn_ticker(connection_list.clone());

    loop {
        tracing::info!("Waiting for incoming connection...");
        let connection_list = Arc::clone(&connection_list);
        let incoming_session = server.accept().await;

        tokio::spawn(handle_connection(incoming_session, connection_list));
    }
}

fn create_config() -> Result<(Sha256Digest, ServerConfig)> {
    let identity = Identity::self_signed(["localhost", "127.0.0.1", "::1"])?;

    let cert_hash = identity.certificate_chain().as_slice()[0].hash();

    let config = ServerConfig::builder()
        .with_bind_default(WT_PORT)
        .with_identity(identity)
        .build();

    Ok((cert_hash, config))
}

async fn run_server() -> Result<()> {
    fmt::init();
    tracing::info!("WebTransport Server started ✅");

    let (cert_hash, config) = create_config()?;
    let cert_hash_str = STANDARD.encode(cert_hash.as_ref());

    std::fs::write("cert-hash.txt", &cert_hash_str)?;
    start_server(config).await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    run_server().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_valid_client_message() {
        let result = get_client_message(b"{\"xPos\":42}");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().x_pos, 42);
    }

    #[test]
    fn should_fail_on_invalid_client_message_key() {
        let result = get_client_message(b"{\"xPosss\":42}");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_successfully_remove_connection() {
        let connection_list = create_connections_registry();
        let (tx1, _rx1) = mpsc::channel(1);
        let (tx2, _rx2) = mpsc::channel(2);

        {
            let mut map = connection_list.lock().await;
            map.insert(
                1,
                Connection {
                    connection_id: 1,
                    connected_on: Utc::now(),
                    latest_message: ClientMessage::default(),
                    tx: tx1,
                },
            );
            map.insert(
                2,
                Connection {
                    connection_id: 2,
                    connected_on: Utc::now(),
                    latest_message: ClientMessage::default(),
                    tx: tx2,
                },
            );
        }

        remove_connection(&connection_list, 1).await;

        let map = connection_list.lock().await;

        assert!(!map.contains_key(&1));
    }
}
