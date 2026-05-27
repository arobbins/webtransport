use crate::Config;
use crate::connections::*;
use crate::models::*;

use anyhow::Result;
use tokio::sync::mpsc;
use tokio::time::{Duration, interval};
use wtransport::Endpoint;
use wtransport::datagram::Datagram;

pub fn get_client_message(datagram: &[u8]) -> Result<ClientMessage> {
    let message = str::from_utf8(datagram)?;
    Ok(serde_json::from_str(message)?)
}

fn get_server_message(connection: &Connection, client_message: &ClientMessage) -> ServerMessage {
    ServerMessage {
        connection_id: connection.connection_id,
        connected_on: connection.connected_on,
        data: client_message.clone(),
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

async fn handle_datagram(
    datagram: Datagram,
    connection_id: usize,
    connection_list: &ConnectionList,
) -> Result<()> {
    if datagram.is_empty() {
        return Ok(());
    }

    let connection = get_connection(connection_list, connection_id).await?;
    let client_message = get_client_message(&datagram)?;
    let server_message = get_server_message(&connection, &client_message);
    let transmitters = create_transmitters_list(connection_list, connection_id).await;

    tracing::info!("Received datagram from connection: {}", connection_id);

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

async fn handle_connection(
    incoming_session: wtransport::endpoint::IncomingSession,
    connection_list: ConnectionList,
) {
    if let Err(e) = run_connection(incoming_session, &connection_list).await {
        tracing::error!("Connection error: {}", e);
    }
}

fn start_ticker(connection_list: ConnectionList, ticker_interval: u64) {
    tokio::spawn({
        async move {
            let mut interval = interval(Duration::from_secs(ticker_interval));
            loop {
                interval.tick().await;
                let count = connection_list.lock().await.len();
                tracing::info!("Tick — {} active connections", count);
            }
        }
    });
}

pub async fn start_server(config: Config) -> Result<()> {
    let server = Endpoint::server(config.server_config)?;
    let connection_list = create_connections_registry();

    start_ticker(connection_list.clone(), config.ticker_interval);

    tracing::info!("⚡️ Welcome to Tristram - \"Stay a while and listen.\"");

    tracing::info!("Waiting for incoming connections ...");
    loop {
        let connection_list = connection_list.clone();
        let incoming_session = server.accept().await;

        tokio::spawn(handle_connection(incoming_session, connection_list));
    }
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
}
