use crate::models::*;

use anyhow::Result;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub type ConnectionList = Arc<Mutex<HashMap<usize, Connection>>>;

pub fn create_connections_registry() -> ConnectionList {
    Arc::new(Mutex::new(HashMap::new()))
}

pub async fn add_connection(
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

pub async fn remove_connection(connection_list: &ConnectionList, connection_id: usize) {
    let mut map = connection_list.lock().await;
    map.remove(&connection_id);
    tracing::info!("Total current connections: {:?}", map.len());
}

pub async fn update_connection(
    connection_list: &ConnectionList,
    connection_id: usize,
    message: ClientMessage,
) {
    let mut map = connection_list.lock().await;

    if let Some(connection) = map.get_mut(&connection_id) {
        connection.latest_message = message;
    }
}

pub async fn get_connection(
    connection_list: &ConnectionList,
    connection_id: usize,
) -> Result<Connection> {
    let map = connection_list.lock().await;
    map.get(&connection_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Connection {} not found", connection_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn should_successfully_remove_connection() {
        let connection_list = create_connections_registry();
        let (tx1, _rx1) = tokio::sync::mpsc::channel(1);
        let (tx2, _rx2) = tokio::sync::mpsc::channel(2);

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
