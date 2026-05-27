use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;

pub type MessageSender = Sender<serde_json::Value>;

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClientMessage {
    pub x_pos: u64,
}

#[derive(Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ServerMessage {
    pub connection_id: usize,
    pub connected_on: DateTime<Utc>,
    pub data: ClientMessage,
}

#[derive(Debug, Clone)]
pub struct Connection {
    pub connection_id: usize,
    pub connected_on: DateTime<Utc>,
    pub latest_message: ClientMessage,
    pub tx: MessageSender,
}
