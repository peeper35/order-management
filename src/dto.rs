use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderMessage {
    pub id: String,
    #[serde(rename = "type")]
    pub queue_type: QueueType,
    pub symbol: String,
    pub open_price: f64,
    pub order_type: OrderType,
    pub sl: f64,
    pub tgt: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderType {
    MIS,
    CNC,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueueType {
    #[serde(rename = "pending_orders")]
    PendingOrders,
    #[serde(rename = "executed_orders")]
    ExecutedOrders,
    #[serde(rename = "update_orders")]
    UpdateOrders,
}

#[derive(Debug, Clone, Serialize)]
pub struct WebsocketMsgSnd<'a> {
    pub action: String,
    pub symbol: &'a str,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WebsocketMsgRcv {
    pub symbol: String,
    pub price: f64,
}
