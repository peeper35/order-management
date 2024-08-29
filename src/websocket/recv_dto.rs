use serde::Deserialize;

pub const AUTH_RECV_MSG: &str = "AuthenticateResult";
pub const SUBSCRIBE_REALTIME_RECV_MSG: &str = "RealtimeResult";

#[derive(Debug, Deserialize)]
pub struct AuthenticateRecv {
    #[serde(rename = "Complete")]
    pub complete: bool,
    #[serde(rename = "Message")]
    pub message: String,
    #[serde(rename = "MessageType")]
    pub message_type: String,
}

#[derive(Debug, Deserialize)]
pub struct RealtimeRecv {
    #[serde(rename = "Exchange")]
    pub exchange: String,
    #[serde(rename = "InstrumentIdentifier")]
    pub instrument_identifier: String,
    #[serde(rename = "LastTradeTime")]
    pub last_trade_time: u64,
    #[serde(rename = "ServerTime")]
    pub server_time: u64,
    #[serde(rename = "AverageTradedPrice")]
    pub average_traded_price: f32,
    #[serde(rename = "BuyPrice")]
    pub buy_price: f32,
    #[serde(rename = "BuyQty")]
    pub buy_qty: u32,
    #[serde(rename = "Close")]
    pub close: f32,
    #[serde(rename = "High")]
    pub high: f32,
    #[serde(rename = "Low")]
    pub low: f32,
    #[serde(rename = "LastTradePrice")]
    pub last_trade_price: f32,
    #[serde(rename = "LastTradeQty")]
    pub last_trade_qty: u32,
    #[serde(rename = "Open")]
    pub open: f32,
    #[serde(rename = "OpenInterest")]
    pub open_interest: u32,
    #[serde(rename = "QuotationLot")]
    pub quotation_lot: f32,
    #[serde(rename = "SellPrice")]
    pub sell_price: f32,
    #[serde(rename = "SellQty")]
    pub sell_qty: u32,
    #[serde(rename = "TotalQtyTraded")]
    pub total_qty_traded: u32,
    #[serde(rename = "Value")]
    pub value: f32,
    #[serde(rename = "PreOpen")]
    pub pre_open: bool,
    #[serde(rename = "PriceChange")]
    pub price_change: f32,
    #[serde(rename = "PriceChangePercentage")]
    pub price_change_percentage: f32,
    #[serde(rename = "OpenInterestChange")]
    pub open_interest_change: u32,
    #[serde(rename = "MessageType")]
    pub message_type: String,
}
