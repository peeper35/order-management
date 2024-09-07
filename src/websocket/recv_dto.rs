use serde::Deserialize;

#[derive(Deserialize, Default, Debug)]
pub struct RealtimeRecv {
    pub instrument_token: i32,
    pub last_price: f64,
    pub average_price: Option<f64>,
    pub last_quantity: Option<f64>,
    pub last_trade_time: Option<u64>,
    pub oi: Option<f64>,
    pub oi_day_high: Option<f64>,
    pub oi_day_low: Option<f64>,
    pub change: f64,
    pub volume: Option<u64>,
    pub ohlc: OHLC,
    pub buy: Option<Vec<BuySell>>,
    pub buy_quantity: Option<f64>,
    pub sell: Option<Vec<BuySell>>,
    pub sell_quantity: Option<f64>,
    pub timestamp: u64,
}

#[derive(Deserialize, Debug)]
pub struct BuySell {
    pub orders: f64,
    pub price: f64,
    pub quantity: f64,
}

#[derive(Deserialize, Default, Debug)]
pub struct OHLC {
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
}
