//! HashKey Public WebSocket Stream
//!
//! Handles market data subscriptions (depth, trades) via WebSocket.
//! Reference: https://hashkeypro-apidoc.readme.io/reference/ws-v2-depth

use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::prelude::{
    Event,
    LOCAL_ASK_DEPTH_EVENT,
    LOCAL_BID_DEPTH_EVENT,
    LOCAL_BUY_TRADE_EVENT,
    LOCAL_SELL_TRADE_EVENT,
    LiveEvent,
};
use tokio::{
    select,
    sync::{
        broadcast::{Receiver, error::RecvError},
        mpsc::UnboundedSender,
    },
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{debug, error};

use crate::{
    connector::PublishEvent,
    hashkey::{
        HashKeyError,
        msg::{DepthMessage, PublicWsMessage, TradeMessage, WsPing, WsSubscription, WsSubscriptionParams},
    },
};

pub struct PublicStream {
    ev_tx: UnboundedSender<PublishEvent>,
    symbol_rx: Receiver<String>,
}

impl PublicStream {
    pub fn new(ev_tx: UnboundedSender<PublishEvent>, symbol_rx: Receiver<String>) -> Self {
        Self { ev_tx, symbol_rx }
    }

    /// Parse depth data from string tuples to (f64, f64)
    fn parse_depth_data(
        bids: &[(String, String)],
        asks: &[(String, String)],
    ) -> Result<(Vec<(f64, f64)>, Vec<(f64, f64)>), HashKeyError> {
        let mut parsed_bids = Vec::with_capacity(bids.len());
        let mut parsed_asks = Vec::with_capacity(asks.len());

        for (price, qty) in bids {
            let px: f64 = price.parse().map_err(|_| {
                HashKeyError::OpError(format!("Invalid bid price: {}", price))
            })?;
            let q: f64 = qty.parse().map_err(|_| {
                HashKeyError::OpError(format!("Invalid bid qty: {}", qty))
            })?;
            parsed_bids.push((px, q));
        }

        for (price, qty) in asks {
            let px: f64 = price.parse().map_err(|_| {
                HashKeyError::OpError(format!("Invalid ask price: {}", price))
            })?;
            let q: f64 = qty.parse().map_err(|_| {
                HashKeyError::OpError(format!("Invalid ask qty: {}", qty))
            })?;
            parsed_asks.push((px, q));
        }

        Ok((parsed_bids, parsed_asks))
    }

    /// Handle incoming depth message
    fn handle_depth(&self, msg: &DepthMessage) -> Result<(), HashKeyError> {
        let local_ts = Utc::now().timestamp_nanos_opt().unwrap();
        let exch_ts = msg.data.t * 1_000_000; // ms -> ns

        // Convert symbol format: BTCUSDT -> BTC_USDT
        let symbol = Self::convert_symbol_to_bot_format(&msg.data.s);

        // Parse bids and asks
        let (bids, asks) = Self::parse_depth_data(&msg.data.b, &msg.data.a)?;

        // Send bid updates
        for (px, qty) in bids {
            self.ev_tx
                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                    symbol: symbol.clone(),
                    event: Event {
                        ev: LOCAL_BID_DEPTH_EVENT,
                        exch_ts,
                        local_ts,
                        order_id: 0,
                        px,
                        qty,
                        ival: 0,
                        fval: 0.0,
                    },
                }))
                .unwrap();
        }

        // Send ask updates
        for (px, qty) in asks {
            self.ev_tx
                .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                    symbol: symbol.clone(),
                    event: Event {
                        ev: LOCAL_ASK_DEPTH_EVENT,
                        exch_ts,
                        local_ts,
                        order_id: 0,
                        px,
                        qty,
                        ival: 0,
                        fval: 0.0,
                    },
                }))
                .unwrap();
        }

        Ok(())
    }

    /// Handle incoming trade message
    fn handle_trade(&self, msg: &TradeMessage) -> Result<(), HashKeyError> {
        let local_ts = Utc::now().timestamp_nanos_opt().unwrap();
        let exch_ts = msg.data.t * 1_000_000; // ms -> ns

        // Parse price and quantity
        let px: f64 = msg.data.p.parse().map_err(|_| {
            HashKeyError::OpError(format!("Invalid price: {}", msg.data.p))
        })?;
        let qty: f64 = msg.data.q.parse().map_err(|_| {
            HashKeyError::OpError(format!("Invalid quantity: {}", msg.data.q))
        })?;

        // Determine event type based on maker flag
        // m = true means buyer is maker (this is a sell trade)
        // m = false means buyer is taker (this is a buy trade)
        let ev = if msg.data.m {
            LOCAL_SELL_TRADE_EVENT
        } else {
            LOCAL_BUY_TRADE_EVENT
        };

        // Use symbol from params, not from data (data.s may not exist)
        let symbol = Self::convert_symbol_to_bot_format(&msg.params.symbol);

        self.ev_tx
            .send(PublishEvent::LiveEvent(LiveEvent::Feed {
                symbol,
                event: Event {
                    ev,
                    exch_ts,
                    local_ts,
                    order_id: 0,
                    px,
                    qty,
                    ival: 0,
                    fval: 0.0,
                },
            }))
            .unwrap();

        Ok(())
    }

    /// Handle incoming WebSocket message
    async fn handle_message(&self, text: &str) -> Result<(), HashKeyError> {
        // Try to parse as different message types
        if let Ok(msg) = serde_json::from_str::<PublicWsMessage>(text) {
            match msg {
                PublicWsMessage::Pong(pong) => {
                    debug!(?pong, "Received pong");
                }
                PublicWsMessage::Depth(depth) => {
                    if let Err(e) = self.handle_depth(&depth) {
                        error!(?e, "Error handling depth message");
                    }
                }
                PublicWsMessage::Trade(trade) => {
                    if let Err(e) = self.handle_trade(&trade) {
                        error!(?e, "Error handling trade message");
                    }
                }
                PublicWsMessage::Realtimes(_) => {
                    // Ignore realtimes for now
                }
            }
        } else {
            debug!(%text, "Unknown message format");
        }

        Ok(())
    }

    /// Convert symbol from HashKey format (BTCUSDT) to Bot format (BTC_USDT)
    fn convert_symbol_to_bot_format(symbol: &str) -> String {
        // Common quote currencies
        let quote_currencies = ["USDT", "USDC", "USD", "BTC", "ETH"];

        for quote in &quote_currencies {
            if symbol.ends_with(quote) {
                let base = &symbol[..symbol.len() - quote.len()];
                return format!("{}_{}", base, quote);
            }
        }

        // If no match found, return as-is
        symbol.to_string()
    }

    /// Convert symbol from Bot format (BTC_USDT) to HashKey format (BTCUSDT)
    fn convert_symbol_to_hashkey_format(symbol: &str) -> String {
        symbol.replace("_", "")
    }

    /// Subscribe to market data for a symbol
    async fn subscribe<S>(&self, write: &mut S, symbol: &str) -> Result<(), HashKeyError>
    where
        S: SinkExt<Message> + Unpin,
        <S as futures_util::Sink<Message>>::Error: std::fmt::Debug,
    {
        let hashkey_symbol = Self::convert_symbol_to_hashkey_format(symbol);

        // Subscribe to depth
        let depth_sub = WsSubscription {
            topic: "depth".to_string(),
            event: "sub".to_string(),
            params: WsSubscriptionParams {
                symbol: hashkey_symbol.clone(),
            },
        };
        let msg = serde_json::to_string(&depth_sub)?;
        write.send(Message::Text(msg.into())).await.map_err(|e| {
            HashKeyError::OpError(format!("Failed to send depth subscription: {:?}", e))
        })?;

        // Subscribe to trades
        let trade_sub = WsSubscription {
            topic: "trade".to_string(),
            event: "sub".to_string(),
            params: WsSubscriptionParams {
                symbol: hashkey_symbol,
            },
        };
        let msg = serde_json::to_string(&trade_sub)?;
        write.send(Message::Text(msg.into())).await.map_err(|e| {
            HashKeyError::OpError(format!("Failed to send trade subscription: {:?}", e))
        })?;

        Ok(())
    }

    /// Connect to WebSocket and start processing messages
    pub async fn connect(&mut self, url: &str) -> Result<(), HashKeyError> {
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();

        // Ping interval (every 10 seconds as per HashKey docs)
        let mut ping_interval = time::interval(Duration::from_secs(10));

        loop {
            select! {
                // Send ping
                _ = ping_interval.tick() => {
                    let ping = WsPing {
                        ping: Utc::now().timestamp_millis(),
                    };
                    let msg = serde_json::to_string(&ping)?;
                    write.send(Message::Text(msg.into())).await?;
                }

                // Handle new symbol subscription
                msg = self.symbol_rx.recv() => {
                    match msg {
                        Ok(symbol) => {
                            if let Err(e) = self.subscribe(&mut write, &symbol).await {
                                error!(?e, %symbol, "Failed to subscribe");
                            }
                        }
                        Err(RecvError::Closed) => {
                            return Ok(());
                        }
                        Err(RecvError::Lagged(num)) => {
                            error!("{num} subscription requests were missed.");
                        }
                    }
                }

                // Handle incoming messages
                message = read.next() => {
                    match message {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(e) = self.handle_message(&text).await {
                                error!(?e, %text, "Error handling message");
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            write.send(Message::Pong(data)).await?;
                        }
                        Some(Ok(Message::Close(frame))) => {
                            return Err(HashKeyError::ConnectionAbort(
                                frame.map(|f| f.to_string()).unwrap_or_default()
                            ));
                        }
                        Some(Ok(Message::Binary(_)))
                        | Some(Ok(Message::Frame(_)))
                        | Some(Ok(Message::Pong(_))) => {}
                        Some(Err(e)) => {
                            return Err(HashKeyError::from(e));
                        }
                        None => {
                            return Err(HashKeyError::ConnectionInterrupted);
                        }
                    }
                }
            }
        }
    }
}
