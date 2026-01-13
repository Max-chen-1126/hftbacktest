//! HashKey Private WebSocket Stream
//!
//! Handles private data (order updates, executions) via WebSocket.
//! Requires a listen key obtained from REST API.

use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use hftbacktest::prelude::LiveEvent;
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
use tracing::{debug, error, info};

use crate::{
    connector::PublishEvent,
    hashkey::{
        HashKeyError,
        SharedSymbolSet,
        msg::{ExecutionReport, PrivateWsMessage, TicketInfo, WsPing},
        ordermanager::SharedOrderManager,
        rest::HashKeyClient,
    },
};

pub struct PrivateStream {
    ev_tx: UnboundedSender<PublishEvent>,
    order_manager: SharedOrderManager,
    symbols: SharedSymbolSet,
    client: HashKeyClient,
    symbol_rx: Receiver<String>,
}

impl PrivateStream {
    pub fn new(
        ev_tx: UnboundedSender<PublishEvent>,
        order_manager: SharedOrderManager,
        symbols: SharedSymbolSet,
        client: HashKeyClient,
        symbol_rx: Receiver<String>,
    ) -> Self {
        Self {
            ev_tx,
            order_manager,
            symbols,
            client,
            symbol_rx,
        }
    }

    /// Handle execution report (order status update)
    fn handle_execution_report(&self, report: &ExecutionReport) -> Result<(), HashKeyError> {
        debug!(?report, "Received execution report");

        let mut order_manager = self.order_manager.lock().unwrap();

        match order_manager.update_from_execution_report(report) {
            Ok(order_ext) => {
                // Convert symbol back to Bot format
                let symbol = Self::convert_symbol_to_bot_format(&order_ext.symbol);

                self.ev_tx
                    .send(PublishEvent::LiveEvent(LiveEvent::Order {
                        symbol,
                        order: order_ext.order,
                    }))
                    .unwrap();
            }
            Err(HashKeyError::PrefixUnmatched) => {
                // Order not created by this connector, ignore
                debug!(
                    client_order_id = %report.client_order_id,
                    "Ignoring order not created by this connector"
                );
            }
            Err(e) => {
                error!(?e, ?report, "Error updating order from execution report");
            }
        }

        Ok(())
    }

    /// Handle ticket info (trade confirmation)
    fn handle_ticket_info(&self, ticket: &TicketInfo) -> Result<(), HashKeyError> {
        debug!(?ticket, "Received ticket info");

        // Ticket info provides individual trade details
        // This is useful for tracking executions
        // For now, we rely on execution reports for order updates

        Ok(())
    }

    /// Handle incoming WebSocket message
    async fn handle_message(&self, text: &str) -> Result<(), HashKeyError> {
        // Try to parse as different message types
        if let Ok(msg) = serde_json::from_str::<PrivateWsMessage>(text) {
            match msg {
                PrivateWsMessage::Pong(pong) => {
                    debug!(?pong, "Received pong");
                }
                PrivateWsMessage::ExecutionReport(report) => {
                    self.handle_execution_report(&report)?;
                }
                PrivateWsMessage::AccountInfo(info) => {
                    debug!(?info, "Received account info");
                    // Could be used to update balance/position
                }
                PrivateWsMessage::TicketInfo(ticket) => {
                    self.handle_ticket_info(&ticket)?;
                }
            }
        } else {
            debug!(%text, "Unknown private message format");
        }

        Ok(())
    }

    /// Convert symbol from HashKey format to Bot format
    fn convert_symbol_to_bot_format(symbol: &str) -> String {
        let quote_currencies = ["USDT", "USDC", "USD", "BTC", "ETH"];

        for quote in &quote_currencies {
            if symbol.ends_with(quote) {
                let base = &symbol[..symbol.len() - quote.len()];
                return format!("{}_{}", base, quote);
            }
        }

        symbol.to_string()
    }

    /// Start listen key keepalive task
    fn start_keepalive_task(&self, listen_key: String) {
        let client = self.client.clone();

        tokio::spawn(async move {
            // Keep alive every 30 minutes (listen key valid for 60 minutes)
            let mut interval = time::interval(Duration::from_secs(30 * 60));

            loop {
                interval.tick().await;

                match client.keepalive_listen_key(&listen_key).await {
                    Ok(_) => {
                        debug!("Listen key keepalive successful");
                    }
                    Err(e) => {
                        error!(?e, "Failed to keepalive listen key");
                        break;
                    }
                }
            }
        });
    }

    /// Connect to private WebSocket
    pub async fn connect(&mut self, base_url: &str) -> Result<(), HashKeyError> {
        // First, get a listen key
        info!("Obtaining listen key...");
        let listen_key_response = self.client.create_listen_key().await?;
        let listen_key = listen_key_response.listen_key;
        info!("Listen key obtained");

        // Start keepalive task
        self.start_keepalive_task(listen_key.clone());

        // Connect to private WebSocket with listen key
        let url = format!("{}/{}", base_url, listen_key);
        let request = url.into_client_request()?;
        let (ws_stream, _) = connect_async(request).await?;
        let (mut write, mut read) = ws_stream.split();

        info!("Connected to private WebSocket");

        // Ping interval
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

                // Handle new symbol registration
                msg = self.symbol_rx.recv() => {
                    match msg {
                        Ok(symbol) => {
                            debug!(%symbol, "New symbol registered");
                            // Private stream receives all order updates automatically
                            // No need to subscribe per symbol
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
                                error!(?e, %text, "Error handling private message");
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            write.send(Message::Pong(data)).await?;
                        }
                        Some(Ok(Message::Close(frame))) => {
                            // Clean up listen key
                            if let Err(e) = self.client.delete_listen_key(&listen_key).await {
                                error!(?e, "Failed to delete listen key");
                            }
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
