use std::{collections::HashMap, fmt::Debug};

use hftbacktest::prelude::*;
use hftbacktest::{
    live::{
        ipc::iceoryx::IceoryxUnifiedChannel, Instrument, LiveBot, LiveBotBuilder, LoggingRecorder,
    },
    prelude::{Bot, HashMapMarketDepth},
};

use hftbacktest::types::{LiveEvent, Order};

/// A message will be received by the publisher thread and then published to the bots.
#[derive(Debug)]
pub enum PublishEvent {
    BatchStart(u64),
    BatchEnd(u64),
    LiveEvent(LiveEvent),
    RegisterInstrument {
        id: u64,
        symbol: String,
        tick_size: f64,
    },
}

pub fn listen_depth<MD, I, R>(hbt: &mut I, _recorder: &mut R) -> Result<(), i64>
where
    MD: MarketDepth,
    I: Bot<MD>,
    <I as Bot<MD>>::Error: Debug,
{
    while hbt.elapse(1_000_000_000).unwrap() != ElapseResult::EndOfData {
        // let depth: &MD = hbt.depth(0);
        // println!(
        //     "current_timestamp={}, best_bid={}, best_ask={}",
        //     hbt.current_timestamp(),
        //     depth.best_bid(),
        //     depth.best_ask(),
        // );

        // Trades
        for last_trade in hbt.last_trades(0) {
            println!("{:?}", last_trade.px);
            println!("{:?}", last_trade.qty);
            println!("{:?}", last_trade.ev);
            println!("--------------------------------");
        }

        // Feed latency
        // if let Some((local_timestamp, exch_timestamp)) = hbt.feed_latency(0) {
        //     println!("feed_latency -> local_timestamp={}, exch_timestamp={}", local_timestamp, exch_timestamp);
        // } else {
        //     println!("feed_latency=None");
        // }
    }

    println!("Depth test done");
    Ok(())
}

pub fn summit_buy_order<MD, I, R>(hbt: &mut I, _recorder: &mut R) -> Result<(), i64>
where
    MD: MarketDepth,
    I: Bot<MD>,
    <I as Bot<MD>>::Error: Debug,
{
    let order_id: u64 = rand::random::<u64>();
    let price: f64 = 90000.0;
    let qty: f64 = 0.0001;
    std::thread::sleep(std::time::Duration::from_secs(3));

    hbt.submit_buy_order(
        0,
        order_id,
        price,
        qty,
        TimeInForce::GTX,
        OrdType::Limit,
        false,
    )
    .unwrap();

    let order_id: u64 = rand::random::<u64>();
    let price: f64 = 100000.0;
    let qty: f64 = 0.0001;
    std::thread::sleep(std::time::Duration::from_secs(3));

    // hbt.submit_buy_order(
    //     0,
    //     order_id,
    //     price,
    //     qty,
    //     TimeInForce::GTX,
    //     OrdType::Limit,
    //     false,
    // )
    // .unwrap();

    let order_id: u64 = rand::random::<u64>();
    let price: f64 = 110000.0;
    let qty: f64 = 0.0001;
    std::thread::sleep(std::time::Duration::from_secs(3));

    // hbt.submit_buy_order(
    //     0,
    //     order_id,
    //     price,
    //     qty,
    //     TimeInForce::GTX,
    //     OrdType::Limit,
    //     false,
    // )
    // .unwrap();

    println!("Summit buy order done");
    Ok(())
}

pub fn summit_sell_order<MD, I, R>(hbt: &mut I, _recorder: &mut R) -> Result<(), i64>
where
    MD: MarketDepth,
    I: Bot<MD>,
    <I as Bot<MD>>::Error: Debug,
{
    let order_id: u64 = rand::random::<u64>();
    let price: f64 = 94460.8;
    let qty: f64 = 0.00015;
    std::thread::sleep(std::time::Duration::from_secs(3));

    hbt.submit_sell_order(
        0,
        order_id,
        price,
        qty,
        TimeInForce::GTX,
        OrdType::Limit,
        false,
    )
    .unwrap();

    println!("Summit sell order done");
    Ok(())
}

pub fn listen_orders<MD, I, R>(hbt: &mut I, _recorder: &mut R) -> Result<(), i64>
where
    MD: MarketDepth,
    I: Bot<MD>,
    <I as Bot<MD>>::Error: Debug,
{
    while hbt.elapse(1_000_000_000).unwrap() != ElapseResult::EndOfData {
        println!(
            "current_timestamp={}, orders={:?}",
            hbt.current_timestamp(),
            hbt.orders(0)
        );

        // Order latency
        // if let Some((local_timestamp, exch_timestamp, _)) = hbt.order_latency(0) {
        //     println!("order_latency -> local_timestamp={}, exch_timestamp={}", local_timestamp, exch_timestamp);
        // } else {
        //     println!("order_latency=None");
        // }
    }

    println!("Orders test done");
    Ok(())
}

pub fn listen_position<MD, I, R>(hbt: &mut I, _recorder: &mut R) -> Result<(), i64>
where
    MD: MarketDepth,
    I: Bot<MD>,
    <I as Bot<MD>>::Error: Debug,
{
    while hbt.elapse(1_000_000_000).unwrap() != ElapseResult::EndOfData {
        println!(
            "current_timestamp={}, position={}",
            hbt.current_timestamp(),
            hbt.position(0)
        );
    }

    println!("Position test done");
    Ok(())
}

pub fn cancel_order<MD, I, R>(hbt: &mut I, _recorder: &mut R) -> Result<(), i64>
where
    MD: MarketDepth,
    I: Bot<MD>,
    <I as Bot<MD>>::Error: Debug,
{
    std::thread::sleep(std::time::Duration::from_secs(3));
    let orders = hbt.orders(0);
    let cancel_order_ids: Vec<u64> = orders.values().map(|order| order.order_id).collect();
    println!("orders: {:?}", orders);

    for order_id in cancel_order_ids {
        println!("Cancel order id={}", order_id);
        let send = hbt.cancel(0, order_id, false);
        match send {
            Ok(_) => println!("Order {} cancelled successfully", order_id),
            Err(e) => println!("Failed to cancel order {}: {:?}", order_id, e),
        }
    }

    println!("Cancel order done");
    Ok(())
}

pub fn cancel_all_orders<MD, I, R>(hbt: &mut I, _recorder: &mut R) -> Result<(), i64>
where
    MD: MarketDepth,
    I: Bot<MD>,
    <I as Bot<MD>>::Error: Debug,
{
    std::thread::sleep(std::time::Duration::from_secs(3));
    let send = hbt.cancel_all(0);
    match send {
        Ok(_) => println!("All orders cancelled successfully"),
        Err(e) => println!("Failed to cancel orders: {:?}", e),
    }

    println!("Cancel all orders done");
    Ok(())
}

pub fn modify_order<MD, I, R>(hbt: &mut I, _recorder: &mut R) -> Result<(), i64>
where
    MD: MarketDepth,
    I: Bot<MD>,
    <I as Bot<MD>>::Error: Debug,
{
    std::thread::sleep(std::time::Duration::from_secs(5));

    // 取得當前訂單
    let orders = hbt.orders(0);
    println!("Current orders count: {}", orders.len());

    if orders.is_empty() {
        println!("No orders to modify");
        return Ok(());
    }

    // 先收集要修改的訂單資訊（避免借用衝突）
    let order_to_modify = orders.iter().next().map(|(order_id, order)| {
        let original_price = order.price_tick as f64 * order.tick_size;
        let qty = order.qty;
        let tick_size = order.tick_size;
        let status = order.status;
        (*order_id, original_price, qty, tick_size, status)
    });

    // 修改訂單
    if let Some((order_id, original_price, qty, tick_size, status)) = order_to_modify {
        println!(
            "Original order_id={}, price={}, qty={}, status={:?}",
            order_id, original_price, qty, status
        );

        // 修改價格：原價格 + 100 USDT
        let new_price = original_price + 100.0;
        let new_qty = qty;

        println!(
            "Modifying order_id={} to new_price={}, new_qty={}",
            order_id, new_price, new_qty
        );

        let result = hbt.modify(0, order_id, new_price, new_qty, false);
        match result {
            Ok(_) => println!("Order {} modified successfully", order_id),
            Err(e) => println!("Failed to modify order {}: {:?}", order_id, e),
        }
    }

    println!("Modify order done");
    Ok(())
}

/// BBO Maker
pub fn bbo_maker_execute<MD, I, R>(
    hbt: &mut I,
    _recorder: &mut R,
    is_buy: bool,
    asset_no: usize,
    order_quantity: f64,
) -> Result<(), i64>
where
    MD: MarketDepth,
    I: Bot<MD>,
    <I as Bot<MD>>::Error: Debug,
{
    std::thread::sleep(std::time::Duration::from_secs(3));
    let mut current_best_bid: f64 = 0.0;
    let mut current_best_ask: f64 = 0.0;

    // 先等待一次 elapse 讓系統同步實際倉位
    hbt.elapse(1_000_000_000).unwrap();
    let original_position = hbt.position(asset_no);
    println!("初始倉位: {}", original_position);

    while hbt.elapse(500_000_000).unwrap() != ElapseResult::EndOfData {
        // 檢查是否已成交（使用容差避免浮點數精度問題）
        let position = hbt.position(asset_no);
        let position_changed = (position - original_position).abs() > 1e-8;
        if position_changed {
            println!("訂單已成交，倉位從 {} 變為 {}", original_position, position);
            break;
        }

        // 获取当前市场深度
        let depth: &MD = hbt.depth(0);
        let best_bid = depth.best_bid();
        let best_ask = depth.best_ask();
        let tick_size = depth.tick_size();

        println!("best bid={}, best_ask={}", best_bid, best_ask,);

        // 檢查 BBO 價格是否更動，如果更動則取消訂單
        let orders = hbt.orders(0);
        let buy_order_ids: Vec<u64> = orders
            .iter()
            .filter(|(_, order)| order.status != Status::Canceled && order.side == Side::Buy)
            .map(|(order_id, _)| *order_id)
            .collect();
        let sell_order_ids: Vec<u64> = orders
            .iter()
            .filter(|(_, order)| order.status != Status::Canceled && order.side == Side::Sell)
            .map(|(order_id, _)| *order_id)
            .collect();

        if is_buy {
            if !buy_order_ids.is_empty() {
                for order_id in buy_order_ids {
                    if current_best_bid != 0.0 && current_best_bid < best_bid {
                        hbt.cancel(asset_no, order_id, false);
                        println!("BBO 價格變更，取消訂單");
                    }
                }
                continue;
            }
        } else {
            if !sell_order_ids.is_empty() {
                for order_id in sell_order_ids {
                    if current_best_ask != 0.0 && current_best_ask > best_ask {
                        hbt.cancel(asset_no, order_id, false);
                        println!("BBO 價格變更，取消訂單");
                    }
                }
                continue;
            }
        }

        // 提交訂單
        if is_buy {
            let order_id = chrono::Utc::now().timestamp() as u64 + (best_bid / tick_size) as u64;
            hbt.submit_buy_order(
                asset_no,
                order_id,
                best_bid,
                order_quantity,
                TimeInForce::GTX,
                OrdType::Limit,
                false,
            );
            println!("Submit buy order");
            current_best_bid = best_bid;
        } else {
            let order_id = chrono::Utc::now().timestamp() as u64 + (best_ask / tick_size) as u64;
            hbt.submit_sell_order(
                asset_no,
                order_id,
                best_ask,
                order_quantity,
                TimeInForce::GTX,
                OrdType::Limit,
                false,
            );
            println!("Submit sell order");
            current_best_ask = best_ask;
        }
    }

    Ok(())
}

pub fn listen_private_trades<MD, I, R>(hbt: &mut I, _recorder: &mut R) -> Result<(), i64>
where
    MD: MarketDepth,
    I: Bot<MD>,
    <I as Bot<MD>>::Error: Debug,
{
    while hbt.elapse(1_000_000_000).unwrap() != ElapseResult::EndOfData {
        // private trades
        for last_private_trade in hbt.last_private_trades(0) {
            println!(
                "Trade: price={:?}, quantity={:?}, exch_ts={:?}, trade_id={:?}, order_id={:?}",
                last_private_trade.px,
                last_private_trade.qty,
                last_private_trade.exch_ts,
                last_private_trade.trade_id,
                last_private_trade.order_id
            );
            println!("--------------------------------");
        }

        hbt.clear_last_private_trades(Some(0));
    }

    println!("Private trades listening completed");
    Ok(())
}

fn prepare_live() -> LiveBot<IceoryxUnifiedChannel, HashMapMarketDepth> {
    let hbt: LiveBot<IceoryxUnifiedChannel, HashMapMarketDepth> = LiveBotBuilder::new()
        .register(Instrument::new(
            "gate_margin_8_btcusdt_status_log",
            "BTC_USDT",
            0.1,
            0.00001,
            HashMapMarketDepth::new(0.1, 0.00001),
            10000,
        ))
        .build()
        .unwrap();

    hbt
}

fn main() {
    tracing_subscriber::fmt::init();
    let mut hbt = prepare_live();
    let mut recorder = LoggingRecorder::new();

    /// Depth
    // listen_depth(&mut hbt, &mut recorder).unwrap();

    /// Summit buy order
    // summit_buy_order(&mut hbt, &mut recorder).unwrap();

    /// Summit sell order
    // summit_sell_order(&mut hbt, &mut recorder).unwrap();

    // Modify
    // modify_order(&mut hbt, &mut recorder).unwrap();

    /// Orders
    // listen_orders(&mut hbt, &mut recorder).unwrap();

    /// Position
    // listen_position(&mut hbt, &mut recorder).unwrap();

    /// Cancel order
    // cancel_order(&mut hbt, &mut recorder).unwrap();
    // cancel_all_orders(&mut hbt, &mut recorder).unwrap();

    /// BBO Maker
    // bbo_maker_execute(&mut hbt, &mut recorder, true, 0, 0.0001).unwrap();
    // hbt.close().unwrap();

    // Private trades
    listen_private_trades(&mut hbt, &mut recorder).unwrap();
}