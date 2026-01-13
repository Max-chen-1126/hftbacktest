#!/usr/bin/env python3
"""
HashKey WebSocket Data Validation Script (Optimized)

Purpose:
1. Verify if the `v` field has an incrementing pattern for ordering
2. Check if Depth push is a complete book or partial levels
3. Compare Trade vs Depth timestamp latency
4. Confirm if first Depth after subscription is a full snapshot
5. **NEW** Validate Synthetic BBO effectiveness (Trade-driven LOB prediction)

Optimizations:
- High precision timing with time.perf_counter_ns()
- uvloop for faster event loop (if available)
- TCP_NODELAY for reduced network latency
- GC disabled during recording

Usage:
    python test_hashkey_ws.py [--duration 60] [--symbol BTCUSDT] [--env production] [--api-version v2]
"""

import asyncio
import json
import argparse
import gc
import time
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional
import statistics

# Try to use uvloop for better performance
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    UVLOOP_AVAILABLE = True
except ImportError:
    UVLOOP_AVAILABLE = False

try:
    import websockets
    from websockets.client import connect
except ImportError:
    print("Please install websockets: pip install websockets")
    exit(1)


@dataclass
class DepthLevel:
    """Single price level in orderbook"""
    price: float
    qty: float
    exch_ts: int  # Exchange timestamp in ms


@dataclass
class BBOState:
    """Best Bid/Offer state"""
    best_bid: Optional[DepthLevel] = None
    best_ask: Optional[DepthLevel] = None
    version: str = ""
    exch_ts: int = 0
    local_ts_ns: int = 0


@dataclass
class TradeEvent:
    """Trade event data"""
    trade_id: str
    price: float
    qty: float
    is_buyer_maker: bool  # True = SELL (buyer is maker), False = BUY
    exch_ts: int  # ms
    local_ts_ns: int  # ns


@dataclass
class SyntheticBBOPrediction:
    """Record of a synthetic BBO prediction based on trade"""
    trade: TradeEvent
    predicted_side: str  # 'bid' or 'ask'
    predicted_price_consumed: float
    prediction_ts_ns: int
    # Filled in when next depth arrives
    actual_bbo_changed: Optional[bool] = None
    actual_new_price: Optional[float] = None
    validation_ts_ns: Optional[int] = None
    time_advantage_ms: Optional[float] = None


@dataclass
class Statistics:
    """Accumulated statistics"""
    depth_count: int = 0
    trade_count: int = 0
    depth_latencies_ms: list = field(default_factory=list)
    trade_latencies_ms: list = field(default_factory=list)
    depth_intervals_ms: list = field(default_factory=list)
    versions: list = field(default_factory=list)
    bid_counts: list = field(default_factory=list)
    ask_counts: list = field(default_factory=list)
    # Synthetic BBO validation
    predictions: list = field(default_factory=list)
    prediction_hits: int = 0
    prediction_misses: int = 0


class HashKeyDataValidator:
    def __init__(self, symbol: str, duration: int, env: str, api_version: str = "v2"):
        self.symbol = symbol
        self.duration = duration
        self.env = env
        self.api_version = api_version

        # WebSocket URLs
        if env == "sandbox":
            self.ws_url = f"wss://stream-pro.sim.hashkeydev.com/quote/ws/{api_version}"
        else:
            self.ws_url = f"wss://stream-pro.hashkey.com/quote/ws/{api_version}"

        # High precision timing baseline
        self.start_time_ns = 0
        self.start_datetime = None

        # Current BBO state (from depth snapshots)
        self.current_bbo = BBOState()
        self.last_depth_ts_ns = 0

        # Statistics
        self.stats = Statistics()

        # Raw records for detailed analysis
        self.depth_records = []
        self.trade_records = []

        # Pending predictions awaiting validation
        self.pending_predictions: list[SyntheticBBOPrediction] = []

    def get_timestamp_ns(self) -> int:
        """Get high-precision monotonic timestamp in nanoseconds"""
        return time.perf_counter_ns()

    def ns_to_ms(self, ns: int) -> float:
        """Convert nanoseconds to milliseconds"""
        return ns / 1_000_000

    async def subscribe(self, ws):
        """Subscribe to depth and trade topics"""
        if self.api_version == "v1":
            depth_sub = {
                "symbol": self.symbol,
                "topic": "depth",
                "event": "sub",
                "params": {"binary": False},
                "id": 1
            }
            trade_sub = {
                "symbol": self.symbol,
                "topic": "trade",
                "event": "sub",
                "params": {"binary": False},
                "id": 2
            }
        else:  # v2
            depth_sub = {
                "topic": "depth",
                "event": "sub",
                "params": {"binary": False, "symbol": self.symbol}
            }
            trade_sub = {
                "topic": "trade",
                "event": "sub",
                "params": {"binary": False, "symbol": self.symbol}
            }

        await ws.send(json.dumps(depth_sub))
        print(f"[INFO] Subscribed to depth for {self.symbol}")

        await ws.send(json.dumps(trade_sub))
        print(f"[INFO] Subscribed to trade for {self.symbol}")

    async def send_ping(self, ws):
        """Send periodic ping to keep connection alive"""
        while True:
            try:
                ping_msg = {"ping": int(time.time() * 1000)}
                await ws.send(json.dumps(ping_msg))
                await asyncio.sleep(10)
            except Exception:
                break

    def extract_depth_data(self, data: dict) -> dict:
        """Extract depth data handling both v1 (list) and v2 (dict) formats"""
        d = data.get('data')
        if isinstance(d, list) and len(d) > 0:
            return d[0]
        elif isinstance(d, dict):
            return d
        return {}

    def extract_trade_data(self, data: dict) -> dict:
        """Extract trade data handling both v1 (list) and v2 (dict) formats"""
        d = data.get('data')
        if isinstance(d, list) and len(d) > 0:
            return d[0]
        elif isinstance(d, dict):
            return d
        return {}

    def handle_depth(self, data: dict, local_ts_ns: int):
        """Process depth message and update BBO state"""
        d = self.extract_depth_data(data)
        if not d:
            return

        exch_ts = d.get('t', 0)
        version = d.get('v', '')
        bids = d.get('b', [])
        asks = d.get('a', [])

        # Calculate latency
        local_ts_ms = local_ts_ns // 1_000_000
        # Need to convert local monotonic to epoch for comparison
        elapsed_ns = local_ts_ns - self.start_time_ns
        epoch_ms = int(self.start_datetime.timestamp() * 1000) + (elapsed_ns // 1_000_000)
        latency_ms = epoch_ms - exch_ts if exch_ts else 0

        self.stats.depth_latencies_ms.append(latency_ms)
        self.stats.versions.append(version)
        self.stats.bid_counts.append(len(bids))
        self.stats.ask_counts.append(len(asks))
        self.stats.depth_count += 1

        # Calculate interval from last depth
        if self.last_depth_ts_ns > 0:
            interval_ms = self.ns_to_ms(local_ts_ns - self.last_depth_ts_ns)
            self.stats.depth_intervals_ms.append(interval_ms)
        self.last_depth_ts_ns = local_ts_ns

        # Update BBO state
        old_bbo = BBOState(
            best_bid=self.current_bbo.best_bid,
            best_ask=self.current_bbo.best_ask,
            version=self.current_bbo.version,
            exch_ts=self.current_bbo.exch_ts
        )

        if bids:
            self.current_bbo.best_bid = DepthLevel(
                price=float(bids[0][0]),
                qty=float(bids[0][1]),
                exch_ts=exch_ts
            )
        if asks:
            self.current_bbo.best_ask = DepthLevel(
                price=float(asks[0][0]),
                qty=float(asks[0][1]),
                exch_ts=exch_ts
            )
        self.current_bbo.version = version
        self.current_bbo.exch_ts = exch_ts
        self.current_bbo.local_ts_ns = local_ts_ns

        # Validate pending predictions
        self.validate_predictions(old_bbo, local_ts_ns)

        # Output
        best_bid = self.current_bbo.best_bid
        best_ask = self.current_bbo.best_ask
        spread = (best_ask.price - best_bid.price) if (best_bid and best_ask) else 0
        print(f"[DEPTH] v={version[-15:] if len(version) > 15 else version}, "
              f"bids={len(bids)}, asks={len(asks)}, "
              f"BBO={best_bid.price if best_bid else 'N/A'}/"
              f"{best_ask.price if best_ask else 'N/A'}, "
              f"spread={spread:.2f}, lat={latency_ms}ms")

    def handle_trade(self, data: dict, local_ts_ns: int):
        """Process trade message and create synthetic BBO prediction"""
        d = self.extract_trade_data(data)
        if not d:
            return

        trade_id = str(d.get('v', ''))
        price = float(d.get('p', 0))
        qty = float(d.get('q', 0))
        is_buyer_maker = d.get('m', False)
        exch_ts = d.get('t', 0)

        # Calculate latency
        elapsed_ns = local_ts_ns - self.start_time_ns
        epoch_ms = int(self.start_datetime.timestamp() * 1000) + (elapsed_ns // 1_000_000)
        latency_ms = epoch_ms - exch_ts if exch_ts else 0

        self.stats.trade_latencies_ms.append(latency_ms)
        self.stats.trade_count += 1

        trade = TradeEvent(
            trade_id=trade_id,
            price=price,
            qty=qty,
            is_buyer_maker=is_buyer_maker,
            exch_ts=exch_ts,
            local_ts_ns=local_ts_ns
        )

        # Synthetic BBO Prediction Logic
        # If trade happens AT the best price, predict that level will be consumed
        prediction = self.create_synthetic_prediction(trade)

        side = "SELL" if is_buyer_maker else "BUY"
        pred_str = ""
        if prediction:
            pred_str = f" [PRED: {prediction.predicted_side} consumed]"

        print(f"[TRADE] {side} {qty}@{price}, lat={latency_ms}ms{pred_str}")

    def create_synthetic_prediction(self, trade: TradeEvent) -> Optional[SyntheticBBOPrediction]:
        """
        Create a prediction if trade is at BBO.

        Logic:
        - BUY trade (is_buyer_maker=False): Aggressor bought, consumed ASK liquidity
        - SELL trade (is_buyer_maker=True): Aggressor sold, consumed BID liquidity
        """
        if not self.current_bbo.best_bid or not self.current_bbo.best_ask:
            return None

        prediction = None

        if trade.is_buyer_maker:
            # SELL trade - aggressor hit the bid
            # If trade price == best_bid price, predict bid will be consumed
            if abs(trade.price - self.current_bbo.best_bid.price) < 0.01:
                # Check if trade qty >= bid qty (level might be fully consumed)
                if trade.qty >= self.current_bbo.best_bid.qty * 0.5:  # 50% threshold
                    prediction = SyntheticBBOPrediction(
                        trade=trade,
                        predicted_side='bid',
                        predicted_price_consumed=self.current_bbo.best_bid.price,
                        prediction_ts_ns=trade.local_ts_ns
                    )
        else:
            # BUY trade - aggressor lifted the ask
            # If trade price == best_ask price, predict ask will be consumed
            if abs(trade.price - self.current_bbo.best_ask.price) < 0.01:
                if trade.qty >= self.current_bbo.best_ask.qty * 0.5:
                    prediction = SyntheticBBOPrediction(
                        trade=trade,
                        predicted_side='ask',
                        predicted_price_consumed=self.current_bbo.best_ask.price,
                        prediction_ts_ns=trade.local_ts_ns
                    )

        if prediction:
            self.pending_predictions.append(prediction)
            self.stats.predictions.append(prediction)

        return prediction

    def validate_predictions(self, old_bbo: BBOState, validation_ts_ns: int):
        """Validate pending predictions against new depth snapshot"""
        remaining = []

        for pred in self.pending_predictions:
            # Check if BBO actually changed
            if pred.predicted_side == 'bid':
                old_price = old_bbo.best_bid.price if old_bbo.best_bid else None
                new_price = self.current_bbo.best_bid.price if self.current_bbo.best_bid else None

                if old_price and new_price:
                    # BBO changed if price decreased (bid was consumed)
                    bbo_changed = new_price < old_price
                    pred.actual_bbo_changed = bbo_changed
                    pred.actual_new_price = new_price
                    pred.validation_ts_ns = validation_ts_ns
                    pred.time_advantage_ms = self.ns_to_ms(validation_ts_ns - pred.prediction_ts_ns)

                    if bbo_changed:
                        self.stats.prediction_hits += 1
                        print(f"  [✓ PRED HIT] Bid consumed: {old_price} -> {new_price}, "
                              f"advantage={pred.time_advantage_ms:.1f}ms")
                    else:
                        self.stats.prediction_misses += 1
                else:
                    remaining.append(pred)

            elif pred.predicted_side == 'ask':
                old_price = old_bbo.best_ask.price if old_bbo.best_ask else None
                new_price = self.current_bbo.best_ask.price if self.current_bbo.best_ask else None

                if old_price and new_price:
                    # BBO changed if price increased (ask was consumed)
                    bbo_changed = new_price > old_price
                    pred.actual_bbo_changed = bbo_changed
                    pred.actual_new_price = new_price
                    pred.validation_ts_ns = validation_ts_ns
                    pred.time_advantage_ms = self.ns_to_ms(validation_ts_ns - pred.prediction_ts_ns)

                    if bbo_changed:
                        self.stats.prediction_hits += 1
                        print(f"  [✓ PRED HIT] Ask consumed: {old_price} -> {new_price}, "
                              f"advantage={pred.time_advantage_ms:.1f}ms")
                    else:
                        self.stats.prediction_misses += 1
                else:
                    remaining.append(pred)

        self.pending_predictions = remaining

    async def record_data(self):
        """Main recording loop with optimizations"""
        print(f"\n[INFO] Connecting to {self.ws_url}")
        print(f"[INFO] uvloop: {'enabled' if UVLOOP_AVAILABLE else 'not available'}")
        print(f"[INFO] Recording for {self.duration} seconds...")
        print("-" * 80)

        # Disable GC during recording for consistent timing
        gc.collect()
        gc.disable()

        try:
            # Connect with TCP_NODELAY
            async with connect(
                self.ws_url,
                ping_interval=None,  # We handle ping manually
                ping_timeout=None,
                close_timeout=5,
            ) as ws:
                # Try to set TCP_NODELAY
                try:
                    transport = ws.transport
                    if hasattr(transport, 'get_extra_info'):
                        sock = transport.get_extra_info('socket')
                        if sock:
                            import socket
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                            print("[INFO] TCP_NODELAY enabled")
                except Exception as e:
                    print(f"[WARN] Could not set TCP_NODELAY: {e}")

                await self.subscribe(ws)

                # Start ping task
                ping_task = asyncio.create_task(self.send_ping(ws))

                # Initialize timing
                self.start_time_ns = self.get_timestamp_ns()
                self.start_datetime = datetime.now()

                try:
                    while True:
                        elapsed_ns = self.get_timestamp_ns() - self.start_time_ns
                        if elapsed_ns >= self.duration * 1_000_000_000:
                            break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                            local_ts_ns = self.get_timestamp_ns()

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError:
                                continue

                            topic = data.get('topic')

                            if topic == 'depth' and 'data' in data:
                                self.depth_records.append(data)
                                self.handle_depth(data, local_ts_ns)

                            elif topic == 'trade' and 'data' in data:
                                self.trade_records.append(data)
                                self.handle_trade(data, local_ts_ns)

                        except asyncio.TimeoutError:
                            elapsed_s = elapsed_ns / 1_000_000_000
                            print(f"[WARN] No message for 5s... ({elapsed_s:.0f}s elapsed)")

                finally:
                    ping_task.cancel()

        finally:
            gc.enable()

        print("-" * 80)
        print(f"[INFO] Recording complete. Depth: {self.stats.depth_count}, "
              f"Trade: {self.stats.trade_count}")

    def analyze_versions(self):
        """Analyze version field patterns"""
        print("\n" + "=" * 80)
        print("VERSION FIELD ANALYSIS")
        print("=" * 80)

        if not self.stats.versions:
            print("[WARN] No version data")
            return

        print(f"Total: {len(self.stats.versions)}")
        print(f"First 5: {self.stats.versions[:5]}")
        print(f"Last 5: {self.stats.versions[-5:]}")

        # Parse and analyze
        parsed = []
        for v in self.stats.versions:
            if isinstance(v, str) and '_' in v:
                parts = v.split('_')
                try:
                    parsed.append((int(parts[0]), int(parts[1])))
                except ValueError:
                    pass

        if parsed:
            first_parts = [p[0] for p in parsed]
            second_parts = [p[1] for p in parsed]

            is_increasing = all(first_parts[i] <= first_parts[i + 1]
                                for i in range(len(first_parts) - 1))

            gaps = [first_parts[i + 1] - first_parts[i]
                    for i in range(len(first_parts) - 1)]

            print(f"\nFormat: {{id}}_{{count}}")
            print(f"First part monotonic: {is_increasing}")
            print(f"First part range: {min(first_parts)} -> {max(first_parts)}")
            print(f"Second part unique: {set(second_parts)}")
            print(f"Gaps - min: {min(gaps)}, max: {max(gaps)}, avg: {sum(gaps) / len(gaps):.1f}")

            large_gaps = [g for g in gaps if g > 10]
            if large_gaps:
                print(f"Large gaps (>10): {len(large_gaps)} occurrences")

    def analyze_depth(self):
        """Analyze depth completeness"""
        print("\n" + "=" * 80)
        print("DEPTH COMPLETENESS ANALYSIS")
        print("=" * 80)

        if not self.stats.bid_counts:
            print("[WARN] No depth data")
            return

        print(f"Total messages: {len(self.stats.bid_counts)}")

        print(f"\nBid levels:")
        print(f"  Min: {min(self.stats.bid_counts)}, Max: {max(self.stats.bid_counts)}, "
              f"Avg: {statistics.mean(self.stats.bid_counts):.1f}")

        print(f"\nAsk levels:")
        print(f"  Min: {min(self.stats.ask_counts)}, Max: {max(self.stats.ask_counts)}, "
              f"Avg: {statistics.mean(self.stats.ask_counts):.1f}")

        if len(set(self.stats.bid_counts)) == 1 and len(set(self.stats.ask_counts)) == 1:
            print("\n[CONCLUSION] FIXED-SIZE snapshots")
        else:
            print("\n[CONCLUSION] VARIABLE-SIZE snapshots")

    def analyze_timing(self):
        """Analyze latency and intervals"""
        print("\n" + "=" * 80)
        print("TIMING ANALYSIS (High Precision)")
        print("=" * 80)

        if self.stats.depth_latencies_ms:
            lat = self.stats.depth_latencies_ms
            print(f"\nDepth Latency (ms):")
            print(f"  Min: {min(lat)}, Max: {max(lat)}")
            print(f"  Mean: {statistics.mean(lat):.2f}, Median: {statistics.median(lat):.2f}")
            if len(lat) > 1:
                print(f"  StdDev: {statistics.stdev(lat):.2f}")

        if self.stats.depth_intervals_ms:
            intv = self.stats.depth_intervals_ms
            print(f"\nDepth Update Interval (ms):")
            print(f"  Min: {min(intv):.1f}, Max: {max(intv):.1f}")
            print(f"  Mean: {statistics.mean(intv):.1f}, Median: {statistics.median(intv):.1f}")
            if len(intv) > 1:
                print(f"  StdDev: {statistics.stdev(intv):.1f}")

            # Histogram of intervals
            buckets = {'<100ms': 0, '100-200ms': 0, '200-500ms': 0, '>500ms': 0}
            for i in intv:
                if i < 100:
                    buckets['<100ms'] += 1
                elif i < 200:
                    buckets['100-200ms'] += 1
                elif i < 500:
                    buckets['200-500ms'] += 1
                else:
                    buckets['>500ms'] += 1
            print(f"  Distribution: {buckets}")

        if self.stats.trade_latencies_ms:
            lat = self.stats.trade_latencies_ms
            print(f"\nTrade Latency (ms):")
            print(f"  Min: {min(lat)}, Max: {max(lat)}")
            print(f"  Mean: {statistics.mean(lat):.2f}, Median: {statistics.median(lat):.2f}")
            if len(lat) > 1:
                print(f"  StdDev: {statistics.stdev(lat):.2f}")

    def analyze_synthetic_bbo(self):
        """Analyze synthetic BBO prediction effectiveness"""
        print("\n" + "=" * 80)
        print("SYNTHETIC BBO PREDICTION ANALYSIS")
        print("=" * 80)

        total_predictions = len(self.stats.predictions)
        if total_predictions == 0:
            print("[INFO] No predictions made (no trades at BBO with significant volume)")
            print("[INFO] This could mean:")
            print("  - Low trading activity during test period")
            print("  - Trades not occurring at BBO prices")
            print("  - Trade qty < 50% of BBO qty threshold")
            return

        validated = [p for p in self.stats.predictions if p.actual_bbo_changed is not None]
        hits = self.stats.prediction_hits
        misses = self.stats.prediction_misses

        print(f"\nTotal Predictions: {total_predictions}")
        print(f"Validated: {len(validated)}")
        print(f"Hits (BBO changed as predicted): {hits}")
        print(f"Misses (BBO did not change): {misses}")

        if validated:
            accuracy = hits / len(validated) * 100
            print(f"\nPrediction Accuracy: {accuracy:.1f}%")

            # Time advantage analysis
            advantages = [p.time_advantage_ms for p in validated if p.time_advantage_ms]
            if advantages:
                print(f"\nTime Advantage (ms) - Lead time before depth confirmation:")
                print(f"  Min: {min(advantages):.1f}")
                print(f"  Max: {max(advantages):.1f}")
                print(f"  Mean: {statistics.mean(advantages):.1f}")
                if len(advantages) > 1:
                    print(f"  Median: {statistics.median(advantages):.1f}")

            # Detailed hit analysis
            hit_preds = [p for p in validated if p.actual_bbo_changed]
            if hit_preds:
                print(f"\nSuccessful Predictions Detail:")
                for p in hit_preds[:5]:  # Show first 5
                    side = "BID" if p.predicted_side == 'bid' else "ASK"
                    print(f"  {side}: {p.predicted_price_consumed} -> {p.actual_new_price}, "
                          f"trade_qty={p.trade.qty}, advantage={p.time_advantage_ms:.1f}ms")

        print("\n" + "-" * 40)
        print("SYNTHETIC BBO EFFECTIVENESS VERDICT:")
        print("-" * 40)

        if total_predictions == 0:
            print("⚠️  INCONCLUSIVE - Need more trading activity to validate")
        elif hits == 0:
            print("❌ NOT EFFECTIVE - Predictions did not match BBO changes")
            print("   Possible reasons:")
            print("   - Hidden orders / iceberg orders refilling BBO")
            print("   - Multiple trades aggregated before depth update")
            print("   - Threshold too aggressive (50% qty)")
        elif accuracy >= 70:
            print("✅ EFFECTIVE - High prediction accuracy")
            print(f"   {accuracy:.0f}% of trades at BBO correctly predicted price movement")
            if advantages:
                print(f"   Average time advantage: {statistics.mean(advantages):.1f}ms")
        else:
            print("⚠️  PARTIALLY EFFECTIVE - Moderate accuracy")
            print(f"   {accuracy:.0f}% accuracy - use with caution")

    def save_data(self, filename: str = None):
        """Save collected data"""
        if filename is None:
            filename = f"hashkey_data_{self.api_version}_{self.symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        # Serialize predictions
        predictions_data = []
        for p in self.stats.predictions:
            predictions_data.append({
                'trade_id': p.trade.trade_id,
                'trade_price': p.trade.price,
                'trade_qty': p.trade.qty,
                'trade_side': 'SELL' if p.trade.is_buyer_maker else 'BUY',
                'predicted_side': p.predicted_side,
                'predicted_price': p.predicted_price_consumed,
                'actual_changed': p.actual_bbo_changed,
                'actual_new_price': p.actual_new_price,
                'time_advantage_ms': p.time_advantage_ms
            })

        output = {
            "metadata": {
                "symbol": self.symbol,
                "duration": self.duration,
                "environment": self.env,
                "api_version": self.api_version,
                "ws_url": self.ws_url,
                "uvloop_enabled": UVLOOP_AVAILABLE,
                "recorded_at": datetime.now().isoformat(),
                "depth_count": self.stats.depth_count,
                "trade_count": self.stats.trade_count
            },
            "statistics": {
                "depth_latency_ms": {
                    "min": min(self.stats.depth_latencies_ms) if self.stats.depth_latencies_ms else None,
                    "max": max(self.stats.depth_latencies_ms) if self.stats.depth_latencies_ms else None,
                    "mean": statistics.mean(self.stats.depth_latencies_ms) if self.stats.depth_latencies_ms else None
                },
                "depth_interval_ms": {
                    "min": min(self.stats.depth_intervals_ms) if self.stats.depth_intervals_ms else None,
                    "max": max(self.stats.depth_intervals_ms) if self.stats.depth_intervals_ms else None,
                    "mean": statistics.mean(self.stats.depth_intervals_ms) if self.stats.depth_intervals_ms else None
                },
                "synthetic_bbo": {
                    "total_predictions": len(self.stats.predictions),
                    "hits": self.stats.prediction_hits,
                    "misses": self.stats.prediction_misses,
                    "accuracy": (self.stats.prediction_hits / len([p for p in self.stats.predictions if p.actual_bbo_changed is not None]) * 100) if any(p.actual_bbo_changed is not None for p in self.stats.predictions) else None
                }
            },
            "synthetic_bbo_predictions": predictions_data,
            "depth_records": self.depth_records,
            "trade_records": self.trade_records
        }

        with open(filename, 'w') as f:
            json.dump(output, f, indent=2)

        print(f"\n[INFO] Data saved to {filename}")

    async def run(self):
        """Run full validation"""
        await self.record_data()
        self.analyze_versions()
        self.analyze_depth()
        self.analyze_timing()
        self.analyze_synthetic_bbo()
        self.save_data()


def main():
    parser = argparse.ArgumentParser(description="HashKey WebSocket Data Validation (Optimized)")
    parser.add_argument("--duration", type=int, default=60,
                        help="Recording duration in seconds")
    parser.add_argument("--symbol", type=str, default="BTCUSD",
                        help="Trading pair (sandbox: BTCUSD, production: BTCUSDT)")
    parser.add_argument("--env", type=str, choices=["sandbox", "production"],
                        default="sandbox", help="Environment")
    parser.add_argument("--api-version", type=str, choices=["v1", "v2"],
                        default="v2", help="API version")

    args = parser.parse_args()

    print("=" * 80)
    print("HashKey WebSocket Data Validation Script (Optimized)")
    print("=" * 80)
    print(f"Symbol: {args.symbol}")
    print(f"Duration: {args.duration}s")
    print(f"Environment: {args.env}")
    print(f"API Version: {args.api_version}")
    print(f"uvloop: {'available' if UVLOOP_AVAILABLE else 'not installed (pip install uvloop)'}")

    validator = HashKeyDataValidator(args.symbol, args.duration, args.env, args.api_version)
    asyncio.run(validator.run())

    print("\n" + "=" * 80)
    print("VALIDATION COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
