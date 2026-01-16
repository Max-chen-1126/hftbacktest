#!/usr/bin/env python3
"""
HashKey BBO vs Depth Comparison Test

Purpose:
1. Compare BBO endpoint vs Depth endpoint performance
2. Verify if BBO "Real-time push" is actually faster than Depth
3. Check BBO and Depth data consistency

Usage:
    python test_hashkey_bbo_vs_depth.py [--duration 60] [--symbol BTCUSDT]
"""

import argparse
import asyncio
import gc
import json
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

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
class BBOEvent:
    """BBO data from bbo endpoint"""
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float
    exch_ts: int  # ms
    local_ts_ns: int  # ns


@dataclass
class DepthBBOEvent:
    """BBO extracted from depth endpoint"""
    bid_price: float
    bid_qty: float
    ask_price: float
    ask_qty: float
    exch_ts: int  # ms
    local_ts_ns: int  # ns
    version: str
    bid_levels: int
    ask_levels: int


@dataclass
class Statistics:
    """Accumulated statistics"""
    # BBO endpoint stats
    bbo_count: int = 0
    bbo_latencies_ms: list = field(default_factory=list)
    bbo_intervals_ms: list = field(default_factory=list)
    bbo_changes: int = 0  # Number of actual BBO changes

    # Depth endpoint stats
    depth_count: int = 0
    depth_latencies_ms: list = field(default_factory=list)
    depth_intervals_ms: list = field(default_factory=list)
    depth_bbo_changes: int = 0  # Number of BBO changes in depth

    # Consistency check
    bbo_depth_matches: int = 0
    bbo_depth_mismatches: int = 0

    # Time advantage analysis
    bbo_first_count: int = 0  # BBO endpoint reported change first
    depth_first_count: int = 0  # Depth endpoint reported change first
    time_advantages_ms: list = field(default_factory=list)


def price_to_tick(px: float, tick_size: float = 0.01) -> int:
    """Convert price to tick integer to avoid float comparison issues"""
    return int(round(px / tick_size))


class BBOvsDepthValidator:
    def __init__(self, symbol: str, duration: int, env: str = "production"):
        self.symbol = symbol
        self.duration = duration
        self.env = env

        # WebSocket URL
        if env == "sandbox":
            self.ws_url = "wss://stream-pro.sim.hashkeydev.com/quote/ws/v2"
        else:
            self.ws_url = "wss://stream-pro.hashkey.com/quote/ws/v2"

        # Timing
        self.start_time_ns = 0

        # Current state
        self.last_bbo: Optional[BBOEvent] = None
        self.last_depth_bbo: Optional[DepthBBOEvent] = None
        self.last_bbo_ts_ns = 0
        self.last_depth_ts_ns = 0

        # Statistics
        self.stats = Statistics()

        # Raw records (JSON)
        self.bbo_records = []
        self.depth_records = []

        # Event lists for post-hoc analysis (with local_ts_ns)
        self.bbo_events: list[BBOEvent] = []
        self.depth_events: list[DepthBBOEvent] = []

        # For detecting who reports change first
        self.pending_bbo_change: Optional[dict] = None
        self.pending_depth_change: Optional[dict] = None

    def get_timestamp_ns(self) -> int:
        return time.time_ns()

    def ns_to_ms(self, ns: int) -> float:
        return ns / 1_000_000

    async def subscribe(self, ws):
        """Subscribe to both BBO and Depth"""
        # BBO subscription
        bbo_sub = {
            "topic": "bbo",
            "event": "sub",
            "params": {"symbol": self.symbol}
        }
        await ws.send(json.dumps(bbo_sub))
        print(f"[INFO] Subscribed to BBO for {self.symbol}")

        # Depth subscription
        depth_sub = {
            "topic": "depth",
            "event": "sub",
            "params": {"binary": False, "symbol": self.symbol}
        }
        await ws.send(json.dumps(depth_sub))
        print(f"[INFO] Subscribed to Depth for {self.symbol}")

    async def send_ping(self, ws):
        """Send periodic ping"""
        while True:
            try:
                ping_msg = {"ping": int(time.time() * 1000)}
                await ws.send(json.dumps(ping_msg))
                await asyncio.sleep(10)
            except Exception:
                break

    def handle_bbo(self, data: dict, local_ts_ns: int):
        """Process BBO message"""
        d = data.get("data", {})
        if not d:
            return

        bid_price = float(d.get("b", 0))
        bid_qty = float(d.get("bz", 0))
        ask_price = float(d.get("a", 0))
        ask_qty = float(d.get("az", 0))
        exch_ts = d.get("t", 0)

        # Calculate latency
        local_ts_ms = local_ts_ns // 1_000_000
        latency_ms = local_ts_ms - exch_ts if exch_ts else 0

        self.stats.bbo_latencies_ms.append(latency_ms)
        self.stats.bbo_count += 1

        # Calculate interval
        if self.last_bbo_ts_ns > 0:
            interval_ms = self.ns_to_ms(local_ts_ns - self.last_bbo_ts_ns)
            self.stats.bbo_intervals_ms.append(interval_ms)
        self.last_bbo_ts_ns = local_ts_ns

        # Check if BBO changed
        bbo_changed = False
        if self.last_bbo:
            if (bid_price != self.last_bbo.bid_price or
                ask_price != self.last_bbo.ask_price):
                bbo_changed = True
                self.stats.bbo_changes += 1

        event = BBOEvent(
            bid_price=bid_price,
            bid_qty=bid_qty,
            ask_price=ask_price,
            ask_qty=ask_qty,
            exch_ts=exch_ts,
            local_ts_ns=local_ts_ns
        )
        self.last_bbo = event

        # Store event for post-hoc windowed analysis
        self.bbo_events.append(event)

        # NOTE: Removed real-time consistency check here.
        # Consistency is now calculated in analyze_consistency_windowed()
        # using proper time alignment (±100ms window) and tick-based comparison.

        spread = ask_price - bid_price if bid_price and ask_price else 0
        change_str = " [CHANGED]" if bbo_changed else ""
        print(f"[BBO] {bid_price}/{ask_price}, spread=${spread:.2f}, lat={latency_ms}ms{change_str}")

    def handle_depth(self, data: dict, local_ts_ns: int):
        """Process Depth message and extract BBO"""
        d = data.get("data", {})
        if isinstance(d, list) and len(d) > 0:
            d = d[0]
        if not d:
            return

        bids = d.get("b", [])
        asks = d.get("a", [])
        exch_ts = d.get("t", 0)
        version = d.get("v", "")

        if not bids or not asks:
            return

        bid_price = float(bids[0][0])
        bid_qty = float(bids[0][1])
        ask_price = float(asks[0][0])
        ask_qty = float(asks[0][1])

        # Calculate latency
        local_ts_ms = local_ts_ns // 1_000_000
        latency_ms = local_ts_ms - exch_ts if exch_ts else 0

        self.stats.depth_latencies_ms.append(latency_ms)
        self.stats.depth_count += 1

        # Calculate interval
        if self.last_depth_ts_ns > 0:
            interval_ms = self.ns_to_ms(local_ts_ns - self.last_depth_ts_ns)
            self.stats.depth_intervals_ms.append(interval_ms)
        self.last_depth_ts_ns = local_ts_ns

        # Check if BBO changed
        bbo_changed = False
        if self.last_depth_bbo:
            if (bid_price != self.last_depth_bbo.bid_price or
                ask_price != self.last_depth_bbo.ask_price):
                bbo_changed = True
                self.stats.depth_bbo_changes += 1

        event = DepthBBOEvent(
            bid_price=bid_price,
            bid_qty=bid_qty,
            ask_price=ask_price,
            ask_qty=ask_qty,
            exch_ts=exch_ts,
            local_ts_ns=local_ts_ns,
            version=version,
            bid_levels=len(bids),
            ask_levels=len(asks)
        )
        self.last_depth_bbo = event

        # Store event for post-hoc windowed analysis
        self.depth_events.append(event)

        spread = ask_price - bid_price
        change_str = " [CHANGED]" if bbo_changed else ""
        v_short = version[-15:] if len(version) > 15 else version
        print(f"[DEPTH] {bid_price}/{ask_price}, spread=${spread:.2f}, "
              f"levels={len(bids)}/{len(asks)}, v={v_short}, lat={latency_ms}ms{change_str}")

    async def record_data(self):
        """Main recording loop"""
        print(f"\n[INFO] Connecting to {self.ws_url}")
        print(f"[INFO] uvloop: {'enabled' if UVLOOP_AVAILABLE else 'not available'}")
        print(f"[INFO] Recording BBO + Depth for {self.duration} seconds...")
        print("-" * 80)

        gc.collect()
        gc.disable()

        try:
            self.start_time_ns = self.get_timestamp_ns()

            async with connect(
                self.ws_url,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
            ) as ws:
                # Setup TCP_NODELAY
                try:
                    transport = ws.transport
                    if hasattr(transport, "get_extra_info"):
                        sock = transport.get_extra_info("socket")
                        if sock:
                            import socket
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                            print("[INFO] TCP_NODELAY enabled")
                except Exception as e:
                    print(f"[WARN] Could not set TCP_NODELAY: {e}")

                await self.subscribe(ws)

                # Start ping task
                ping_task = asyncio.create_task(self.send_ping(ws))

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

                            topic = data.get("topic")

                            if topic == "bbo" and "data" in data:
                                self.bbo_records.append(data)
                                self.handle_bbo(data, local_ts_ns)

                            elif topic == "depth" and "data" in data:
                                self.depth_records.append(data)
                                self.handle_depth(data, local_ts_ns)

                        except asyncio.TimeoutError:
                            elapsed_sec = elapsed_ns / 1_000_000_000
                            print(f"[WARN] No message for 5s... ({elapsed_sec:.0f}s elapsed)")

                finally:
                    ping_task.cancel()

        finally:
            gc.enable()

        print("-" * 80)
        print(f"[INFO] Recording complete. BBO: {self.stats.bbo_count}, "
              f"Depth: {self.stats.depth_count}")

    def analyze_consistency_windowed(
        self, window_ms: float = 100, stale_ms: float = 2000, tick_size: float = 0.01
    ):
        """
        Calculate BBO vs Depth consistency using proper time alignment.

        Methodology:
        - Time alignment: Find closest depth snapshot within ±window_ms
        - Price comparison: Use tick-based comparison to avoid float precision issues
        - Stale filter: Skip if time gap exceeds stale_ms

        Args:
            window_ms: Maximum time difference to consider a valid match (default: 100ms)
            stale_ms: Skip comparison if gap exceeds this value (default: 2000ms)
            tick_size: Price tick size for comparison (default: 0.01)
        """
        if not self.bbo_events or not self.depth_events:
            print("\n--- Data Consistency (Windowed) ---")
            print("Insufficient data for windowed analysis")
            return

        depth = self.depth_events
        depth_idx = 0

        matches = 0
        mismatches = 0
        skipped_no_depth = 0
        skipped_stale = 0

        for b in self.bbo_events:
            # Two-pointer: find the closest depth event
            while (
                depth_idx + 1 < len(depth)
                and depth[depth_idx + 1].local_ts_ns <= b.local_ts_ns
            ):
                depth_idx += 1

            # Consider both the current and next depth as candidates
            candidates = [depth[depth_idx]]
            if depth_idx + 1 < len(depth):
                candidates.append(depth[depth_idx + 1])

            # Find the one with minimum time difference
            best = min(candidates, key=lambda d: abs(d.local_ts_ns - b.local_ts_ns))
            dt_ms = abs(best.local_ts_ns - b.local_ts_ns) / 1_000_000

            # Window filter: skip if no depth within window
            if dt_ms > window_ms:
                skipped_no_depth += 1
                continue

            # Stale filter: skip if gap is too large (shouldn't happen if window_ms < stale_ms)
            if dt_ms > stale_ms:
                skipped_stale += 1
                continue

            # Tick-based comparison to avoid float precision issues
            b_bid = price_to_tick(b.bid_price, tick_size)
            b_ask = price_to_tick(b.ask_price, tick_size)
            d_bid = price_to_tick(best.bid_price, tick_size)
            d_ask = price_to_tick(best.ask_price, tick_size)

            if b_bid == d_bid and b_ask == d_ask:
                matches += 1
            else:
                mismatches += 1

        total = matches + mismatches

        print("\n--- Data Consistency (Windowed) ---")
        print(
            f"Parameters: window=±{window_ms}ms, stale>{stale_ms}ms excluded, tick_size={tick_size}"
        )
        print(f"Matches:    {matches}")
        print(f"Mismatches: {mismatches}")
        print(f"Total compared: {total}")
        if total > 0:
            match_rate = matches / total * 100
            print(f"Match rate: {match_rate:.1f}%")
            # Store for statistics
            self.stats.bbo_depth_matches = matches
            self.stats.bbo_depth_mismatches = mismatches
        print(f"Skipped (no depth in window): {skipped_no_depth}")
        print(f"Skipped (stale): {skipped_stale}")

        return {
            "matches": matches,
            "mismatches": mismatches,
            "total": total,
            "match_rate": matches / total * 100 if total > 0 else None,
            "skipped_no_depth": skipped_no_depth,
            "skipped_stale": skipped_stale,
            "window_ms": window_ms,
            "stale_ms": stale_ms,
            "tick_size": tick_size,
        }

    def analyze_comparison(self):
        """Compare BBO vs Depth performance"""
        print("\n" + "=" * 80)
        print("BBO vs DEPTH COMPARISON ANALYSIS")
        print("=" * 80)

        print("\n--- Message Count ---")
        print(f"BBO messages:   {self.stats.bbo_count}")
        print(f"Depth messages: {self.stats.depth_count}")

        if self.stats.bbo_count > 0 and self.stats.depth_count > 0:
            ratio = self.stats.bbo_count / self.stats.depth_count
            print(f"Ratio (BBO/Depth): {ratio:.2f}x")

        print("\n--- Latency Comparison (ms) ---")
        if self.stats.bbo_latencies_ms:
            bbo_lat = self.stats.bbo_latencies_ms
            print(f"BBO:   min={min(bbo_lat)}, max={max(bbo_lat)}, "
                  f"mean={statistics.mean(bbo_lat):.2f}")

        if self.stats.depth_latencies_ms:
            depth_lat = self.stats.depth_latencies_ms
            print(f"Depth: min={min(depth_lat)}, max={max(depth_lat)}, "
                  f"mean={statistics.mean(depth_lat):.2f}")

        print("\n--- Update Interval Comparison (ms) ---")
        if self.stats.bbo_intervals_ms:
            bbo_int = self.stats.bbo_intervals_ms
            print(f"BBO:   min={min(bbo_int):.1f}, max={max(bbo_int):.1f}, "
                  f"mean={statistics.mean(bbo_int):.1f}")

            # Distribution
            buckets = {'<50ms': 0, '50-100ms': 0, '100-200ms': 0,
                       '200-500ms': 0, '500-1000ms': 0, '>1s': 0}
            for i in bbo_int:
                if i < 50:
                    buckets['<50ms'] += 1
                elif i < 100:
                    buckets['50-100ms'] += 1
                elif i < 200:
                    buckets['100-200ms'] += 1
                elif i < 500:
                    buckets['200-500ms'] += 1
                elif i < 1000:
                    buckets['500-1000ms'] += 1
                else:
                    buckets['>1s'] += 1
            print(f"       Distribution: {buckets}")

        if self.stats.depth_intervals_ms:
            depth_int = self.stats.depth_intervals_ms
            print(f"Depth: min={min(depth_int):.1f}, max={max(depth_int):.1f}, "
                  f"mean={statistics.mean(depth_int):.1f}")

            # Distribution
            buckets = {'<50ms': 0, '50-100ms': 0, '100-200ms': 0,
                       '200-500ms': 0, '500-1000ms': 0, '>1s': 0}
            for i in depth_int:
                if i < 50:
                    buckets['<50ms'] += 1
                elif i < 100:
                    buckets['50-100ms'] += 1
                elif i < 200:
                    buckets['100-200ms'] += 1
                elif i < 500:
                    buckets['200-500ms'] += 1
                elif i < 1000:
                    buckets['500-1000ms'] += 1
                else:
                    buckets['>1s'] += 1
            print(f"       Distribution: {buckets}")

        print("\n--- BBO Change Detection ---")
        print(f"BBO endpoint changes:   {self.stats.bbo_changes}")
        print(f"Depth endpoint changes: {self.stats.depth_bbo_changes}")

        if self.stats.bbo_count > 0:
            bbo_change_rate = self.stats.bbo_changes / self.stats.bbo_count * 100
            print(f"BBO change rate:   {bbo_change_rate:.1f}%")

        if self.stats.depth_count > 0:
            depth_change_rate = self.stats.depth_bbo_changes / self.stats.depth_count * 100
            print(f"Depth change rate: {depth_change_rate:.1f}%")

        # Use windowed consistency analysis instead of real-time comparison
        self.analyze_consistency_windowed(window_ms=100, stale_ms=2000, tick_size=0.01)

        # Verdict
        print("\n" + "-" * 40)
        print("VERDICT:")
        print("-" * 40)

        if self.stats.bbo_count == 0:
            print("BBO endpoint returned NO DATA - may not be available for this symbol")
        elif self.stats.bbo_intervals_ms and self.stats.depth_intervals_ms:
            bbo_mean = statistics.mean(self.stats.bbo_intervals_ms)
            depth_mean = statistics.mean(self.stats.depth_intervals_ms)

            if bbo_mean < depth_mean * 0.5:
                print(f"BBO is SIGNIFICANTLY FASTER ({bbo_mean:.0f}ms vs {depth_mean:.0f}ms)")
                print("RECOMMENDATION: Use BBO endpoint for latency-sensitive strategies")
            elif bbo_mean < depth_mean:
                print(f"BBO is FASTER ({bbo_mean:.0f}ms vs {depth_mean:.0f}ms)")
                print("RECOMMENDATION: Consider BBO for simple strategies")
            else:
                print(f"BBO is NOT faster ({bbo_mean:.0f}ms vs {depth_mean:.0f}ms)")
                print("RECOMMENDATION: Use Depth for more information")

    def save_data(self, filename: str = None):
        """Save collected data"""
        if filename is None:
            filename = f"hashkey_bbo_vs_depth_{self.symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        output = {
            "metadata": {
                "symbol": self.symbol,
                "duration": self.duration,
                "environment": self.env,
                "ws_url": self.ws_url,
                "uvloop_enabled": UVLOOP_AVAILABLE,
                "recorded_at": datetime.now().isoformat(),
                "bbo_count": self.stats.bbo_count,
                "depth_count": self.stats.depth_count,
            },
            "statistics": {
                "bbo": {
                    "count": self.stats.bbo_count,
                    "changes": self.stats.bbo_changes,
                    "latency_ms": {
                        "min": min(self.stats.bbo_latencies_ms)
                        if self.stats.bbo_latencies_ms
                        else None,
                        "max": max(self.stats.bbo_latencies_ms)
                        if self.stats.bbo_latencies_ms
                        else None,
                        "mean": statistics.mean(self.stats.bbo_latencies_ms)
                        if self.stats.bbo_latencies_ms
                        else None,
                    },
                    "interval_ms": {
                        "min": min(self.stats.bbo_intervals_ms)
                        if self.stats.bbo_intervals_ms
                        else None,
                        "max": max(self.stats.bbo_intervals_ms)
                        if self.stats.bbo_intervals_ms
                        else None,
                        "mean": statistics.mean(self.stats.bbo_intervals_ms)
                        if self.stats.bbo_intervals_ms
                        else None,
                    },
                },
                "depth": {
                    "count": self.stats.depth_count,
                    "bbo_changes": self.stats.depth_bbo_changes,
                    "latency_ms": {
                        "min": min(self.stats.depth_latencies_ms)
                        if self.stats.depth_latencies_ms
                        else None,
                        "max": max(self.stats.depth_latencies_ms)
                        if self.stats.depth_latencies_ms
                        else None,
                        "mean": statistics.mean(self.stats.depth_latencies_ms)
                        if self.stats.depth_latencies_ms
                        else None,
                    },
                    "interval_ms": {
                        "min": min(self.stats.depth_intervals_ms)
                        if self.stats.depth_intervals_ms
                        else None,
                        "max": max(self.stats.depth_intervals_ms)
                        if self.stats.depth_intervals_ms
                        else None,
                        "mean": statistics.mean(self.stats.depth_intervals_ms)
                        if self.stats.depth_intervals_ms
                        else None,
                    },
                },
                "consistency": {
                    "matches": self.stats.bbo_depth_matches,
                    "mismatches": self.stats.bbo_depth_mismatches,
                    "methodology": {
                        "description": "Windowed time-aligned comparison with tick-based price matching",
                        "window_ms": 100,
                        "stale_ms": 2000,
                        "tick_size": 0.01,
                    },
                },
            },
            "bbo_records": self.bbo_records,
            "depth_records": self.depth_records,
        }

        with open(filename, 'w') as f:
            json.dump(output, f, indent=2)

        print(f"\n[INFO] Data saved to {filename}")

    async def run(self):
        """Run full comparison test"""
        await self.record_data()
        self.analyze_comparison()
        self.save_data()


def main():
    parser = argparse.ArgumentParser(description="HashKey BBO vs Depth Comparison Test")
    parser.add_argument("--duration", type=int, default=60,
                        help="Recording duration in seconds")
    parser.add_argument("--symbol", type=str, default="BTCUSDT",
                        help="Trading pair")
    parser.add_argument("--env", type=str, choices=["sandbox", "production"],
                        default="production", help="Environment")

    args = parser.parse_args()

    print("=" * 80)
    print("HashKey BBO vs Depth Comparison Test")
    print("=" * 80)
    print(f"Symbol: {args.symbol}")
    print(f"Duration: {args.duration}s")
    print(f"Environment: {args.env}")
    print(f"uvloop: {'available' if UVLOOP_AVAILABLE else 'not installed'}")

    validator = BBOvsDepthValidator(args.symbol, args.duration, args.env)
    asyncio.run(validator.run())

    print("\n" + "=" * 80)
    print("COMPARISON TEST COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
