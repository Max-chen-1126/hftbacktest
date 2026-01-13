#!/usr/bin/env python3
"""
HashKey Multi-Connection Interleaving Test

Purpose:
Validate if multiple WebSocket connections receive updates at different times,
which would allow "synthetic" higher frequency sampling by merging streams.

Hypothesis:
If the exchange has server-side jitter in pushing to different connections,
we can merge 3-5 streams to achieve ~20-30ms effective sampling rate instead of 100ms.

Usage:
    python test_hashkey_multiconn.py --connections 3 --duration 60 --symbol BTCUSDT
"""

import asyncio
import json
import argparse
import time
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
import statistics

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    UVLOOP_AVAILABLE = True
except ImportError:
    UVLOOP_AVAILABLE = False

try:
    import websockets
except ImportError:
    print("Please install websockets: pip install websockets")
    exit(1)


@dataclass
class VersionEvent:
    """A single depth event with version and timing"""
    version: str
    version_id: int  # First part of version (numeric)
    conn_id: int
    local_ts_ns: int
    exch_ts: int


@dataclass
class ConnectionStats:
    """Statistics for a single connection"""
    conn_id: int
    depth_count: int = 0
    first_ts_ns: int = 0
    last_ts_ns: int = 0
    versions: list = field(default_factory=list)


class MultiConnectionValidator:
    def __init__(self, symbol: str, duration: int, num_connections: int, env: str):
        self.symbol = symbol
        self.duration = duration
        self.num_connections = num_connections
        self.env = env

        if env == "sandbox":
            self.ws_url = "wss://stream-pro.sim.hashkeydev.com/quote/ws/v2"
        else:
            self.ws_url = "wss://stream-pro.hashkey.com/quote/ws/v2"

        self.start_time_ns = 0
        self.start_datetime = None

        # All version events from all connections
        self.all_events: list[VersionEvent] = []

        # Per-connection stats
        self.conn_stats: dict[int, ConnectionStats] = {}

        # Lock for thread-safe access
        self.lock = asyncio.Lock()

    def get_timestamp_ns(self) -> int:
        return time.perf_counter_ns()

    def ns_to_ms(self, ns: int) -> float:
        return ns / 1_000_000

    async def run_connection(self, conn_id: int):
        """Run a single WebSocket connection"""
        stats = ConnectionStats(conn_id=conn_id)
        self.conn_stats[conn_id] = stats

        try:
            async with websockets.connect(
                self.ws_url,
                ping_interval=None,
                ping_timeout=None,
            ) as ws:
                # Subscribe to depth
                sub_msg = {
                    "topic": "depth",
                    "event": "sub",
                    "params": {"binary": False, "symbol": self.symbol}
                }
                await ws.send(json.dumps(sub_msg))

                # Start ping task
                async def ping_loop():
                    while True:
                        try:
                            await ws.send(json.dumps({"ping": int(time.time() * 1000)}))
                            await asyncio.sleep(10)
                        except:
                            break

                ping_task = asyncio.create_task(ping_loop())

                try:
                    while True:
                        elapsed_ns = self.get_timestamp_ns() - self.start_time_ns
                        if elapsed_ns >= self.duration * 1_000_000_000:
                            break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                            local_ts_ns = self.get_timestamp_ns()

                            data = json.loads(msg)
                            if data.get('topic') != 'depth' or 'data' not in data:
                                continue

                            d = data['data']
                            if isinstance(d, list) and len(d) > 0:
                                d = d[0]

                            version = d.get('v', '')
                            exch_ts = d.get('t', 0)

                            # Parse version ID
                            version_id = 0
                            if '_' in version:
                                try:
                                    version_id = int(version.split('_')[0])
                                except:
                                    pass

                            event = VersionEvent(
                                version=version,
                                version_id=version_id,
                                conn_id=conn_id,
                                local_ts_ns=local_ts_ns,
                                exch_ts=exch_ts
                            )

                            async with self.lock:
                                self.all_events.append(event)
                                stats.depth_count += 1
                                stats.versions.append(version_id)
                                if stats.first_ts_ns == 0:
                                    stats.first_ts_ns = local_ts_ns
                                stats.last_ts_ns = local_ts_ns

                        except asyncio.TimeoutError:
                            continue

                finally:
                    ping_task.cancel()

        except Exception as e:
            print(f"[CONN {conn_id}] Error: {e}")

    async def run(self):
        """Run all connections in parallel"""
        print(f"\n[INFO] Starting {self.num_connections} connections to {self.ws_url}")
        print(f"[INFO] Recording for {self.duration} seconds...")
        print("-" * 80)

        self.start_time_ns = self.get_timestamp_ns()
        self.start_datetime = datetime.now()

        # Start all connections
        tasks = [self.run_connection(i) for i in range(self.num_connections)]
        await asyncio.gather(*tasks)

        print("-" * 80)
        print(f"[INFO] Recording complete. Total events: {len(self.all_events)}")

        self.analyze()

    def analyze(self):
        """Analyze multi-connection data"""
        print("\n" + "=" * 80)
        print("MULTI-CONNECTION ANALYSIS")
        print("=" * 80)

        # Per-connection stats
        print("\n### Per-Connection Statistics ###")
        for conn_id, stats in sorted(self.conn_stats.items()):
            duration_s = self.ns_to_ms(stats.last_ts_ns - stats.first_ts_ns) / 1000 if stats.first_ts_ns else 0
            rate = stats.depth_count / duration_s if duration_s > 0 else 0
            print(f"  Conn {conn_id}: {stats.depth_count} events, {rate:.1f} events/sec")

        # Group events by version
        version_events: dict[int, list[VersionEvent]] = {}
        for ev in self.all_events:
            if ev.version_id not in version_events:
                version_events[ev.version_id] = []
            version_events[ev.version_id].append(ev)

        print(f"\n### Version Distribution ###")
        print(f"  Total unique versions: {len(version_events)}")

        # Analyze versions received by multiple connections
        multi_conn_versions = {v: evs for v, evs in version_events.items() if len(evs) > 1}
        single_conn_versions = {v: evs for v, evs in version_events.items() if len(evs) == 1}

        print(f"  Versions received by multiple conns: {len(multi_conn_versions)}")
        print(f"  Versions received by single conn: {len(single_conn_versions)}")

        if not multi_conn_versions:
            print("\n[WARN] No versions received by multiple connections - cannot analyze jitter")
            return

        # Analyze timing differences for same version across connections
        print("\n### Same-Version Timing Analysis ###")
        print("(Time difference between connections receiving the SAME version)")

        timing_diffs = []
        for version_id, events in multi_conn_versions.items():
            events_sorted = sorted(events, key=lambda e: e.local_ts_ns)
            for i in range(1, len(events_sorted)):
                diff_ms = self.ns_to_ms(events_sorted[i].local_ts_ns - events_sorted[0].local_ts_ns)
                timing_diffs.append(diff_ms)

        if timing_diffs:
            print(f"  Samples: {len(timing_diffs)}")
            print(f"  Min diff: {min(timing_diffs):.2f}ms")
            print(f"  Max diff: {max(timing_diffs):.2f}ms")
            print(f"  Mean diff: {statistics.mean(timing_diffs):.2f}ms")
            print(f"  Median diff: {statistics.median(timing_diffs):.2f}ms")
            if len(timing_diffs) > 1:
                print(f"  StdDev: {statistics.stdev(timing_diffs):.2f}ms")

            # Histogram
            buckets = {'<1ms': 0, '1-5ms': 0, '5-20ms': 0, '20-50ms': 0, '>50ms': 0}
            for d in timing_diffs:
                if d < 1:
                    buckets['<1ms'] += 1
                elif d < 5:
                    buckets['1-5ms'] += 1
                elif d < 20:
                    buckets['5-20ms'] += 1
                elif d < 50:
                    buckets['20-50ms'] += 1
                else:
                    buckets['>50ms'] += 1
            print(f"  Distribution: {buckets}")

        # Calculate effective sampling rate if we merge all connections
        print("\n### Merged Stream Analysis ###")

        # Sort all events by timestamp
        all_sorted = sorted(self.all_events, key=lambda e: e.local_ts_ns)

        # Remove duplicate versions (keep first occurrence)
        seen_versions = set()
        unique_events = []
        for ev in all_sorted:
            if ev.version_id not in seen_versions:
                seen_versions.add(ev.version_id)
                unique_events.append(ev)

        print(f"  Total events (all conns): {len(self.all_events)}")
        print(f"  Unique versions (merged): {len(unique_events)}")

        # Calculate effective intervals
        if len(unique_events) > 1:
            merged_intervals = []
            for i in range(1, len(unique_events)):
                interval_ms = self.ns_to_ms(unique_events[i].local_ts_ns - unique_events[i-1].local_ts_ns)
                merged_intervals.append(interval_ms)

            print(f"\n  Merged stream intervals:")
            print(f"    Min: {min(merged_intervals):.1f}ms")
            print(f"    Max: {max(merged_intervals):.1f}ms")
            print(f"    Mean: {statistics.mean(merged_intervals):.1f}ms")
            print(f"    Median: {statistics.median(merged_intervals):.1f}ms")

            # Compare to single connection
            single_conn_intervals = []
            for conn_id, stats in self.conn_stats.items():
                if len(stats.versions) > 1:
                    # Get events for this connection sorted by time
                    conn_events = sorted(
                        [e for e in self.all_events if e.conn_id == conn_id],
                        key=lambda e: e.local_ts_ns
                    )
                    for i in range(1, len(conn_events)):
                        interval = self.ns_to_ms(conn_events[i].local_ts_ns - conn_events[i-1].local_ts_ns)
                        single_conn_intervals.append(interval)
                break  # Just use first connection for comparison

            if single_conn_intervals:
                print(f"\n  Single connection intervals (for comparison):")
                print(f"    Mean: {statistics.mean(single_conn_intervals):.1f}ms")
                print(f"    Median: {statistics.median(single_conn_intervals):.1f}ms")

                improvement = statistics.mean(single_conn_intervals) / statistics.mean(merged_intervals)
                print(f"\n  Improvement factor: {improvement:.2f}x")

        # Verdict
        print("\n" + "-" * 40)
        print("MULTI-CONNECTION INTERLEAVING VERDICT:")
        print("-" * 40)

        if not timing_diffs:
            print("⚠️  INCONCLUSIVE - Not enough overlapping data")
        elif statistics.mean(timing_diffs) > 20:
            print("✅ EFFECTIVE - Significant jitter between connections")
            print(f"   Average {statistics.mean(timing_diffs):.1f}ms difference for same version")
            print(f"   Multi-connection merging CAN improve effective sampling rate")
        elif statistics.mean(timing_diffs) > 5:
            print("⚠️  PARTIALLY EFFECTIVE - Moderate jitter")
            print(f"   Average {statistics.mean(timing_diffs):.1f}ms difference")
            print("   Some benefit from multi-connection, but limited")
        else:
            print("❌ NOT EFFECTIVE - Low jitter between connections")
            print(f"   Average {statistics.mean(timing_diffs):.1f}ms difference")
            print("   Connections are well-synchronized, multi-conn provides little benefit")

        # Save results
        self.save_results()

    def save_results(self):
        """Save analysis results to JSON"""
        filename = f"hashkey_multiconn_{self.num_connections}conn_{self.symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        # Prepare event data
        events_data = []
        for ev in self.all_events:
            events_data.append({
                'version': ev.version,
                'version_id': ev.version_id,
                'conn_id': ev.conn_id,
                'local_ts_ns': ev.local_ts_ns,
                'exch_ts': ev.exch_ts
            })

        output = {
            "metadata": {
                "symbol": self.symbol,
                "duration": self.duration,
                "num_connections": self.num_connections,
                "environment": self.env,
                "ws_url": self.ws_url,
                "recorded_at": datetime.now().isoformat(),
                "total_events": len(self.all_events)
            },
            "per_connection_stats": {
                str(k): {"depth_count": v.depth_count}
                for k, v in self.conn_stats.items()
            },
            "events": events_data
        }

        with open(filename, 'w') as f:
            json.dump(output, f, indent=2)

        print(f"\n[INFO] Data saved to {filename}")


def main():
    parser = argparse.ArgumentParser(description="HashKey Multi-Connection Interleaving Test")
    parser.add_argument("--connections", type=int, default=3,
                        help="Number of parallel WebSocket connections (default: 3)")
    parser.add_argument("--duration", type=int, default=60,
                        help="Recording duration in seconds (default: 60)")
    parser.add_argument("--symbol", type=str, default="BTCUSDT",
                        help="Trading pair symbol")
    parser.add_argument("--env", type=str, choices=["sandbox", "production"],
                        default="production", help="Environment")

    args = parser.parse_args()

    print("=" * 80)
    print("HashKey Multi-Connection Interleaving Test")
    print("=" * 80)
    print(f"Symbol: {args.symbol}")
    print(f"Connections: {args.connections}")
    print(f"Duration: {args.duration}s")
    print(f"Environment: {args.env}")
    print(f"uvloop: {'available' if UVLOOP_AVAILABLE else 'not installed'}")

    validator = MultiConnectionValidator(
        args.symbol, args.duration, args.connections, args.env
    )
    asyncio.run(validator.run())

    print("\n" + "=" * 80)
    print("TEST COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
