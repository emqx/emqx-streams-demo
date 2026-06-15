"""Late joiner on the NATS side: ordered JetStream consumer over TELEMETRY.

DeliverPolicy ALL replays the full history, then keeps tailing. With
--verify-order, asserts per-sensor sequence monotonicity (JetStream
preserves stream order; the bridge publishes per-sensor in order).
"""

import argparse
import asyncio
import json
import os
import sys

import nats
from nats.js.api import DeliverPolicy

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="offset", default="earliest",
                        help="earliest | latest")
    parser.add_argument("--max-messages", type=int, default=0)
    parser.add_argument("--timeout", type=int, default=0)
    parser.add_argument("--verify-order", action="store_true")
    args = parser.parse_args()

    policy = DeliverPolicy.ALL if args.offset == "earliest" else DeliverPolicy.NEW
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    sub = await js.subscribe(
        "telemetry.>", stream="TELEMETRY", ordered_consumer=True,
        deliver_policy=policy,
    )
    print(f"replaying TELEMETRY from {args.offset}", flush=True)

    last_seq: dict[str, int] = {}
    received = 0
    violations = 0

    async def consume() -> None:
        nonlocal received, violations
        async for msg in sub.messages:
            reading = json.loads(msg.data)
            sensor, seq = reading["sensor"], reading["seq"]
            if args.verify_order:
                prev = last_seq.get(sensor, 0)
                if seq <= prev:
                    violations += 1
                    print(f"ORDER VIOLATION: {sensor} seq {seq} after {prev}", flush=True)
                last_seq[sensor] = seq
            received += 1
            print(f"[replay] {sensor} seq={seq} {reading['temp_c']}°C ts={reading['ts']}",
                  flush=True)
            if args.max_messages and received >= args.max_messages:
                return

    if args.max_messages:
        try:
            await asyncio.wait_for(consume(), timeout=args.timeout or None)
        except asyncio.TimeoutError:
            print(f"TIMEOUT: got {received}/{args.max_messages} messages", flush=True)
            sys.exit(1)
        if args.verify_order:
            if violations:
                sys.exit(2)
            print(f"per-key ordering OK ({len(last_seq)} sensors, {received} messages)",
                  flush=True)
    else:
        await consume()


if __name__ == "__main__":
    asyncio.run(main())
