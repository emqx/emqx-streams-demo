"""Late joiner: replay the telemetry stream from a chosen offset.

Subscribes to $stream/telemetry with the stream-offset subscription
property. History arrives first, then the subscription keeps tailing
live. With --verify-order, asserts that sequence numbers are strictly
increasing per sensor (EMQX Streams guarantee ordering per key; the
key here is the publishing client id).
"""

import argparse
import json
import sys
import threading
import time

from common import connect_v5, stream_offset_props

last_seq: dict[str, int] = {}
received = 0
order_violations = 0
done = threading.Event()
args = None


def on_message(client, userdata, msg):
    global received, order_violations
    reading = json.loads(msg.payload)
    sensor, seq = reading["sensor"], reading["seq"]

    if args.verify_order:
        prev = last_seq.get(sensor, 0)
        if seq <= prev:
            order_violations += 1
            print(f"ORDER VIOLATION: {sensor} seq {seq} after {prev}", flush=True)
        last_seq[sensor] = seq

    received += 1
    print(f"[replay] {sensor} seq={seq} {reading['temp_c']}°C ts={reading['ts']}", flush=True)
    if args.max_messages and received >= args.max_messages:
        done.set()


def main() -> None:
    global args
    parser = argparse.ArgumentParser()
    parser.add_argument("--from", dest="offset", default="earliest",
                        help="earliest | latest | Unix timestamp in microseconds")
    parser.add_argument("--max-messages", type=int, default=0,
                        help="exit after N messages (0 = run forever)")
    parser.add_argument("--timeout", type=int, default=0,
                        help="exit non-zero if --max-messages not reached in time")
    parser.add_argument("--verify-order", action="store_true")
    args = parser.parse_args()

    client = connect_v5(
        f"replay-{int(time.time())}", on_message,
        [("$stream/telemetry", 1, stream_offset_props(args.offset))],
    )
    client.loop_start()
    print(f"replaying $stream/telemetry from {args.offset}", flush=True)

    if args.max_messages:
        if not done.wait(timeout=args.timeout or None):
            print(f"TIMEOUT: got {received}/{args.max_messages} messages", flush=True)
            sys.exit(1)
        if args.verify_order:
            if order_violations:
                sys.exit(2)
            print(f"per-key ordering OK ({len(last_seq)} sensors, {received} messages)",
                  flush=True)
    else:
        while True:
            time.sleep(60)


if __name__ == "__main__":
    main()
