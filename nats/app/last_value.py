"""Current state per sensor, from the NATS KV bucket.

The bridge puts the latest reading per sensor into KV; this consumer
watches the bucket. KV is the canonical NATS answer to last-value
state (a bucket is a JetStream stream with per-key compaction).
"""

import asyncio
import json
import os

import nats
import nats.errors

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")


async def main() -> None:
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    kv = await js.key_value("sensor-state")
    state: dict[str, dict] = {}

    print("current state from KV bucket sensor-state", flush=True)
    watcher = await kv.watchall()
    while True:
        try:
            entry = await watcher.updates(timeout=30)
        except nats.errors.TimeoutError:
            continue
        if entry is None or entry.value is None:
            continue  # None marks the end of the initial snapshot
        reading = json.loads(entry.value)
        state[reading["sensor"]] = reading
        snapshot = "  ".join(
            f"{sensor}={e['temp_c']}°C(seq {e['seq']})"
            for sensor, e in sorted(state.items())
        )
        print(f"[state] {snapshot}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
