"""Provision the NATS side: two JetStream streams and a KV bucket.

TELEMETRY  append-only history of telemetry.>  (limits retention, 7d)
TASKS      work-queue stream on tasks.>        (messages removed on ack)
sensor-state  KV bucket holding latest reading per sensor

Run NATS the way Synadia recommends: native streams, native consumers,
native KV. Idempotent.
"""

import asyncio
import os

import nats
from nats.js.api import KeyValueConfig, RetentionPolicy, StreamConfig

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
WEEK_S = 7 * 24 * 3600


async def ensure_stream(jsm, config: StreamConfig) -> None:
    try:
        await jsm.add_stream(config)
        print(f"created stream {config.name}")
    except Exception as exc:  # already exists with same/different config
        print(f"stream {config.name}: {exc} (assuming already exists)")


async def main() -> None:
    nc = await nats.connect(NATS_URL)
    jsm = nc.jsm()
    js = nc.jetstream()

    await ensure_stream(
        jsm,
        StreamConfig(
            name="TELEMETRY",
            subjects=["telemetry.>"],
            retention=RetentionPolicy.LIMITS,
            max_age=WEEK_S,
            # Publisher dedup window for Nats-Msg-Id (set by the bridge).
            duplicate_window=120,
        ),
    )
    await ensure_stream(
        jsm,
        StreamConfig(
            name="TASKS",
            subjects=["tasks.>"],
            retention=RetentionPolicy.WORK_QUEUE,
        ),
    )
    try:
        await js.create_key_value(KeyValueConfig(bucket="sensor-state"))
        print("created KV bucket sensor-state")
    except Exception as exc:
        print(f"KV sensor-state: {exc} (assuming already exists)")

    await nc.close()
    print("provisioning complete")


if __name__ == "__main__":
    asyncio.run(main())
