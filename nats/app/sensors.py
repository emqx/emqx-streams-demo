"""Native NATS sensor fleet.

The NATS-native counterpart to scenario/sensors (the MQTT fleet). Same
data, each fleet speaking its own broker's protocol, no gateway in
between.

Three sensors publish JSON telemetry once a second to
telemetry.<line>.<sensor> (captured by the TELEMETRY stream) and write
their latest reading to the sensor-state KV bucket. A dispatcher
publishes a calibration task to tasks.dispatch every 15 seconds
(drained from the TASKS work-queue stream).

Each telemetry publish carries Nats-Msg-Id = "<sensor>-<seq>", so a
retried publish is deduplicated by JetStream's dedup window.
"""

import asyncio
import json
import os
import random
from datetime import datetime, timezone

import nats

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
LINE = os.environ.get("LINE", "line-1")
SENSORS = ["s1", "s2", "s3"]
TASK_INTERVAL_TICKS = 15


async def main() -> None:
    nc = await nats.connect(NATS_URL, max_reconnect_attempts=-1)
    js = nc.jetstream()
    kv = await js.key_value("sensor-state")

    seq = {s: 0 for s in SENSORS}
    temp = {s: 20.0 + i for i, s in enumerate(SENSORS)}
    task_seq = 0
    ticks = 0
    print(f"publishing to {NATS_URL} as {len(SENSORS)} NATS sensors", flush=True)

    while True:
        for s in SENSORS:
            seq[s] += 1
            temp[s] += random.uniform(-0.3, 0.3)
            reading = {
                "line": LINE,
                "sensor": s,
                "seq": seq[s],
                "temp_c": round(temp[s], 2),
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            payload = json.dumps(reading).encode()
            try:
                await js.publish(
                    f"telemetry.{LINE}.{s}",
                    payload,
                    headers={"Nats-Msg-Id": f"{s}-{seq[s]}"},
                )
                await kv.put(f"{LINE}.{s}", payload)
            except Exception as exc:  # broker restart etc.; reconnect and retry next tick
                print(f"publish error (will retry): {exc}", flush=True)

        ticks += 1
        if ticks % TASK_INTERVAL_TICKS == 0:
            task_seq += 1
            task = {
                "task": "calibrate",
                "sensor": random.choice(SENSORS),
                "seq": task_seq,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            try:
                await js.publish("tasks.dispatch", json.dumps(task).encode())
                print(f"dispatched task #{task_seq}: calibrate {task['sensor']}", flush=True)
            except Exception as exc:
                print(f"task publish error (will retry): {exc}", flush=True)

        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())
