"""Competing consumer on the TASKS work-queue stream.

Two replicas share one durable pull consumer; JetStream delivers each
task to exactly one worker, removes it on ack, and redelivers on ack
timeout. This is JetStream's work-queue retention doing what it is
designed for.
"""

import asyncio
import json
import os

import nats

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
WORKER_ID = os.environ.get("WORKER_ID", "worker")


async def main() -> None:
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    sub = await js.pull_subscribe("tasks.>", durable="workers", stream="TASKS")
    print(f"{WORKER_ID} consuming TASKS (durable pull consumer 'workers')", flush=True)

    while True:
        try:
            msgs = await sub.fetch(1, timeout=5)
        except nats.errors.TimeoutError:
            continue
        for msg in msgs:
            task = json.loads(msg.data)
            print(f"[{WORKER_ID}] picked up task #{task['seq']}: "
                  f"{task['task']} {task['sensor']}", flush=True)
            await asyncio.sleep(1)  # pretend to calibrate
            await msg.ack()
            print(f"[{WORKER_ID}] done task #{task['seq']}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
