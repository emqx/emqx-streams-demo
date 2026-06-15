"""Competing consumer on a Message Queue.

Two replicas subscribe to $queue/tasks; the broker load-balances,
and each task is processed by exactly one worker. This is EMQX's
work-queue primitive (Message Queues), a separate feature from Streams.
"""

import json
import os
import time

from common import connect_v5

WORKER_ID = os.environ.get("WORKER_ID", "worker")


def on_message(client, userdata, msg):
    task = json.loads(msg.payload)
    print(f"[{WORKER_ID}] picked up task #{task['seq']}: {task['task']} {task['sensor']}",
          flush=True)
    time.sleep(1)  # pretend to calibrate
    print(f"[{WORKER_ID}] done task #{task['seq']}", flush=True)


def main() -> None:
    client = connect_v5(WORKER_ID, on_message, [("$queue/tasks", 1, None)])
    print(f"{WORKER_ID} consuming $queue/tasks", flush=True)
    client.loop_forever()


if __name__ == "__main__":
    main()
