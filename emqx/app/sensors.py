"""Simulated sensor fleet. Identical for both stacks.

Three sensors publish JSON telemetry once a second to
factory/<line>/<sensor>/telemetry, each from its own MQTT connection
(client id = sensor id). A dispatcher publishes a calibration task to
factory/tasks every 15 seconds.

Plain MQTT 3.1.1 publishers on purpose: producing into EMQX Streams
requires no client change and no MQTT 5.0.
"""

import json
import os
import random
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
LINE = os.environ.get("LINE", "line-1")
SENSORS = ["s1", "s2", "s3"]
TASK_INTERVAL_S = 15


def make_client(client_id: str) -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()
    return client


def main() -> None:
    clients = {sensor: make_client(f"{LINE}-{sensor}") for sensor in SENSORS}
    dispatcher = make_client(f"{LINE}-dispatcher")

    seq = {sensor: 0 for sensor in SENSORS}
    task_seq = 0
    temp = {sensor: 20.0 + i for i, sensor in enumerate(SENSORS)}
    last_task = time.monotonic()

    print(f"publishing to {MQTT_HOST}:{MQTT_PORT} as {len(SENSORS)} sensors", flush=True)
    while True:
        for sensor in SENSORS:
            seq[sensor] += 1
            temp[sensor] += random.uniform(-0.3, 0.3)
            payload = {
                "line": LINE,
                "sensor": sensor,
                "seq": seq[sensor],
                "temp_c": round(temp[sensor], 2),
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            clients[sensor].publish(
                f"factory/{LINE}/{sensor}/telemetry", json.dumps(payload), qos=1
            )

        if time.monotonic() - last_task >= TASK_INTERVAL_S:
            task_seq += 1
            task = {
                "task": "calibrate",
                "sensor": random.choice(SENSORS),
                "seq": task_seq,
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            dispatcher.publish("factory/tasks", json.dumps(task), qos=1)
            print(f"dispatched task #{task_seq}: calibrate {task['sensor']}", flush=True)
            last_task = time.monotonic()

        time.sleep(1)


if __name__ == "__main__":
    main()
