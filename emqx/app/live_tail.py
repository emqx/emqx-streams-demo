"""Live tail: a plain MQTT subscription, exactly as without Streams.

Streams capture messages on the side; the real-time pub/sub path is
untouched. This consumer would work against any MQTT broker.
"""

import json

from common import connect_v5


def on_message(client, userdata, msg):
    reading = json.loads(msg.payload)
    print(
        f"[live] {reading['sensor']} seq={reading['seq']} {reading['temp_c']}°C",
        flush=True,
    )


def main() -> None:
    client = connect_v5("live-tail", on_message,
                        [("factory/+/+/telemetry", 1, None)])
    print("tailing factory/+/+/telemetry", flush=True)
    client.loop_forever()


if __name__ == "__main__":
    main()
