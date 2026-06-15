"""Current state per sensor, from the last-value stream.

Subscribes to $stream/state from earliest. A last-value stream keeps
only the newest message per key (key = publishing client id), so
"earliest" delivers the current state of every sensor immediately,
then live updates follow. A dashboard restarting cold gets the full
fleet state in one subscription, no retained-message or external-store
plumbing.
"""

import json

from common import connect_v5, stream_offset_props

state: dict[str, dict] = {}


def on_message(client, userdata, msg):
    reading = json.loads(msg.payload)
    state[reading["sensor"]] = reading
    snapshot = "  ".join(
        f"{sensor}={entry['temp_c']}°C(seq {entry['seq']})"
        for sensor, entry in sorted(state.items())
    )
    print(f"[state] {snapshot}", flush=True)


def main() -> None:
    client = connect_v5("last-value", on_message,
                        [("$stream/state", 1, stream_offset_props("earliest"))])
    print("current state from $stream/state (last-value)", flush=True)
    client.loop_forever()


if __name__ == "__main__":
    main()
