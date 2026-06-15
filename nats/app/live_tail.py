"""Live tail on the NATS side: a core NATS subscription on telemetry.>.

Core subscriptions are fire-and-forget (no history); JetStream captures
the same subjects durably on the side.
"""

import asyncio
import json
import os

import nats

NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")


async def main() -> None:
    nc = await nats.connect(NATS_URL)
    sub = await nc.subscribe("telemetry.>")
    print("tailing telemetry.> (core NATS)", flush=True)
    async for msg in sub.messages:
        reading = json.loads(msg.data)
        print(f"[live] {reading['sensor']} seq={reading['seq']} {reading['temp_c']}°C",
              flush=True)


if __name__ == "__main__":
    asyncio.run(main())
