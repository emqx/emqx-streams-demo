# EMQX MQTT Streams vs NATS JetStream

This repo runs the same small telemetry workload on two broker stacks:

- MQTT sensors -> EMQX, with MQTT Streams, a last-value stream, and EMQX
  Message Queue.
- NATS sensors -> NATS, with JetStream, a KV bucket, and a work-queue stream.

There is no gateway between the two. Each fleet uses its broker's native
client. The point is to show what changes when you add durable history, current
state, and queue workers to a broker-native telemetry path.

| Consumer | What it checks |
|---|---|
| `live-tail` | real-time readings still arrive as normal pub/sub |
| `replay` | a late consumer can read the full history in order |
| `last-value` | a cold-started consumer can get the latest reading per sensor |
| `worker` x2 | each task is handled by one worker |

```
EMQX build                         NATS build

MQTT sensors                       NATS sensors
     |                                  |
   EMQX                               NATS
   |- $stream/telemetry              |- TELEMETRY stream
   |- $stream/state (last-value)     |- sensor-state KV
   '- $queue/tasks                   '- TASKS work queue
```

## Scenario

The workload is deliberately small:

- 3 sensors, `s1` to `s3`, on `line-1`
- 1 telemetry reading per sensor per second
- 1 calibration task every 15 seconds

Telemetry payload:

```json
{"line": "line-1", "sensor": "s1", "seq": 17, "temp_c": 21.4,
 "ts": "2026-06-12T10:00:00+00:00"}
```

`seq` increments per sensor. Replay consumers use it to verify ordering.

EMQX publishes telemetry at QoS 1 to
`factory/line-1/<sensor>/telemetry`. NATS publishes to
`telemetry.line-1.<sensor>` with `Nats-Msg-Id: <sensor>-<seq>`.

Task payload:

```json
{"task": "calibrate", "sensor": "s2", "seq": 4,
 "ts": "2026-06-12T10:00:00+00:00"}
```

EMQX publishes tasks to `factory/tasks`. NATS publishes tasks to
`tasks.dispatch`.

## Run it

Prerequisite: Docker with Compose. The EMQX and NATS stacks use different
ports, so you can run one or both.

```bash
make up-emqx        # MQTT sensors -> EMQX (streams + queue) + consumers
make up-nats        # NATS sensors -> JetStream (streams + work queue + KV) + consumers
```

Watch the consumers:

```bash
docker compose logs -f live-tail-emqx last-value-emqx worker-emqx-1 worker-emqx-2
docker compose logs -f live-tail-nats last-value-nats worker-nats-1 worker-nats-2
```

### What you could try

1. Live tail: `docker compose logs -f live-tail-emqx`

   The live path is still ordinary pub/sub. Streams capture the same traffic
   without changing the producer.

2. Replay: wait a minute, then run `make replay-emqx`

   The replay consumer starts from the beginning, verifies per-sensor order,
   and then keeps tailing live traffic. The NATS equivalent is
   `make replay-nats`.

3. Current state: `docker compose logs -f last-value-emqx`

   A cold-started dashboard gets one current reading per sensor. EMQX serves
   this from a last-value stream; NATS serves it from the `sensor-state` KV
   bucket.

4. Work queue: `docker compose logs -f worker-emqx-1 worker-emqx-2`

   Each task is delivered to one worker. EMQX uses `$queue/tasks`; NATS uses a
   shared durable pull consumer on the `TASKS` stream.

5. Broker restart: `make restart-broker-emqx`, then replay again

   History survives the restart. Use `make restart-broker-nats` for the NATS
   side.

`make help` lists the other commands.

## Mapping

| Concern | EMQX | NATS |
|---|---|---|
| History / replay | stream `telemetry` from `factory/+/+/telemetry` | stream `TELEMETRY` from `telemetry.<line>.<sensor>` |
| Current state | last-value stream `state`, derived from the telemetry topic | KV bucket `sensor-state`, written by the producer |
| Work queue | Message Queue `tasks` from `factory/tasks`, read via `$queue/tasks` | stream `TASKS` from `tasks.dispatch`, shared durable pull consumer |
| Order unit | stream key expression, here the MQTT client id | subject per sensor |
| Retention | configured as 7 days | configured as 7 days (`max_age`) |
| Produce | ordinary MQTT publish, any MQTT version | NATS publish with `Nats-Msg-Id` for deduplication |
| Consume stream history | MQTT 5.0 subscribe with a stream offset property | NATS JetStream consumer |

## Replay code

Full code lives in `emqx/app/replay.py` and `nats/app/replay.py`. These
snippets remove imports and argument parsing.

EMQX, MQTT 5.0:

```python
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
client.on_message = lambda c, u, msg: print(json.loads(msg.payload))
client.connect("emqx", 1883)

# replay from the start: one MQTT 5 subscription property
props = Properties(PacketTypes.SUBSCRIBE)
props.UserProperty = ("stream-offset", "earliest")
client.subscribe("$stream/telemetry", qos=1, properties=props)
client.loop_forever()
```

NATS, JetStream:

```python
nc = await nats.connect("nats://nats:4222")
js = nc.jetstream()

# replay from the start: an ordered consumer over the stream
sub = await js.subscribe("telemetry.>", stream="TELEMETRY",
                         ordered_consumer=True, deliver_policy=DeliverPolicy.ALL)
async for msg in sub.messages:
    print(json.loads(msg.data))
```

Both consumers read history first and then keep tailing. The queue workers look
different because the NATS version fetches and acknowledges messages explicitly.

EMQX, Message Queue:

```python
client.on_message = lambda c, u, msg: handle(json.loads(msg.payload))
client.subscribe("$queue/tasks", qos=1)   # run N replicas; each task to one
client.loop_forever()
```

NATS, work-queue stream:

```python
sub = await js.pull_subscribe("tasks.>", durable="workers", stream="TASKS")
while True:
    for msg in await sub.fetch(1, timeout=5):
        handle(json.loads(msg.data))
        await msg.ack()
```

## Differences

| Area | Difference |
|---|---|
| Consumer position | JetStream stores named durable consumer offsets on the server. EMQX stream consumers choose a start offset and track exact resume state themselves if needed. |
| Exactly once | MQTT QoS 2 is per-hop delivery. JetStream deduplication is producer-id based, with normal consumer ack handling. |
| Device behavior | MQTT has last will, retained messages, and persistent client sessions as protocol features. NATS can model similar outcomes, but not as device-client protocol semantics. |
| Endpoint protocol | MQTT favors existing device fleets and fixed firmware. NATS fits best when you control the endpoints and can use a NATS client. |
| Portability | MQTT has many broker and client implementations. JetStream is tied to `nats-server`; both streaming features are implementation-specific. |
| Licensing | NATS is Apache-2.0. EMQX 5.9+ is BSL 1.1; the Enterprise image includes a single-node Community License, with a Commercial License needed for full commercial or clustered use. |

## Which one to start with

Start with EMQX MQTT Streams when the telemetry path is MQTT. You keep device
ingest, replayable history, latest-value state, offline-session behavior, and
broker-side task dispatch in the same MQTT system, without adding a bridge or
changing device clients.

Start with NATS JetStream when you control the endpoints, can use NATS clients
directly, and want JetStream durable consumers plus NATS KV, object storage, and
request-reply in the same system.

## Layout

```
docker-compose.yml        both stacks, compose profiles: emqx | nats
emqx/base.hocon           streams + message queues enabled, api key bootstrap
emqx/provision.sh         REST: telemetry stream, state stream, task queue
emqx/app/                 MQTT fleet + consumers (paho): sensors, live_tail, replay, last_value, worker
nats/app/provision.py     JetStream: TELEMETRY, TASKS (workqueue), KV bucket
nats/app/sensors.py       NATS fleet simulator (nats-py)
nats/app/{live_tail,replay,last_value,worker}.py   native NATS consumers
```

## License

Apache-2.0. EMQX Enterprise and NATS are products of their respective owners;
this repository only orchestrates official container images.
