# Durable streams for device telemetry: EMQX MQTT Streams vs NATS JetStream

The same IoT scenario built twice, once on each system, with each
fleet speaking its broker's native protocol. A small fleet of sensors
publishes telemetry, and four consumers have different needs:

| Consumer | Needs |
|---|---|
| `live-tail` | real-time readings as they happen |
| `replay` | joins late, replays the full history in order |
| `last-value` | current state of every sensor on a cold start |
| `worker` x2 | each dispatched task processed by exactly one worker |

Both systems do all four. The two stacks are symmetric (one broker and a
sensor fleet each), so the comparison is about features and fit.

```
EMQX build                         NATS build

MQTT sensors                       NATS sensors
     |                                  |
   EMQX                               NATS
   |- $stream/telemetry              |- TELEMETRY stream
   |- $stream/state (last-value)     |- sensor-state KV
   '- $queue/tasks                   '- TASKS work queue
```

## Run it

Prerequisites: Docker with compose. Each stack runs standalone; run one or
both (ports do not clash).

```bash
make up-emqx        # MQTT sensors -> EMQX (streams + queue) + consumers
make up-nats        # NATS sensors -> JetStream (streams + work queue + KV) + consumers
```

Watch the consumers:

```bash
docker compose logs -f live-tail-emqx last-value-emqx worker-emqx-1 worker-emqx-2
docker compose logs -f live-tail-nats last-value-nats worker-nats-1 worker-nats-2
```

### The 15-minute path

1. Live tail: `docker compose logs -f live-tail-emqx`. The real-time path is
   ordinary pub/sub; streams capture on the side without changing it.
2. Late joiner: wait a minute, then `make replay-emqx`. A consumer that was
   not there gets the full history in per-sensor order, then keeps tailing.
   On EMQX this is one MQTT 5.0 subscription to `$stream/telemetry` with a
   `stream-offset: earliest` property; on NATS (`make replay-nats`) it is an
   ordered JetStream consumer with `DeliverPolicy: all`.
3. Current state: `docker compose logs -f last-value-emqx`. A cold-started
   dashboard sees the latest reading of every sensor at once. EMQX serves it
   from a last-value stream; NATS from a KV bucket.
4. Work queue: `docker compose logs -f worker-emqx-1 worker-emqx-2`. Each
   task lands on exactly one worker. EMQX uses a Message Queue
   (`$queue/tasks`); NATS uses a work-queue stream with a shared durable pull
   consumer.
5. Durability: `make restart-broker-emqx` (or `-nats`), then replay again.
   History survives the restart. EMQX persists to Durable Storage (RocksDB),
   NATS to JetStream file storage.

`make help` lists everything.

## How each one builds it

| Concern | EMQX | NATS |
|---|---|---|
| History / replay | append-only stream `telemetry` | `TELEMETRY` stream |
| Current state | last-value stream `state` (derived from the same topic) | KV bucket `sensor-state` (the producer writes it) |
| Work queue | Message Queue `tasks`, read via `$queue/tasks` | work-queue stream `TASKS`, shared durable pull consumer |
| Order unit | key expression (here the client id) | subject per sensor |
| Produce | MQTT publish, any version, no client change | NATS publish, `Nats-Msg-Id` for dedup |
| Consume a stream | MQTT 5.0 subscription with an offset property | NATS client |

## Consuming a stream, side by side

Replaying the telemetry stream from the beginning takes about the same amount of code either way. Trimmed from the repo (`emqx/app/replay.py` and `nats/app/replay.py`; imports and arg-parsing removed):

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

Each gets the full history first, then keeps tailing live. The work queue is
the one place the shapes differ: on EMQX you subscribe and the broker
load-balances; on NATS you fetch and acknowledge in a loop, which is also what
gives you the explicit at-least-once redelivery.

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

## What actually differs

On the core job the two are close. Each replays history in order,
load-balances a work queue, and serves the latest value per sensor (MQTT
retained messages and last-value streams on one side, a KV bucket or a
last-per-subject stream on the other). Where they diverge:

- Durable consumers. JetStream keeps a named consumer's position on the
  server, so a crashed consumer reattaches and resumes exactly where it
  stopped. An EMQX stream consumer picks a start point and tracks its own
  position to resume. For a long-lived pipeline that must never miss or
  reprocess, JetStream's model is less to get right.
- Exactly-once, two meanings. MQTT QoS 2 is a transport guarantee: the broker
  runs a handshake so a message crosses each hop exactly once, both inbound
  from the producer and outbound to a consumer, with no application code. It
  does not collapse logical duplicates (two publishes of the same reading).
  JetStream is the mirror image: no per-hop handshake, but a producer stamps a
  `Nats-Msg-Id` and the server drops a repeat within a window, and a consumer
  double-acks so a lost ack does not cause reprocessing. MQTT gives exactly-
  once transmission for free; JetStream gives exactly-once processing when the
  producer sets ids and the consumer cooperates.
- The device features around the stream. This demo does not exercise them, but
  they are a main reason to keep a fleet on MQTT. A last-will message lets the
  broker announce a device that drops off, so the system learns it is gone
  without its own timeout logic. A retained message gives a dashboard or a
  replacement unit the current state the instant it subscribes. A per-client
  session is a durable mailbox per device, so commands sent while it was
  offline arrive on reconnect. EMQX has all three as an MQTT broker; native
  NATS has none as protocol features (presence from connection events, state
  from a KV bucket, per-device durability from a consumer model not built
  around a client identity). NATS does match MQTT on the basics (wildcards,
  load-balanced subscriptions, headers, per-message TTL since 2.11) and its
  request-reply is more first-class; the gap is these device features.
- The protocol the endpoints speak. This is usually what decides it, and the
  demo does not show it, because each fleet uses its native client. The
  device and OT installed base is overwhelmingly MQTT, much of it third-party
  or fixed-firmware hardware whose protocol you cannot change. If your
  endpoints are or must be MQTT, EMQX puts durable streams natively where the
  devices already are, alongside the rest of the MQTT and OT stack (rule
  engine, Neuron). When you do control the endpoints and they can run a NATS
  client, that ecosystem advantage falls away and it becomes a feature call,
  where NATS's breadth is a real draw.
- Open standard versus single implementation. MQTT is an OASIS and
  ISO/IEC 20922 standard with many interoperable brokers and clients, so you
  can change brokers without re-touching the fleet. The NATS protocol has one
  reference implementation (CNCF nats-server, Apache-2.0); you have the
  source, but there is no alternative server to move to. In fairness the
  streaming feature is proprietary on both sides (MQTT Streams to EMQX,
  JetStream to NATS); the portability is at the device layer, not the stream.
- Licensing and footprint. NATS is Apache-2.0 and a single ~20 MB Go binary.
  EMQX is a heavier broker on the Erlang runtime, free for single-node and
  non-commercial use (this demo uses the built-in license, 25 sessions), with
  a license required for clustering or commercial production.

Beyond streaming, each is also a broader platform: NATS adds a key-value store, an
object store, and request-reply; EMQX adds a SQL rule engine and data bridges
to Kafka and databases. A native MQTT KV store is on the EMQX 7.0 roadmap
(~Sept 2026); an object store is not announced.

Producing into an EMQX stream works from any MQTT client unchanged; consuming
a stream requires MQTT 5.0 (the offset rides on a v5 subscription property).
Ordering is per key on both sides as configured here (per sensor), and the
replay consumers verify it with `--verify-order`.

## Choosing

Choose NATS JetStream when your endpoints speak NATS or you build them to, or
when you want an Apache-2.0 system that is also a key-value, object, and
request-reply platform.

Choose EMQX MQTT Streams when your fleet is MQTT, as most IoT and OT fleets
are or must be, and you want durable streams together with the rest of the
MQTT and OT stack in one system.

## Layout

```
docker-compose.yml        both stacks, compose profiles: emqx | nats
SCENARIO.md               the shared scenario both stacks implement
emqx/base.hocon           streams + message queues enabled, api key bootstrap
emqx/provision.sh         REST: telemetry stream, state stream, task queue
emqx/app/                 MQTT fleet + consumers (paho): sensors, live_tail, replay, last_value, worker
nats/app/provision.py     JetStream: TELEMETRY, TASKS (workqueue), KV bucket
nats/app/sensors.py       NATS fleet simulator (nats-py)
nats/app/{live_tail,replay,last_value,worker}.py   native NATS consumers
```

## License

Apache-2.0. EMQX Enterprise and NATS are products of their respective
owners; this repository only orchestrates official container images.
