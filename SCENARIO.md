# Scenario spec

Both builds run the same scenario with the same data shape. Each fleet uses
its broker's native client: the MQTT fleet is `emqx/app/sensors.py` (paho), the
NATS fleet is `nats/app/sensors.py` (nats-py). No gateway between them.

## Fleet

- 3 sensors (`s1`, `s2`, `s3`) on `line-1`.
- 1 reading per sensor per second. Payload (identical on both sides):

  ```json
  {"line": "line-1", "sensor": "s1", "seq": 17, "temp_c": 21.4,
   "ts": "2026-06-12T10:00:00+00:00"}
  ```

  `seq` increases by 1 per sensor; consumers use it to verify per-key
  ordering. EMQX publishes to MQTT topic `factory/line-1/<sensor>/telemetry`
  at QoS 1; NATS publishes to subject `telemetry.line-1.<sensor>` with
  `Nats-Msg-Id: <sensor>-<seq>`.

- 1 task every 15 seconds. EMQX: MQTT topic `factory/tasks`. NATS: subject
  `tasks.dispatch`.

  ```json
  {"task": "calibrate", "sensor": "s2", "seq": 4,
   "ts": "2026-06-12T10:00:00+00:00"}
  ```

## Mapping

| Concern | EMQX | NATS |
|---|---|---|
| History | stream `telemetry` from `factory/+/+/telemetry` | stream `TELEMETRY` from `telemetry.<line>.<sensor>` |
| State | last-value stream `state` (key = client id), derived from the same traffic | KV `sensor-state` key `<line>.<sensor>`, written by the producer |
| Tasks | queue `tasks` from `factory/tasks`, consumed via `$queue/tasks` | work-queue stream `TASKS` from `tasks.dispatch` |
| Key / order unit | key expression `message.from` (client id) | subject per sensor |
| Retention | 7 d | 7 d (`max_age`) |
