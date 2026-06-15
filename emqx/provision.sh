#!/bin/sh
# Provision the EMQX side: two streams and one message queue, via REST.
# Idempotent: a 400 "already exists" on re-run is fine.
set -u

AUTH="$EMQX_KEY:$EMQX_SECRET"

echo "waiting for EMQX API..."
until curl -sf -u "$AUTH" "$EMQX_API/status" >/dev/null 2>&1; do
  sleep 2
done

post() {
  path="$1"; body="$2"; label="$3"
  code=$(curl -s -o /tmp/resp -w '%{http_code}' -u "$AUTH" \
    -X POST -H 'Content-Type: application/json' "$EMQX_API$path" -d "$body")
  if [ "$code" -ge 200 ] && [ "$code" -lt 300 ]; then
    echo "created $label"
  elif grep -qi "already_exists" /tmp/resp; then
    echo "$label already exists"
  else
    echo "FAILED ($code) creating $label: $(cat /tmp/resp)"; exit 1
  fi
}

# Append-only stream: every telemetry message, replayable, 7d retention.
# key_expression message.from = the publishing client id, so ordering is
# guaranteed per sensor.
post /message_streams/streams '{
  "name": "telemetry",
  "topic_filter": "factory/+/+/telemetry",
  "is_lastvalue": false,
  "key_expression": "message.from",
  "data_retention_period": "7d"
}' "stream telemetry (append-only)"

# Last-value stream: current state per sensor (compaction by key).
post /message_streams/streams '{
  "name": "state",
  "topic_filter": "factory/+/+/telemetry",
  "is_lastvalue": true,
  "key_expression": "message.from"
}' "stream state (last-value)"

# Message queue: competing consumers on dispatched tasks.
post /queues '{
  "name": "tasks",
  "topic_filter": "factory/tasks",
  "is_lastvalue": false
}' "message queue factory/tasks"

echo "provisioning complete"
