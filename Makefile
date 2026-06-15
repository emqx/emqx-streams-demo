.PHONY: help up-emqx up-nats down reset replay-emqx replay-nats \
        restart-broker-emqx restart-broker-nats smoke-emqx smoke-nats

help:  ## Show this help
	@grep -E '^[a-zA-Z][a-zA-Z0-9_-]*:.*##' $(MAKEFILE_LIST) | \
		sort | awk -F ':.*## ' '{printf "  %-22s %s\n", $$1, $$2}'

up-emqx:  ## Bring up the EMQX stack (broker + streams + queue + sensors + consumers)
	docker compose --profile emqx up -d --build

up-nats:  ## Bring up the NATS stack (mosquitto + bridge + JetStream + sensors + consumers)
	docker compose --profile nats up -d --build

down:  ## Stop everything (volumes preserved)
	docker compose --profile emqx --profile nats --profile emqx-tools --profile nats-tools down

reset:  ## Stop everything and wipe volumes
	docker compose --profile emqx --profile nats --profile emqx-tools --profile nats-tools down -v

replay-emqx:  ## Late joiner: replay full telemetry history from EMQX Streams
	docker compose --profile emqx-tools run --rm replay-emqx \
		python replay.py --from earliest --verify-order

replay-nats:  ## Late joiner: replay full telemetry history from JetStream
	docker compose --profile nats-tools run --rm replay-nats \
		python replay.py --from earliest --verify-order

restart-broker-emqx:  ## Kill and restart EMQX; history survives (durable storage)
	docker compose --profile emqx restart emqx

restart-broker-nats:  ## Kill and restart NATS; history survives (file storage)
	docker compose --profile nats restart nats

smoke-emqx:  ## CI smoke: replay must deliver >=30 ordered messages
	docker compose --profile emqx-tools run --rm replay-emqx \
		python replay.py --from earliest --verify-order --max-messages 30 --timeout 120
	docker compose --profile emqx logs worker-emqx-1 worker-emqx-2 | grep -q "done task"

smoke-nats:  ## CI smoke: replay must deliver >=30 ordered messages
	docker compose --profile nats-tools run --rm replay-nats \
		python replay.py --from earliest --verify-order --max-messages 30 --timeout 120
	docker compose --profile nats logs worker-nats-1 worker-nats-2 | grep -q "done task"
