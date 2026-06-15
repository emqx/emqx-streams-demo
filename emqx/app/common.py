"""Shared MQTT 5.0 connection helper for the EMQX-side consumers.

Consuming EMQX Streams ($stream/...) and Message Queues ($queue/...)
requires an MQTT 5.0 client; producing does not.

Subscriptions are (re)established in on_connect, so consumers survive
a broker restart (paho reconnects, but does not resubscribe by itself).
"""

import os

import paho.mqtt.client as mqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))


def connect_v5(client_id: str, on_message, subscriptions) -> mqtt.Client:
    """subscriptions: list of (topic, qos, properties-or-None)."""

    def on_connect(client, userdata, flags, reason_code, properties):
        for topic, qos, props in subscriptions:
            client.subscribe(topic, qos=qos, properties=props)

    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2, client_id=client_id, protocol=mqtt.MQTTv5
    )
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30, clean_start=True)
    return client


def stream_offset_props(offset: str) -> Properties:
    """Subscription properties selecting the replay start position.

    offset: "earliest", "latest", or a Unix timestamp in microseconds.
    """
    props = Properties(PacketTypes.SUBSCRIBE)
    props.UserProperty = ("stream-offset", offset)
    return props
