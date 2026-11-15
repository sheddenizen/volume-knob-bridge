#!/usr/bin/env python3
"""Forward a rotary encoder's MQTT position to a Home Assistant media_player's volume.

Replaces the old bar script, which called the GLM USB adapter's `genlc` CLI directly. Now that
espgensam owns the RS-485 bus, volume goes through Home Assistant's `media_player.volume_set`
instead, via ha_ws.py -- the same WebSocket client tools/cec_audio_bridge uses, chosen for the
same reason: a held knob can emit a new position every few hundred milliseconds, faster than a
round trip through HA and the bus, and ha_ws.py already coalesces that to one call in flight.

The knob's 0..1000 position maps straight onto the media_player's 0.0..1.0 volume_level; the
dB range it actually corresponds to is whatever espgensam.yaml configures, not this script's
concern.

USAGE

    HA_TOKEN=... python3 bridge.py \\
        --ha-url ws://host.docker.internal:8123/api/websocket \\
        --entity media_player.genelec_sam_system

The token is a Home Assistant long-lived access token. Pass it in the HA_TOKEN environment
variable or with --token-file, never on the command line, where other users can see it.
"""

import argparse
import asyncio
import json
import logging
import os
import pathlib
import signal
import sys

try:
    from ha_ws import HomeAssistant
except ImportError as e:
    sys.exit(f"{e}\nThis needs python3-websockets 13 or newer: pip install 'websockets>=13'")

import paho.mqtt.client as mqtt

log = logging.getLogger("volume_knob_bridge")

KNOB_MAX = 1000


class Bridge:
    def __init__(self, args, token):
        self.args = args
        self.player = HomeAssistant(args.ha_url, token, args.entity)
        self.player.on_change = self.on_player_changed
        self.loop: asyncio.AbstractEventLoop | None = None
        self.pending_set_point: int | None = None

        self.mqtt = mqtt.Client()
        self.mqtt.on_connect = self.on_mqtt_connect
        self.mqtt.on_message = self.on_mqtt_message

    # --- MQTT side (runs on paho's own network thread) ---

    def on_mqtt_connect(self, client, userdata, flags, rc):
        log.info("Connected to MQTT broker %s:%d, rc=%s", self.args.mqtt_host,
                  self.args.mqtt_port, rc)
        client.subscribe(self.args.control_topic)

    def on_mqtt_message(self, client, userdata, msg):
        try:
            position = int(msg.payload.decode())
        except (UnicodeDecodeError, ValueError):
            log.warning("ignoring non-integer payload on %s: %r", msg.topic, msg.payload)
            return
        position = min(max(position, 0), KNOB_MAX)
        log.info("knob -> %d", position)
        self.loop.call_soon_threadsafe(self._apply, position)

    def _apply(self, position: int):
        self.pending_set_point = position
        self.player.set_level(position / KNOB_MAX)

    # --- Home Assistant side ---

    def on_player_changed(self):
        level = self.player.level
        if level is None:
            return
        pos = round(level * KNOB_MAX)
        set_point = self.pending_set_point if self.pending_set_point is not None else pos
        payload = json.dumps({"setPoint": set_point, "pos": pos})
        self.mqtt.publish(self.args.state_topic, payload, qos=1, retain=True)
        log.debug("state -> %s: %s", self.args.state_topic, payload)

    # --- Run ---

    async def run(self):
        self.loop = asyncio.get_running_loop()
        stop = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.loop.add_signal_handler(sig, stop.set)

        self.mqtt.connect(self.args.mqtt_host, self.args.mqtt_port, 60)
        self.mqtt.loop_start()
        task = asyncio.create_task(self.player.run())
        try:
            await stop.wait()
        finally:
            task.cancel()
            self.mqtt.loop_stop()
            self.mqtt.disconnect()


def read_token(args):
    if args.token_file:
        try:
            return pathlib.Path(args.token_file).read_text().strip()
        except OSError as e:
            sys.exit(f"Cannot read the token file: {e}")
    token = os.environ.get("HA_TOKEN", "").strip()
    if not token:
        sys.exit("No Home Assistant token: set HA_TOKEN or use --token-file")
    return token


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mqtt-host", default=os.environ.get("MQTT_HOST", "iothub"))
    ap.add_argument("--mqtt-port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    ap.add_argument("--control-topic", default=os.environ.get(
        "CONTROL_TOPIC", "home/controller/volumecontrol/volume_ctl_stepper/bar/control"))
    ap.add_argument("--state-topic", default=os.environ.get(
        "STATE_TOPIC", "home/controller/volumecontrol/volume_ctl_stepper/bar/state"))
    ap.add_argument("--ha-url", default=os.environ.get(
        "HA_URL", "ws://host.docker.internal:8123/api/websocket"),
        help="Home Assistant WebSocket API URL (default %(default)s)")
    ap.add_argument("--token-file", default=os.environ.get("HA_TOKEN_FILE"),
                    help="file holding a long-lived access token "
                         "(default: the HA_TOKEN environment variable)")
    ap.add_argument("--entity", default=os.environ.get("HA_ENTITY",
                    "media_player.genelec_sam_system"),
                    help="media_player entity to control (default %(default)s)")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    stamp = "%(asctime)s " if sys.stderr.isatty() else ""  # docker/journald add their own
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format=stamp + "%(levelname)s %(name)s: %(message)s")
    logging.getLogger("websockets").setLevel(logging.INFO)

    token = read_token(args)
    asyncio.run(Bridge(args, token).run())


if __name__ == "__main__":
    main()
