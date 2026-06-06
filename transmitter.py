#!/usr/bin/env python3
"""
WakeMyPC software transmitter (Docker / Python).

A drop-in replacement for the Raspberry Pi Pico W hardware transmitter: it
speaks the exact same WebSocket protocol to the WakeMyPC backend, so the
server (and dashboard) treat it like any other transmitter. Run it in a
container on the same LAN as the PCs you want to wake / shut down.

What it does:
  * Connects to  ws(s)://<server>/ws/pico/<UNIQUE_ID>/  and authenticates with
    the device TOKEN (first message).
  * Sends a heartbeat every HEARTBEAT_INTERVAL seconds (keeps it "online").
  * Wake-on-LAN: on a `wol` command, broadcasts the magic packet on the LAN.
  * SSH/TCP relay: on `tcp_relay_open`, opens a raw TCP socket to the target on
    the LAN and pipes bytes both ways (base64 over the WebSocket) so the server
    can run SSH against the target -- the SSH key never leaves the server.
  * Device status: periodically TCP-probes the assigned devices and reports
    online/offline back to the dashboard.

Why `network_mode: host` is required (see docker-compose.yml):
  WOL needs a real LAN UDP broadcast, and the relay/probes need to reach other
  hosts on the LAN by IP. A bridged container network can't do either.

Configuration (environment variables):
  SERVER_URL            Base server URL, e.g. wss://wakemypc.com
                        (or ws://192.168.1.10:8000 for local dev). Required.
  UNIQUE_ID             The transmitter's unique_id as registered on the
                        dashboard. Goes in the WebSocket path. Required.
  TOKEN                 The transmitter's device token (from the dashboard,
                        "rotate token"). Required.
  FIRMWARE_VERSION      Reported version string. Default "docker-1.0.0".
  HEARTBEAT_INTERVAL    Seconds between heartbeats. Default 30.
  DEVICE_SCAN_INTERVAL  Seconds between device-status sweeps. Default 60.
  INSECURE_SKIP_TLS_VERIFY  "1" to skip TLS verification (dev only). Default off.
  LOG_LEVEL             DEBUG / INFO / WARNING. Default INFO.
"""

import asyncio
import base64
import json
import logging
import os
import socket
import ssl
import time

import websockets

# --- Configuration -----------------------------------------------------------


def _require(name: str) -> str:
    val = os.environ.get(name, "").strip()
    if not val:
        raise SystemExit(f"Missing required environment variable: {name}")
    return val


SERVER_URL = _require("SERVER_URL").rstrip("/")
UNIQUE_ID = _require("UNIQUE_ID")
TOKEN = _require("TOKEN")
FIRMWARE_VERSION = os.environ.get("FIRMWARE_VERSION", "docker-1.0.0")
HEARTBEAT_INTERVAL = int(os.environ.get("HEARTBEAT_INTERVAL", "30"))
DEVICE_SCAN_INTERVAL = int(os.environ.get("DEVICE_SCAN_INTERVAL", "60"))
INSECURE_SKIP_TLS_VERIFY = os.environ.get("INSECURE_SKIP_TLS_VERIFY", "") in ("1", "true", "True")

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("transmitter")

WS_URL = f"{SERVER_URL}/ws/pico/{UNIQUE_ID}/"
WOL_PORT = 9
RELAY_BUF = 1024
MAX_SESSIONS = 8


class AuthFailed(Exception):
    """Raised when the backend rejects our token (don't hammer-reconnect)."""


# --- Helpers ------------------------------------------------------------------


def local_ip() -> str:
    """Best-effort LAN IP of this host (the UDP-connect trick needs no traffic)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except Exception:
        return "0.0.0.0"
    finally:
        s.close()


def build_magic_packet(mac: str) -> bytes:
    clean = mac.replace(":", "").replace("-", "").replace(".", "").strip()
    if len(clean) != 12:
        raise ValueError(f"invalid MAC address: {mac!r}")
    return bytes.fromhex("ff" * 6 + clean * 16)


def _ssl_context():
    if not WS_URL.startswith("wss://"):
        return None
    ctx = ssl.create_default_context()
    if INSECURE_SKIP_TLS_VERIFY:
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    return ctx


# --- Transmitter --------------------------------------------------------------


class Transmitter:
    def __init__(self):
        self.ws = None
        self.assigned = []          # [{public_id, name, mac, ip}, ...] from auth_ok
        self.relays = {}            # session_id -> (reader, writer)
        self.messages_handled = 0
        self.boot = time.monotonic()
        self._send_lock = asyncio.Lock()

    # -- transport --
    async def send(self, obj: dict):
        # One lock so concurrent relay/heartbeat tasks don't interleave frames.
        async with self._send_lock:
            await self.ws.send(json.dumps(obj))

    async def run_forever(self):
        backoff = 1
        while True:
            try:
                log.info("connecting to %s", WS_URL)
                async with websockets.connect(
                    WS_URL,
                    ssl=_ssl_context(),
                    ping_interval=None,   # app-level heartbeat instead (proxies dislike client pings)
                    max_size=None,
                    open_timeout=20,
                ) as ws:
                    self.ws = ws
                    await self.authenticate()
                    backoff = 1
                    await self.session()
            except AuthFailed as e:
                log.error("authentication failed (%s) -- retrying in 300s", e)
                await asyncio.sleep(300)
            except Exception as e:  # noqa: BLE001 -- log + reconnect with backoff
                log.warning("connection lost (%s) -- reconnecting in %ss", e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            finally:
                await self._close_all_relays()
                self.ws = None

    async def authenticate(self):
        await self.send(
            {
                "type": "auth",
                "token": TOKEN,
                "device_id": UNIQUE_ID,
                "hardware_id": UNIQUE_ID,
                "firmware_version": FIRMWARE_VERSION,
                "ip": local_ip(),
            }
        )
        raw = await asyncio.wait_for(self.ws.recv(), timeout=20)
        msg = json.loads(raw)
        if msg.get("type") == "auth_ok":
            self.assigned = msg.get("assigned_devices", []) or []
            log.info("authenticated; %d device(s) assigned", len(self.assigned))
        elif msg.get("type") == "auth_fail":
            raise AuthFailed(msg.get("reason", "unknown"))
        else:
            raise AuthFailed(f"unexpected first reply: {msg.get('type')!r}")

    async def session(self):
        tasks = [
            asyncio.create_task(self._recv_loop(), name="recv"),
            asyncio.create_task(self._heartbeat_loop(), name="heartbeat"),
            asyncio.create_task(self._status_loop(), name="status"),
        ]
        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in pending:
                t.cancel()
            for t in done:
                exc = t.exception()
                if exc:
                    raise exc
        finally:
            for t in tasks:
                t.cancel()

    # -- loops --
    async def _recv_loop(self):
        async for raw in self.ws:
            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                log.debug("ignoring non-JSON frame")
                continue
            self.messages_handled += 1
            try:
                await self._dispatch(msg)
            except Exception:  # noqa: BLE001 -- one bad command shouldn't drop the link
                log.exception("handler error for %s", msg.get("type"))

    async def _heartbeat_loop(self):
        while True:
            await self._send_heartbeat()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _status_loop(self):
        # Small initial delay so auth/assignment settles first.
        await asyncio.sleep(2)
        while True:
            await self._report_device_status()
            await asyncio.sleep(DEVICE_SCAN_INTERVAL)

    # -- dispatch --
    async def _dispatch(self, msg: dict):
        t = msg.get("type")
        if t == "wol":
            await self._handle_wol(msg)
        elif t == "tcp_relay_open":
            await self._handle_relay_open(msg)
        elif t == "tcp_relay_data":
            await self._handle_relay_data(msg)
        elif t == "tcp_relay_close":
            await self._handle_relay_close(msg)
        elif t == "ping":
            await self.send({"type": "pong"})
        elif t == "pong":
            pass
        elif t == "request_heartbeat":
            await self._send_heartbeat()
        elif t == "get_status":
            await self._send_status()
        elif t == "scan":
            await self._handle_scan(msg)
        elif t == "identify":
            log.info("identify requested (no LED on a soft transmitter)")
            await self.send({"type": "identify_ack"})
        elif t == "reboot":
            log.info("reboot requested -- exiting so the container restarts")
            await self.send({"type": "reboot_ack", "message": "restarting container"})
            await asyncio.sleep(0.5)
            os._exit(0)
        elif t in ("device_assignment", "auth_ok"):
            devs = msg.get("devices") or msg.get("assigned_devices")
            if devs is not None:
                self.assigned = devs
                log.info("device assignment updated: %d device(s)", len(self.assigned))
        elif t in ("config_update", "wifi_config_get", "wifi_config_set",
                   "ota_update", "get_versions", "firmware_update_available"):
            # Firmware/WiFi/OTA concepts don't apply to a containerized
            # transmitter -- acknowledge softly where a reply is expected.
            log.debug("ignoring firmware-only message: %s", t)
        else:
            log.debug("unhandled message type: %s", t)

    # -- Wake-on-LAN --
    async def _handle_wol(self, msg: dict):
        mac = msg.get("mac", "")
        request_id = msg.get("request_id")
        broadcast = msg.get("broadcast") or "255.255.255.255"
        count = max(1, min(int(msg.get("count", 1) or 1), 10))
        try:
            packet = build_magic_packet(mac)
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            try:
                for _ in range(count):
                    sock.sendto(packet, (broadcast, WOL_PORT))
                    await asyncio.sleep(0.1)
            finally:
                sock.close()
            log.info("WOL sent to %s x%d (broadcast %s)", mac, count, broadcast)
            await self.send({
                "type": "wol_result", "request_id": request_id, "success": True,
                "mac": mac, "packets_sent": count,
            })
        except Exception as e:  # noqa: BLE001
            log.warning("WOL failed for %s: %s", mac, e)
            await self.send({
                "type": "wol_result", "request_id": request_id, "success": False,
                "message": str(e), "mac": mac, "packets_sent": 0,
            })

    # -- SSH / TCP relay --
    async def _handle_relay_open(self, msg: dict):
        sid = msg.get("session_id")
        host = msg.get("host")
        port = int(msg.get("port", 22))
        if len(self.relays) >= MAX_SESSIONS:
            await self.send({"type": "tcp_relay_opened", "session_id": sid,
                             "success": False, "host": host, "port": port,
                             "message": "too many sessions"})
            return
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=10
            )
        except Exception as e:  # noqa: BLE001
            log.warning("relay open %s:%s failed: %s", host, port, e)
            await self.send({"type": "tcp_relay_opened", "session_id": sid,
                             "success": False, "host": host, "port": port,
                             "message": str(e)})
            return
        self.relays[sid] = (reader, writer)
        await self.send({"type": "tcp_relay_opened", "session_id": sid,
                         "success": True, "host": host, "port": port})
        log.info("relay %s opened -> %s:%s", sid, host, port)
        asyncio.create_task(self._pump_socket_to_ws(sid, reader))

    async def _pump_socket_to_ws(self, sid: str, reader: asyncio.StreamReader):
        reason = "target_closed"
        try:
            while True:
                data = await reader.read(RELAY_BUF)
                if not data:
                    break
                await self.send({"type": "tcp_relay_data", "session_id": sid,
                                 "data": base64.b64encode(data).decode("ascii")})
        except Exception as e:  # noqa: BLE001
            reason = f"error:{e}"
        finally:
            if sid in self.relays:
                await self.send({"type": "tcp_relay_closed", "session_id": sid, "reason": reason})
                await self._close_relay(sid)

    async def _handle_relay_data(self, msg: dict):
        sid = msg.get("session_id")
        pair = self.relays.get(sid)
        if not pair:
            return
        _, writer = pair
        try:
            writer.write(base64.b64decode(msg.get("data", "")))
            await writer.drain()
        except Exception as e:  # noqa: BLE001
            await self.send({"type": "tcp_relay_closed", "session_id": sid,
                             "reason": f"send_failed:{e}"})
            await self._close_relay(sid)

    async def _handle_relay_close(self, msg: dict):
        sid = msg.get("session_id")
        await self._close_relay(sid)
        await self.send({"type": "tcp_relay_closed", "session_id": sid, "reason": "requested"})

    async def _close_relay(self, sid: str):
        pair = self.relays.pop(sid, None)
        if not pair:
            return
        _, writer = pair
        try:
            writer.close()
        except Exception:  # noqa: BLE001
            pass

    async def _close_all_relays(self):
        for sid in list(self.relays):
            await self._close_relay(sid)

    # -- status / heartbeat --
    async def _send_heartbeat(self):
        uptime_ms = int((time.monotonic() - self.boot) * 1000)
        await self.send({
            "type": "heartbeat",
            "device_id": UNIQUE_ID,
            "timestamp": int(time.time() * 1000),
            "uptime_ms": uptime_ms,
            "mem_free": 0,
            "mem_alloc": 0,
            "mem_total": 0,
            "messages_handled": self.messages_handled,
            "wifi": {"ssid": "docker", "ip": local_ip(), "rssi": 0},
            "health": {"uptime_seconds": uptime_ms // 1000, "reconnect_count": 0},
        })

    async def _send_status(self):
        uptime_ms = int((time.monotonic() - self.boot) * 1000)
        await self.send({
            "type": "status",
            "device_id": UNIQUE_ID,
            "hardware_id": UNIQUE_ID,
            "uptime_ms": uptime_ms,
            "mem_free": 0, "mem_alloc": 0, "mem_pct_used": 0.0,
            "flash_free": 0, "flash_total": 0, "flash_pct_used": 0.0,
            "messages_handled": self.messages_handled,
            "firmware_version": FIRMWARE_VERSION,
        })

    async def _handle_scan(self, msg: dict):
        targets = msg.get("targets", []) or []
        results = await asyncio.gather(*(self._probe_target(t) for t in targets))
        online = sum(1 for r in results if r["online"])
        await self.send({
            "type": "scan_result", "devices": results,
            "total": len(results), "online": online, "offline": len(results) - online,
        })

    async def _report_device_status(self):
        if not self.assigned:
            return
        results = await asyncio.gather(*(self._probe_target(d) for d in self.assigned))
        await self.send({"type": "device_status", "devices": results})

    async def _probe_target(self, dev: dict) -> dict:
        ip = dev.get("ip")
        online, rt_ms, open_port = False, None, None
        if ip:
            online, rt_ms, open_port = await probe_host(ip)
        return {
            "public_id": dev.get("public_id"),
            "online": online,
            "ip": ip,
            "response_time_ms": rt_ms,
            "port": open_port,
            "name": dev.get("name"),
            "mac": dev.get("mac"),
        }


async def probe_host(ip: str, ports=(22, 80, 3389, 445), timeout=1.0):
    """TCP-probe a host. A successful connect OR a refusal both mean the host
    is up (refused = host reachable, port closed)."""
    for port in ports:
        start = time.monotonic()
        try:
            fut = asyncio.open_connection(ip, port)
            _, writer = await asyncio.wait_for(fut, timeout=timeout)
            writer.close()
            return True, int((time.monotonic() - start) * 1000), port
        except (ConnectionRefusedError, OSError) as e:
            if isinstance(e, ConnectionRefusedError) or "refused" in str(e).lower():
                return True, int((time.monotonic() - start) * 1000), None
            continue
        except asyncio.TimeoutError:
            continue
    return False, None, None


async def main():
    log.info("WakeMyPC transmitter %s starting (unique_id=%s)", FIRMWARE_VERSION, UNIQUE_ID)
    await Transmitter().run_forever()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
