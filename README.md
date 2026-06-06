# WakeMyPC — Docker (software) transmitter

A containerized Python transmitter that speaks the **same WebSocket protocol**
to the WakeMyPC backend as the Raspberry Pi Pico W hardware transmitter. Run it
on any always-on Linux box on the same LAN as the PCs you want to control —
no microcontroller required.

It handles everything the Pico does that matters server-side:

- **Auth + heartbeat** — connects to `wss://<server>/ws/pico/<UNIQUE_ID>/`,
  authenticates with the device token, and stays "online" via heartbeats.
- **Wake-on-LAN** — broadcasts the magic packet on the LAN on a `wol` command.
- **SSH/TCP relay** — pipes the server's SSH bytes to a target PC on the LAN
  (base64 over the WebSocket); the SSH private key never leaves the server.
- **Device status** — periodically TCP-probes assigned devices and reports
  online/offline to the dashboard.

## Requirements

- Docker + Docker Compose on a **Linux** host on the target LAN.
- `network_mode: host` (already set in `docker-compose.yml`). This is required:
  WOL needs a real LAN broadcast, and the relay/probes need to reach LAN hosts
  by IP. Host networking is **not** available on Docker Desktop (macOS/Windows).

## Setup

1. **Register a transmitter** on the dashboard: *Transmitters → Add*. Note its
   `unique_id`, and use **Rotate token** to reveal its device token.
2. **Configure**:
   ```bash
   cp .env.example .env
   # edit .env: SERVER_URL, UNIQUE_ID, TOKEN
   ```
3. **Run**:
   ```bash
   docker compose up -d --build
   docker compose logs -f      # should show "authenticated; N device(s) assigned"
   ```

The transmitter appears online on the dashboard within a few seconds. Assign it
to your devices, and Wake / Shutdown work exactly as with a Pico.

## Configuration

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `SERVER_URL` | yes | — | Base server URL, e.g. `wss://wakemypc.com` (or `ws://host:8000` for dev). |
| `UNIQUE_ID` | yes | — | The transmitter's `unique_id` from the dashboard (used in the WS path). |
| `TOKEN` | yes | — | The transmitter's device token (the connection secret). |
| `FIRMWARE_VERSION` | no | `docker-1.0.0` | Version string reported to the server. |
| `HEARTBEAT_INTERVAL` | no | `30` | Seconds between heartbeats. |
| `DEVICE_SCAN_INTERVAL` | no | `60` | Seconds between device-status sweeps. |
| `INSECURE_SKIP_TLS_VERIFY` | no | `0` | `1` to skip TLS verification (dev/self-signed only). |
| `LOG_LEVEL` | no | `INFO` | `DEBUG` / `INFO` / `WARNING`. |

## How it differs from the Pico

Firmware-only concepts (OTA updates, WiFi config) don't apply and are ignored.
A `reboot` command exits the process so the container restarts (`restart:
unless-stopped`). `identify` is acknowledged but has no LED to blink.

## Security notes

- The token is the only secret — keep `.env` private (it's gitignored).
- The transmitter only ever relays already-encrypted SSH bytes; it never sees
  or stores the SSH key. Treat the host like any device on your LAN.
