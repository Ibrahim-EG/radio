#!/usr/bin/env python3
"""
TACTICOM — Local Intercom System
=================================
A LAN-only, no-internet, no-extra-hardware walkie-talkie server.

Every device that opens the page lands in a live "Lobby" showing all
Sessions (rooms) currently open on the network. Any device can start a
new Session from its own Profile (a display name saved on that device),
optionally locked with an access code. Anyone in the Lobby can tap a
Session to join it — they're prompted for the access code if it's
locked. Devices can leave a Session back to the Lobby, and join a
different one, as many times as they like.

Any device can also tap "Ring the Host", with or without being in a
Session. That rings and vibrates the phone this script is actually
running on for up to 15 seconds -- via Termux:API, not the browser, so
it works whether or not anyone even has the page open. Stop it early by
tapping STOP RINGING on the Android notification it creates.

Requires the Termux:API app (installed separately, same source --
F-Droid or Play Store -- as your Termux app) and:
    pkg install termux-api

Run:
    pip install aiohttp
    python tacticom_server.py

If cert.pem / key.pem are missing, the server will try to generate a
self-signed certificate automatically via OpenSSL (required because
getUserMedia() only works in a secure context).
"""

import asyncio
import hashlib
import json
import logging
import os
import secrets
import shutil
import socket
import ssl
import subprocess
import sys
import time
from dataclasses import dataclass, field

from aiohttp import web

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("tacticom")

routes = web.RouteTableDef()

MAX_SESSION_NAME_LEN = 32
MAX_PROFILE_NAME_LEN = 24
MAX_WS_MSG_SIZE = 1 * 1024 * 1024  # 1 MB safety cap per frame


# --------------------------------------------------------------------------
# Clean console output
#
# The default aiohttp access logger prints one raw Apache-style line per
# HTTP/WS request (IP, timestamp, method, status, user-agent...) which is
# what used to flood the terminal with "strange things" on every connect.
# It's switched off in run_app() below (access_log=None) and replaced
# entirely with these short, readable, colour-coded lines that show what
# actually matters: who connected, joined, or left, and which session.
# --------------------------------------------------------------------------

class Ansi:
    RESET = "\033[0m"
    DIM = "\033[2m"
    BOLD = "\033[1m"
    GREEN = "\033[92m"
    RED = "\033[91m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    GRAY = "\033[90m"


def _now() -> str:
    return time.strftime("%H:%M:%S")


def _line(color: str, tag: str, text: str) -> None:
    print(f"{Ansi.GRAY}{_now()}{Ansi.RESET}  {color}{tag:<8}{Ansi.RESET} {text}")


def log_connect(ip: str) -> None:
    _line(Ansi.BLUE, "CONNECT", f"device connected  ({ip})")


def log_disconnect(name: str | None, ip: str) -> None:
    _line(Ansi.GRAY, "CLOSE", f"{name or 'device'} disconnected  ({ip})")


def log_create(name: str, session: str, ip: str) -> None:
    _line(Ansi.CYAN, "CREATE", f'{name} started session "{session}"  ({ip})')


def log_join(name: str, session: str, ip: str, count: int) -> None:
    _line(Ansi.GREEN, "JOIN", f'{name} joined "{session}"  -  {count} online  ({ip})')


def log_leave(name: str, session: str, ip: str, remaining: int) -> None:
    _line(Ansi.YELLOW, "LEAVE", f'{name} left "{session}"  -  {remaining} remaining  ({ip})')


def log_closed(session: str) -> None:
    _line(Ansi.GRAY, "CLOSED", f'session "{session}" is empty and was removed')


def log_denied(session: str, ip: str) -> None:
    _line(Ansi.RED, "DENIED", f'wrong access code for "{session}"  ({ip})')


def log_ring(ringer: str, ip: str) -> None:
    _line(Ansi.MAGENTA, "RING", f"{ringer} rang the house phone  ({ip})")


def log_ring_stop(ringer: str, reason: str) -> None:
    label = "timed out" if reason == "timeout" else "was stopped"
    _line(Ansi.MAGENTA, "RING", f"ring for {ringer} {label}")


def print_banner(local_ip: str) -> None:
    bar = "=" * 54
    print(f"\n{Ansi.BOLD}{bar}{Ansi.RESET}")
    print(f"{Ansi.BOLD}  TACTICOM — LOCAL INTERCOM ONLINE{Ansi.RESET}")
    print(bar)
    print(f"  Host Node        https://localhost:8443")
    print(f"  Local IP         https://{local_ip}:8443")
    print("  Open the Local IP link on any device on this Wi-Fi.")
    print("  'Ring the Host' rings THIS phone via Termux:API, whether")
    print("  or not anyone has the page open.")
    print(f"{bar}\n")


def print_shutdown() -> None:
    bar = "-" * 54
    print(f"\n{Ansi.YELLOW}{bar}")
    print("  TACTICOM OFFLINE — radio terminated, all sessions closed.")
    print(f"{bar}{Ansi.RESET}\n")


# --------------------------------------------------------------------------
# Session registry + per-connection state
# --------------------------------------------------------------------------

@dataclass
class Session:
    """A named room. Clients broadcast only within their own Session."""
    session_id: str
    display_name: str
    password_hash: str | None
    creator: str
    clients: dict = field(default_factory=dict)   # ws -> member name (deduped)
    created_at: float = field(default_factory=time.time)


class ConnState:
    """Everything the server tracks about one open WebSocket connection."""
    __slots__ = ("ws", "ip", "profile", "session", "member_name")

    def __init__(self, ws: web.WebSocketResponse, ip: str):
        self.ws = ws
        self.ip = ip
        self.profile: str | None = None     # the device's saved display name
        self.session: Session | None = None  # the Session it's currently in, if any
        self.member_name: str | None = None  # its (possibly deduped) name in that Session


sessions: dict[str, Session] = {}
connections: dict[web.WebSocketResponse, ConnState] = {}

RING_DURATION_SEC = 15          # how long a single ring lasts if nobody stops it
RING_COOLDOWN_SEC = 5           # minimum gap after a ring ends before another can start
RING_VIBRATE_INTERVAL_SEC = 1.2  # gap between buzzes while ringing
RING_VIBRATE_DURATION_MS = 900   # length of each individual buzz
RING_NOTIFICATION_ID = "tacticom_ring"

# At most one ring is active at a time. Shape once set:
# {"id": str, "ringer": str, "task": asyncio.Task, "initiator": ws}
active_ring: dict | None = None
last_ring_ended_at: float | None = None  # for the cooldown, so mashing the button can't spam it

# Tapping "STOP RINGING" on the Android notification runs a shell command
# that just touches this file -- the ring loop polls for it. No network
# round-trip, no dependency on curl/wget being installed in Termux, and it
# works even if this script has restarted since the ring began (it hasn't,
# in practice, since one ring never outlives the process, but it costs
# nothing to be simple and robust here).
STOP_FLAG_PATH = os.path.join(os.path.expanduser("~"), ".tacticom_stop_ring")
try:
    os.remove(STOP_FLAG_PATH)  # clear any stale flag left over from a crashed previous run
except OSError:
    pass

TERMUX_AVAILABLE = shutil.which("termux-vibrate") is not None and shutil.which("termux-notification") is not None
if not TERMUX_AVAILABLE:
    log.warning(
        "termux-api commands not found -- 'Ring the Host' will be logged only, "
        "not felt. Install the Termux:API app (same source as Termux itself) "
        "and run: pkg install termux-api"
    )


def hash_password(session_id: str, password: str) -> str:
    # Salting with the session id is enough here — this is a casual local
    # access code, not a security boundary for sensitive data.
    return hashlib.sha256(f"{session_id}:{password}".encode("utf-8")).hexdigest()


def sanitize(text, max_len: int, fallback: str) -> str:
    if not isinstance(text, str):
        return fallback
    text = text.strip()
    return text[:max_len] if text else fallback


def normalize_session_id(text) -> str | None:
    """Canonical ID for a session name, or None if it's empty/invalid.

    Case/whitespace differences used to silently create two separate,
    unconnected rooms with no error at all -- normalizing here means
    "Kitchen", "kitchen", " KITCHEN " all resolve to the same session.
    """
    if not isinstance(text, str):
        return None
    text = text.strip()
    if not text:
        return None
    return text[:MAX_SESSION_NAME_LEN].upper()


def random_callsign() -> str:
    return f"OP-{secrets.randbelow(9000) + 1000}"


# --------------------------------------------------------------------------
# Broadcast helpers
#
# Every send below is isolated in its own try/except. If it weren't, one
# peer mid-teardown could throw an exception that propagates up and kills
# the *sender's* connection too -- a single flaky phone taking down
# everyone else's audio. Dead sockets found this way are cleaned up
# immediately rather than left to linger.
# --------------------------------------------------------------------------

async def broadcast_to_session(session: Session, sender_ws, payload, binary: bool) -> None:
    dead = []
    for client_ws in list(session.clients.keys()):
        if client_ws is sender_ws or client_ws.closed:
            continue
        try:
            if binary:
                await client_ws.send_bytes(payload)
            else:
                await client_ws.send_str(payload)
        except (ConnectionResetError, RuntimeError, ConnectionError):
            dead.append(client_ws)
    for d in dead:
        session.clients.pop(d, None)


async def broadcast_presence(session: Session) -> None:
    msg = json.dumps({
        "type": "presence",
        "session": session.display_name,
        "count": len(session.clients),
        "names": list(session.clients.values()),
    })
    dead = []
    for client_ws in list(session.clients.keys()):
        if client_ws.closed:
            continue
        try:
            await client_ws.send_str(msg)
        except (ConnectionResetError, RuntimeError, ConnectionError):
            dead.append(client_ws)
    for d in dead:
        session.clients.pop(d, None)


def lobby_snapshot() -> list[dict]:
    return [
        {
            "id": s.session_id,
            "name": s.display_name,
            "count": len(s.clients),
            "locked": s.password_hash is not None,
        }
        for s in sorted(sessions.values(), key=lambda s: s.created_at)
    ]


async def send_lobby(conn: ConnState) -> None:
    try:
        await conn.ws.send_str(json.dumps({"type": "lobby", "sessions": lobby_snapshot()}))
    except (ConnectionResetError, RuntimeError, ConnectionError):
        pass


async def push_lobby_to_idle() -> None:
    """Refresh the Lobby list for every device that isn't in a Session
    right now (devices already talking don't need it)."""
    for conn in list(connections.values()):
        if conn.session is None:
            await send_lobby(conn)


async def send_error(conn: ConnState, reason: str) -> None:
    try:
        await conn.ws.send_str(json.dumps({"type": "session_error", "reason": reason}))
    except (ConnectionResetError, RuntimeError, ConnectionError):
        pass


# --------------------------------------------------------------------------
# Ring the Host
#
# Any connected device -- in the Lobby or in a Session, doesn't matter --
# can "ring" the house phone. This rings and vibrates the actual Android
# device this script is running on, via Termux:API, so it works whether or
# not anyone has the page open (a browser-only alert would be useless for
# exactly that reason). Only one ring runs at a time, and a short cooldown
# after it ends stops a mashed button from re-triggering it instantly.
# --------------------------------------------------------------------------

async def send_json(ws, payload: dict) -> None:
    try:
        await ws.send_str(json.dumps(payload))
    except (ConnectionResetError, RuntimeError, ConnectionError):
        pass


async def run_termux(*args: str) -> None:
    """Fire-and-forget a termux-api command without blocking the event
    loop. No-ops quietly if Termux:API isn't installed (e.g. testing this
    script on a regular PC) -- ringing just won't be physically felt."""
    if not TERMUX_AVAILABLE:
        return
    try:
        proc = await asyncio.create_subprocess_exec(
            *args, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()
    except (FileNotFoundError, OSError) as exc:
        log.warning("termux command '%s' failed: %s", args[0], exc)


async def show_ring_notification(ringer: str) -> None:
    await run_termux(
        "termux-notification",
        "--id", RING_NOTIFICATION_ID,
        "--title", "TACTICOM — Incoming Ring",
        "--content", f"{ringer} wants your attention",
        "--priority", "max",
        "--sound",
        "--vibrate", "500,500,500,500,500",
        "--ongoing",
        "--button1", "STOP RINGING",
        "--button1-action", f"touch {STOP_FLAG_PATH}",
    )


async def clear_ring_notification() -> None:
    await run_termux("termux-notification-remove", RING_NOTIFICATION_ID)


def _consume_stop_flag() -> bool:
    if os.path.exists(STOP_FLAG_PATH):
        try:
            os.remove(STOP_FLAG_PATH)
        except OSError:
            pass
        return True
    return False


async def _ring_task(ringer: str) -> str:
    """Runs the physical ring: an ongoing notification plus repeated
    buzzes, checking each cycle for the STOP flag. Always clears the
    notification on the way out, whether it finished, was stopped, or was
    cancelled outright (e.g. server shutdown mid-ring)."""
    await show_ring_notification(ringer)
    reason = "timeout"
    try:
        elapsed = 0.0
        while elapsed < RING_DURATION_SEC:
            if _consume_stop_flag():
                reason = "stopped"
                break
            await run_termux("termux-vibrate", "-d", str(RING_VIBRATE_DURATION_MS), "-f")
            await asyncio.sleep(RING_VIBRATE_INTERVAL_SEC)
            elapsed += RING_VIBRATE_INTERVAL_SEC
    finally:
        await clear_ring_notification()
    return reason


async def finish_ring(ring_id: str, reason: str) -> None:
    """Shared cleanup once ringing has actually stopped, from whichever
    path got there -- ran out the clock, or was cancelled early."""
    global active_ring, last_ring_ended_at
    if active_ring is None or active_ring["id"] != ring_id:
        return
    ring = active_ring
    active_ring = None
    last_ring_ended_at = time.time()
    log_ring_stop(ring["ringer"], reason)
    if ring["initiator"] in connections:
        await send_json(ring["initiator"], {"type": "ring_stop", "id": ring_id, "reason": reason})


async def stop_ring(ring_id: str, reason: str) -> None:
    """Early-stop path: cancels the running ring task. finish_ring() still
    runs afterward for the shared bookkeeping and to notify the browser
    that requested the ring, if it's still connected."""
    if active_ring is None or active_ring["id"] != ring_id:
        return
    active_ring["task"].cancel()
    await finish_ring(ring_id, reason)


async def _run_ring(ring_id: str, ringer: str) -> None:
    try:
        reason = await _ring_task(ringer)
    except asyncio.CancelledError:
        return  # stop_ring() already handles bookkeeping for this path
    await finish_ring(ring_id, reason)


async def start_ring(conn: ConnState) -> None:
    global active_ring
    if active_ring is not None:
        await send_json(conn.ws, {"type": "ring_error", "reason": "Already ringing the house phone — hang tight."})
        return
    if last_ring_ended_at is not None:
        wait_left = RING_COOLDOWN_SEC - (time.time() - last_ring_ended_at)
        if wait_left > 0:
            await send_json(conn.ws, {
                "type": "ring_error",
                "reason": f"Please wait {wait_left:.0f}s before ringing again.",
            })
            return

    ring_id = secrets.token_hex(4)
    ringer = conn.profile or "Someone"
    task = asyncio.create_task(_run_ring(ring_id, ringer))
    active_ring = {"id": ring_id, "ringer": ringer, "task": task, "initiator": conn.ws}
    log_ring(ringer, conn.ip)


def get_local_ip() -> str:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect(("1.1.1.1", 80))
            return s.getsockname()[0]
        except OSError:
            return "127.0.0.1"


# --------------------------------------------------------------------------
# Session lifecycle
# --------------------------------------------------------------------------

async def enter_session(conn: ConnState, session: Session) -> None:
    name = conn.profile or random_callsign()
    taken = set(session.clients.values())
    base, i = name, 2
    while name in taken:
        name = f"{base}-{i}"
        i += 1

    session.clients[conn.ws] = name
    conn.session = session
    conn.member_name = name

    await conn.ws.send_str(json.dumps({
        "type": "session_joined",
        "session": {
            "id": session.session_id,
            "name": session.display_name,
            "locked": session.password_hash is not None,
        },
        "name": name,
        "count": len(session.clients),
    }))
    await broadcast_presence(session)
    log_join(name, session.display_name, conn.ip, len(session.clients))
    await push_lobby_to_idle()


async def exit_session(conn: ConnState) -> None:
    session = conn.session
    if session is None:
        return

    session.clients.pop(conn.ws, None)
    name = conn.member_name or "device"
    remaining = len(session.clients)
    log_leave(name, session.display_name, conn.ip, remaining)

    conn.session = None
    conn.member_name = None

    if session.clients:
        await broadcast_presence(session)
    elif sessions.get(session.session_id) is session:
        del sessions[session.session_id]
        log_closed(session.display_name)

    # This also delivers a fresh Lobby snapshot back to `conn` itself,
    # since it's now idle again -- no separate call needed for that.
    await push_lobby_to_idle()


async def handle_create_session(conn: ConnState, data: dict) -> None:
    if conn.session is not None:
        await exit_session(conn)

    session_id = normalize_session_id(data.get("name"))
    if session_id is None:
        await send_error(conn, "Give the session a name.")
        return
    if session_id in sessions:
        await send_error(conn, f'A session named "{session_id}" already exists — join it from the lobby instead.')
        return

    password = (data.get("password") or "").strip()
    creator = conn.profile or random_callsign()
    session = Session(
        session_id=session_id,
        display_name=session_id,
        password_hash=hash_password(session_id, password) if password else None,
        creator=creator,
    )
    sessions[session_id] = session
    log_create(creator, session.display_name, conn.ip)
    await enter_session(conn, session)


async def handle_join_session(conn: ConnState, data: dict) -> None:
    if conn.session is not None:
        await exit_session(conn)

    session_id = normalize_session_id(data.get("id"))
    session = sessions.get(session_id) if session_id else None
    if session is None:
        await send_error(conn, "That session just ended.")
        await send_lobby(conn)
        return

    if session.password_hash is not None:
        # Trim whitespace -- a trailing space from autofill/autocorrect on
        # just one of two devices used to silently break an otherwise
        # "identical" access code.
        password = (data.get("password") or "").strip()
        if hash_password(session_id, password) != session.password_hash:
            log_denied(session.display_name, conn.ip)
            await send_error(conn, "Incorrect access code.")
            return

    await enter_session(conn, session)


async def handle_text(conn: ConnState, raw: str) -> None:
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return
    if not isinstance(data, dict):
        return

    # Any message can carry a profile update -- keeps profile editing
    # simple on the client without a dedicated round trip.
    if "profile" in data:
        conn.profile = sanitize(data.get("profile"), MAX_PROFILE_NAME_LEN, conn.profile or random_callsign())

    mtype = data.get("type")

    if mtype in ("hello", "list_sessions"):
        await send_lobby(conn)
    elif mtype == "create_session":
        await handle_create_session(conn, data)
    elif mtype == "join_session":
        await handle_join_session(conn, data)
    elif mtype == "leave_session":
        await exit_session(conn)
    elif mtype in ("tx_start", "tx_stop") and conn.session is not None:
        await broadcast_to_session(conn.session, conn.ws, json.dumps(data), binary=False)
    elif mtype == "ring":
        await start_ring(conn)
    elif mtype == "ring_stop":
        # No browser UI exposes this anymore (stopping is meant to happen
        # via the Android notification's own button), but it's kept as a
        # harmless, useful hook for testing the ring flow without a phone.
        if active_ring is not None:
            await stop_ring(active_ring["id"], reason="stopped")


async def handle_binary(conn: ConnState, payload) -> None:
    if conn.session is not None:
        await broadcast_to_session(conn.session, conn.ws, payload, binary=True)


async def cleanup_connection(conn: ConnState) -> None:
    await exit_session(conn)
    connections.pop(conn.ws, None)
    log_disconnect(conn.profile, conn.ip)


# --------------------------------------------------------------------------
# HTML / CSS / JS — single-file client
# --------------------------------------------------------------------------

HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>TACTICOM | Local Intercom System</title>
    <style>
        :root {
            --bg-color: #0b0e14;
            --panel-bg: #141923;
            --panel-border: #232d3f;
            --text-main: #e6edf3;
            --text-muted: #8b949e;
            --accent-green: #2ea043;
            --accent-red: #f85149;
            --accent-blue: #388bfd;
            --accent-amber: #d29922;
        }

        * {
            box-sizing: border-box;
            user-select: none;
            -webkit-user-select: none;
            -webkit-touch-callout: none;
        }

        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, monospace, sans-serif;
            background: var(--bg-color);
            color: var(--text-main);
            margin: 0;
            padding: 16px;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
        }

        .terminal-card {
            background: var(--panel-bg);
            border: 1px solid var(--panel-border);
            border-radius: 16px;
            width: 100%;
            max-width: 420px;
            padding: 24px;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.6);
        }

        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--panel-border);
            padding-bottom: 12px;
            margin-bottom: 20px;
        }

        .brand {
            font-size: 16px;
            font-weight: 800;
            letter-spacing: 2px;
            color: var(--text-main);
            display: flex;
            align-items: center;
            gap: 8px;
        }

        .brand-dot {
            width: 8px;
            height: 8px;
            background: var(--accent-blue);
            border-radius: 50%;
            box-shadow: 0 0 8px var(--accent-blue);
        }

        .status-pill {
            font-size: 11px;
            font-weight: 700;
            letter-spacing: 1px;
            padding: 4px 10px;
            border-radius: 20px;
            background: #1c212c;
            border: 1px solid var(--panel-border);
            display: flex;
            align-items: center;
            gap: 6px;
        }

        .status-dot {
            width: 6px;
            height: 6px;
            border-radius: 50%;
            background: var(--accent-amber);
        }

        .metrics-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 12px;
            margin-bottom: 20px;
        }

        .metric-box {
            background: #0d1117;
            border: 1px solid var(--panel-border);
            padding: 12px;
            border-radius: 8px;
            text-align: left;
        }

        .metric-label {
            font-size: 10px;
            color: var(--text-muted);
            letter-spacing: 1px;
            text-transform: uppercase;
            margin-bottom: 4px;
        }

        .metric-value {
            font-size: 18px;
            font-weight: 700;
            font-family: monospace;
        }

        .vu-container {
            background: #0d1117;
            border: 1px solid var(--panel-border);
            border-radius: 8px;
            padding: 10px;
            margin-bottom: 20px;
        }

        .vu-header {
            display: flex;
            justify-content: space-between;
            font-size: 10px;
            color: var(--text-muted);
            margin-bottom: 6px;
            letter-spacing: 1px;
        }

        .vu-track {
            height: 8px;
            background: #1c212c;
            border-radius: 4px;
            overflow: hidden;
        }

        .vu-bar {
            height: 100%;
            width: 0%;
            background: linear-gradient(90deg, var(--accent-green) 70%, var(--accent-amber) 85%, var(--accent-red) 100%);
            transition: width 0.05s ease;
        }

        .field-group {
            margin-bottom: 12px;
        }

        .field-label {
            font-size: 10px;
            color: var(--text-muted);
            letter-spacing: 1px;
            text-transform: uppercase;
            margin-bottom: 6px;
            display: block;
        }

        .field-input {
            width: 100%;
            background: #0d1117;
            border: 1px solid var(--panel-border);
            color: var(--text-main);
            padding: 10px 12px;
            border-radius: 8px;
            font-family: monospace;
            font-size: 13px;
        }

        .field-input:focus {
            outline: none;
            border-color: var(--accent-blue);
        }

        .field-row {
            display: flex;
            gap: 10px;
        }

        .field-row .field-group {
            flex: 1;
        }

        .error-text {
            color: var(--accent-red);
            font-size: 11px;
            margin-top: -4px;
            margin-bottom: 10px;
            min-height: 14px;
        }

        .error-banner {
            display: none;
            background: rgba(248, 81, 73, 0.12);
            border: 1px solid var(--accent-red);
            color: #ffb4af;
            font-size: 12px;
            font-weight: 700;
            padding: 10px 12px;
            border-radius: 8px;
            margin-bottom: 14px;
            animation: shake 0.35s ease;
        }

        @keyframes shake {
            0%, 100% { transform: translateX(0); }
            25% { transform: translateX(-4px); }
            75% { transform: translateX(4px); }
        }

        .btn-primary {
            width: 100%;
            padding: 16px;
            background: var(--accent-blue);
            color: white;
            border: none;
            border-radius: 10px;
            font-size: 14px;
            font-weight: 800;
            letter-spacing: 1.5px;
            cursor: pointer;
            box-shadow: 0 4px 14px rgba(56, 139, 253, 0.3);
            transition: background 0.2s;
        }

        .btn-primary:disabled {
            opacity: 0.6;
            cursor: default;
        }

        .link-btn {
            background: none;
            border: none;
            color: var(--text-muted);
            font-size: 11px;
            letter-spacing: 1px;
            cursor: pointer;
            text-decoration: underline;
        }

        .field-hint {
            font-size: 10px;
            color: var(--text-muted);
            margin-top: -8px;
            margin-bottom: 14px;
        }

        .session-list {
            display: flex;
            flex-direction: column;
            gap: 8px;
            margin-bottom: 4px;
        }

        .session-empty {
            font-size: 12px;
            color: var(--text-muted);
            background: #0d1117;
            border: 1px dashed var(--panel-border);
            border-radius: 8px;
            padding: 14px;
            text-align: center;
        }

        .session-card {
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: #0d1117;
            border: 1px solid var(--panel-border);
            border-radius: 8px;
            padding: 12px 14px;
            cursor: pointer;
            transition: border-color 0.15s ease, background 0.15s ease;
        }

        .session-card:active {
            background: #171d29;
            border-color: var(--accent-blue);
        }

        .session-card-name {
            font-size: 13px;
            font-weight: 700;
            letter-spacing: 0.5px;
        }

        .session-card-meta {
            font-size: 11px;
            color: var(--text-muted);
            margin-top: 2px;
        }

        .session-card-arrow {
            font-size: 20px;
            color: var(--text-muted);
        }

        .lock-icon {
            font-size: 11px;
        }

        .modal-overlay {
            position: fixed;
            inset: 0;
            background: rgba(0, 0, 0, 0.65);
            display: flex;
            justify-content: center;
            align-items: center;
            z-index: 50;
            padding: 16px;
        }

        .modal-card {
            background: var(--panel-bg);
            border: 1px solid var(--panel-border);
            border-radius: 16px;
            width: 100%;
            max-width: 340px;
            padding: 20px;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.6);
        }

        .modal-title {
            font-size: 13px;
            font-weight: 800;
            margin-bottom: 14px;
            color: var(--text-main);
        }

        .modal-actions {
            display: flex;
            gap: 10px;
        }

        .btn-secondary {
            background: #1c212c;
            border: 1px solid var(--panel-border);
        }

        .ring-row {
            margin: 4px 0 18px 0;
        }

        .btn-ring {
            background: linear-gradient(135deg, #d29922, #f85149);
            box-shadow: 0 4px 14px rgba(248, 81, 73, 0.3);
        }

        .ring-status {
            display: none;
            font-size: 11px;
            color: var(--text-muted);
            text-align: center;
            margin-top: 8px;
        }

        .ptt-trigger-container {
            display: flex;
            justify-content: center;
            align-items: center;
            margin-top: 10px;
        }

        .ptt-button {
            width: 180px;
            height: 180px;
            border-radius: 50%;
            background: radial-gradient(circle at 30% 30%, #2a3241, #141923);
            border: 6px solid #232d3f;
            box-shadow: inset 0 4px 8px rgba(0,0,0,0.8), 0 8px 20px rgba(0,0,0,0.6);
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            color: var(--text-muted);
            cursor: pointer;
            touch-action: none;
            transition: all 0.15s ease;
        }

        .ptt-button.ready {
            border-color: var(--accent-green);
            color: var(--text-main);
            box-shadow: inset 0 2px 6px rgba(0,0,0,0.6), 0 0 15px rgba(46, 160, 67, 0.2);
        }

        .ptt-button.transmitting {
            background: radial-gradient(circle at 30% 30%, #f85149, #9e1313) !important;
            border-color: #ff7b72 !important;
            color: #ffffff !important;
            box-shadow: 0 0 30px rgba(248, 81, 73, 0.6) !important;
            transform: scale(0.96);
        }

        .ptt-icon {
            font-size: 28px;
            margin-bottom: 6px;
        }

        .ptt-label {
            font-size: 12px;
            font-weight: 800;
            letter-spacing: 1.5px;
        }

        .mode-switch {
            display: grid;
            grid-template-columns: 1fr 1fr;
            background: #0d1117;
            border: 1px solid var(--panel-border);
            padding: 4px;
            border-radius: 10px;
            margin-bottom: 24px;
        }

        .mode-option {
            background: transparent;
            border: none;
            color: var(--text-muted);
            padding: 10px;
            font-size: 12px;
            font-weight: 700;
            letter-spacing: 1px;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.2s ease;
        }

        .mode-option.active {
            background: #1c212c;
            color: var(--text-main);
            border: 1px solid var(--panel-border);
            box-shadow: 0 2px 8px rgba(0,0,0,0.4);
        }

        .leave-row {
            text-align: center;
            margin-top: 16px;
        }
    </style>
</head>
<body>

    <div class="terminal-card">
        <div class="header">
            <div class="brand">
                <div class="brand-dot"></div> TACTICOM
            </div>
            <div id="statusPill" class="status-pill">
                <div id="statusDot" class="status-dot"></div>
                <span id="statusText">INITIALIZING</span>
            </div>
        </div>

        <div class="metrics-grid">
            <div class="metric-box">
                <div class="metric-label">Active Nodes</div>
                <div id="peerCount" class="metric-value">0</div>
            </div>
            <div class="metric-box">
                <div class="metric-label">Session</div>
                <div id="channelValue" class="metric-value" style="color: var(--accent-blue);">&mdash;</div>
                <div id="callsignValue" style="font-size:10px;color:var(--text-muted);margin-top:2px;"></div>
            </div>
        </div>

        <div class="vu-container">
            <div class="vu-header">
                <span>AUDIO INPUT LEVEL</span>
                <span id="vuValue">0%</span>
            </div>
            <div class="vu-track">
                <div id="vuBar" class="vu-bar"></div>
            </div>
        </div>

        <div id="lobbyPanel">
            <div class="field-group">
                <label class="field-label">Your Profile</label>
                <input id="profileInput" class="field-input" type="text" maxlength="24" placeholder="e.g. Dad's Phone">
            </div>
            <div class="field-hint">Saved on this device — used as your name in every session.</div>

            <div id="ringRow" class="ring-row">
                <button id="ringBtn" class="btn-primary btn-ring" onclick="ringHost()">&#128276; RING THE HOST</button>
                <div id="ringStatus" class="ring-status"></div>
            </div>

            <div id="lobbyErrorBanner" class="error-banner"></div>

            <label class="field-label">Open Sessions</label>
            <div id="sessionList" class="session-list">
                <div class="session-empty">No sessions yet — start one below.</div>
            </div>

            <div class="field-row" style="margin-top:16px;">
                <div class="field-group">
                    <label class="field-label">New Session Name</label>
                    <input id="newSessionName" class="field-input" type="text" maxlength="32" placeholder="e.g. Kitchen">
                </div>
                <div class="field-group">
                    <label class="field-label">Access Code (optional)</label>
                    <input id="newSessionPassword" class="field-input" type="password" maxlength="64" placeholder="&bull;&bull;&bull;&bull;&bull;&bull;">
                </div>
            </div>
            <button id="createBtn" class="btn-primary" onclick="createSession()">START SESSION</button>
        </div>

        <div id="controls" style="display: none;">
            <div class="mode-switch">
                <button id="pttModeBtn" class="mode-option active" onclick="setMode('ptt')">PRESS TO TALK</button>
                <button id="alwaysModeBtn" class="mode-option" onclick="setMode('always')">CONSTANT LIVE</button>
            </div>

            <div class="ptt-trigger-container">
                <button id="talkBtn" class="ptt-button ready">
                    <div class="ptt-icon">&#127908;</div>
                    <div id="talkLabel" class="ptt-label">HOLD TO TALK</div>
                </button>
            </div>

            <div class="leave-row">
                <button class="link-btn" onclick="leaveSession()">LEAVE SESSION / BACK TO LOBBY</button>
            </div>
        </div>
    </div>

    <div id="passwordModal" class="modal-overlay" style="display:none;">
        <div class="modal-card">
            <div id="modalSessionName" class="modal-title">Session</div>
            <div class="field-group">
                <label class="field-label">Access Code</label>
                <input id="modalPasswordInput" class="field-input" type="password" maxlength="64">
            </div>
            <div id="modalErrorText" class="error-text"></div>
            <div class="modal-actions">
                <button class="btn-primary btn-secondary" onclick="closePasswordModal()">CANCEL</button>
                <button class="btn-primary" onclick="submitPasswordModal()">JOIN</button>
            </div>
        </div>
    </div>

    <script>
        // ------------------------------------------------------------------
        // Audio quality constants.
        //
        // The previous version captured and played back at 16 kHz to keep
        // network messages small and reduce latency -- but that ceiling on
        // sample rate is what made voices sound thin/"crap". On a LAN,
        // bandwidth is not the constraint (48 kHz mono 16-bit PCM is only
        // ~96 KB/s, trivial for Wi-Fi), so quality is raised to 48 kHz --
        // the same rate used for broadcast-quality audio -- while chunking
        // is kept short (80ms) so latency stays low.
        // ------------------------------------------------------------------
        const SAMPLE_RATE = 48000;
        const CHUNK_MS = 80;
        const CHUNK_SAMPLES = Math.round(SAMPLE_RATE * CHUNK_MS / 1000);

        // AudioWorklet processor source, loaded via Blob URL so the whole
        // app stays a single file.
        //
        // The output is explicitly silenced: the node must stay connected
        // to destination for process() to keep firing in every browser,
        // but we never want to hear our own mic looped back locally.
        //
        // It only encodes+posts samples while "active" (i.e. while
        // actually transmitting), so idle CPU use is close to zero.
        const MIC_WORKLET_SRC = `
            class MicCaptureProcessor extends AudioWorkletProcessor {
                constructor() {
                    super();
                    this.active = false;
                    this.chunkSamples = ${CHUNK_SAMPLES};
                    this.buffer = new Float32Array(this.chunkSamples);
                    this.writeIndex = 0;
                    this.port.onmessage = (e) => {
                        if (e.data && e.data.type === 'set-active') {
                            this.active = e.data.active;
                            this.writeIndex = 0; // start clean on every key-up
                        }
                    };
                }
                flush() {
                    const pcm16 = new Int16Array(this.writeIndex);
                    for (let i = 0; i < this.writeIndex; i++) {
                        const s = Math.max(-1, Math.min(1, this.buffer[i]));
                        pcm16[i] = s < 0 ? s * 0x8000 : s * 0x7FFF;
                    }
                    this.port.postMessage(pcm16.buffer, [pcm16.buffer]);
                    this.writeIndex = 0;
                }
                process(inputs, outputs) {
                    const output = outputs[0];
                    if (output && output[0]) output[0].fill(0);
                    if (this.active) {
                        const input = inputs[0];
                        if (input && input[0] && input[0].length) {
                            const channel = input[0];
                            for (let i = 0; i < channel.length; i++) {
                                this.buffer[this.writeIndex++] = channel[i];
                                if (this.writeIndex >= this.chunkSamples) this.flush();
                            }
                        }
                    }
                    return true;
                }
            }
            registerProcessor('mic-capture-processor', MicCaptureProcessor);
        `;

        const JITTER_BUFFER_SEC = 0.12;   // small cushion to smooth normal network jitter
        const MAX_BACKLOG_SEC = 0.6;      // hard cap: never let playback fall more than
                                           // this far behind live -- drop stale audio
                                           // instead of "catching up" on everything missed

        const statusDot = document.getElementById('statusDot');
        const statusText = document.getElementById('statusText');
        const peerCount = document.getElementById('peerCount');
        const channelValue = document.getElementById('channelValue');
        const callsignValue = document.getElementById('callsignValue');
        const lobbyPanel = document.getElementById('lobbyPanel');
        const lobbyErrorBanner = document.getElementById('lobbyErrorBanner');
        const sessionListEl = document.getElementById('sessionList');
        const createBtn = document.getElementById('createBtn');
        const controls = document.getElementById('controls');
        const talkBtn = document.getElementById('talkBtn');
        const talkLabel = document.getElementById('talkLabel');
        const pttBtn = document.getElementById('pttModeBtn');
        const alwaysBtn = document.getElementById('alwaysModeBtn');
        const vuBar = document.getElementById('vuBar');
        const vuValue = document.getElementById('vuValue');
        const profileInput = document.getElementById('profileInput');
        const passwordModal = document.getElementById('passwordModal');
        const modalSessionName = document.getElementById('modalSessionName');
        const modalPasswordInput = document.getElementById('modalPasswordInput');
        const modalErrorText = document.getElementById('modalErrorText');
        const ringBtn = document.getElementById('ringBtn');
        const ringStatus = document.getElementById('ringStatus');

        function showLobbyError(text) {
            lobbyErrorBanner.textContent = text;
            lobbyErrorBanner.style.display = text ? 'block' : 'none';
            lobbyErrorBanner.style.animation = 'none';
            void lobbyErrorBanner.offsetWidth;
            lobbyErrorBanner.style.animation = '';
        }

        let ws = null;
        let reconnectAttempts = 0;
        let lastLobby = [];
        let modalSession = null;
        let pendingPassword = undefined;
        let activeSession = null; // { id, password } while inside a session, else null

        let audioCtx;
        let micStream;
        let micWorklet;
        let analyserNode;
        let nextStartTime = 0;
        let currentMode = 'ptt';
        let isTransmitting = false;

        // ------------------------------------------------------------------
        // Profile: a display name saved on THIS device (localStorage), sent
        // to the server on every connect/reconnect and used as your name
        // whenever you create or join a session.
        // ------------------------------------------------------------------
        let myProfile = localStorage.getItem('tacticom_profile') || '';
        profileInput.value = myProfile;
        // 'input' fires on every keystroke, not just on blur -- so a name
        // typed and immediately used (tapping Start Session without
        // tabbing away first) is never stale.
        profileInput.addEventListener('input', saveProfile);

        function saveProfile() {
            myProfile = profileInput.value.trim().slice(0, 24);
            localStorage.setItem('tacticom_profile', myProfile);
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'set_profile', profile: myProfile || undefined }));
            }
        }

        function setStatus(state) {
            const map = {
                initializing: { text: 'INITIALIZING', color: 'var(--accent-amber)' },
                online: { text: 'ONLINE', color: 'var(--accent-green)' },
                offline: { text: 'OFFLINE', color: 'var(--accent-red)' },
                reconnecting: { text: 'RECONNECTING', color: 'var(--accent-amber)' },
            };
            const s = map[state] || map.initializing;
            statusText.textContent = s.text;
            statusDot.style.background = s.color;
            statusDot.style.boxShadow = state === 'offline' ? 'none' : `0 0 8px ${s.color}`;
        }

        // ------------------------------------------------------------------
        // WebSocket connection with auto-reconnect (exponential backoff).
        // The page always tries to stay connected -- there's no separate
        // "disconnect" step, only leaving a session back to the lobby.
        //
        // On every (re)connect it sends 'hello' with the saved profile, so
        // the lobby always reflects who you are. If the connection dropped
        // while INSIDE a session, it also immediately retries joining that
        // same session (with the same access code) -- a brief Wi-Fi drop
        // resumes automatically instead of dumping you back at the lobby.
        // ------------------------------------------------------------------
        function connectWebSocket() {
            const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
            ws = new WebSocket(`${protocol}//${location.host}/ws`);
            ws.binaryType = 'arraybuffer';

            ws.onopen = () => {
                reconnectAttempts = 0;
                setStatus('online');
                ws.send(JSON.stringify({ type: 'hello', profile: myProfile || undefined }));
                if (activeSession) {
                    ws.send(JSON.stringify({
                        type: 'join_session',
                        id: activeSession.id,
                        password: activeSession.password,
                    }));
                }
            };

            ws.onclose = () => {
                setStatus('reconnecting');
                const delay = Math.min(1000 * (2 ** reconnectAttempts), 8000) + Math.random() * 400;
                reconnectAttempts++;
                setTimeout(connectWebSocket, delay);
            };

            ws.onerror = () => {
                try { ws.close(); } catch (e) { /* already closing */ }
            };

            ws.onmessage = handleMessage;
        }

        function handleMessage(event) {
            if (typeof event.data === 'string') {
                let msg;
                try { msg = JSON.parse(event.data); } catch (e) { return; }
                if (msg.type === 'lobby') {
                    renderLobby(msg.sessions);
                } else if (msg.type === 'session_joined') {
                    onSessionJoined(msg);
                } else if (msg.type === 'session_error') {
                    onSessionError(msg);
                } else if (msg.type === 'presence') {
                    peerCount.textContent = msg.count;
                } else if (msg.type === 'tx_start') {
                    playPreambleTone();
                } else if (msg.type === 'tx_stop') {
                    playCourtesyBeep();
                } else if (msg.type === 'ring_stop') {
                    resetRingButton();
                    showRingStatus(msg.reason === 'timeout' ? 'No answer — try again.' : 'Ringing stopped.');
                } else if (msg.type === 'ring_error') {
                    resetRingButton();
                    showRingStatus(msg.reason);
                }
            } else if (event.data instanceof ArrayBuffer) {
                playReceivedPCM(event.data);
            }
        }

        // ------------------------------------------------------------------
        // Lobby: the list of Sessions currently open, pushed by the server
        // any time it changes. Tapping a card joins it (prompting for an
        // access code first if it's locked).
        // ------------------------------------------------------------------
        function renderLobby(list) {
            lastLobby = list;
            if (!list.length) {
                sessionListEl.innerHTML = '<div class="session-empty">No sessions yet — start one below.</div>';
                return;
            }
            sessionListEl.innerHTML = list.map(s => `
                <div class="session-card" onclick="attemptJoin('${escapeAttr(s.id)}')">
                    <div>
                        <div class="session-card-name">${escapeHtml(s.name)}${s.locked ? ' <span class="lock-icon">&#128274;</span>' : ''}</div>
                        <div class="session-card-meta">${s.count} online</div>
                    </div>
                    <div class="session-card-arrow">&rsaquo;</div>
                </div>
            `).join('');
        }

        function escapeHtml(str) {
            const div = document.createElement('div');
            div.textContent = str;
            return div.innerHTML;
        }

        function escapeAttr(str) {
            return String(str).replace(/'/g, "&#39;");
        }

        function attemptJoin(id) {
            const session = lastLobby.find(s => s.id === id);
            if (!session) return;
            if (session.locked) {
                openPasswordModal(session);
            } else {
                pendingPassword = undefined;
                showLobbyError('');
                ws.send(JSON.stringify({ type: 'join_session', id }));
            }
        }

        function openPasswordModal(session) {
            modalSession = session;
            modalSessionName.textContent = 'Session: ' + session.name;
            modalPasswordInput.value = '';
            modalErrorText.textContent = '';
            passwordModal.style.display = 'flex';
            modalPasswordInput.focus();
        }

        function closePasswordModal() {
            modalSession = null;
            passwordModal.style.display = 'none';
        }

        function submitPasswordModal() {
            if (!modalSession) return;
            pendingPassword = modalPasswordInput.value;
            ws.send(JSON.stringify({ type: 'join_session', id: modalSession.id, password: pendingPassword }));
        }

        function createSession() {
            const name = document.getElementById('newSessionName').value.trim();
            const password = document.getElementById('newSessionPassword').value;
            if (!name) { showLobbyError('Give the session a name.'); return; }
            pendingPassword = password || undefined;
            showLobbyError('');
            createBtn.disabled = true;
            createBtn.textContent = 'STARTING...';
            ws.send(JSON.stringify({ type: 'create_session', name, password: pendingPassword }));
        }

        function onSessionJoined(msg) {
            activeSession = { id: msg.session.id, password: pendingPassword };
            closePasswordModal();
            createBtn.disabled = false;
            createBtn.textContent = 'START SESSION';
            channelValue.textContent = msg.session.name;
            callsignValue.textContent = 'as ' + msg.name;
            peerCount.textContent = msg.count;
            lobbyPanel.style.display = 'none';
            controls.style.display = 'block';
            startAudioPipeline();
        }

        function onSessionError(msg) {
            const reason = msg.reason || 'Could not join session.';
            if (passwordModal.style.display === 'flex') {
                modalErrorText.textContent = reason;
            } else {
                showLobbyError(reason);
            }
            createBtn.disabled = false;
            createBtn.textContent = 'START SESSION';
        }

        function leaveSession() {
            stopTransmission();
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'leave_session' }));
            }
            teardownAudio();
            activeSession = null;
            controls.style.display = 'none';
            lobbyPanel.style.display = 'block';
            peerCount.textContent = '0';
            channelValue.textContent = '\u2014';
            callsignValue.textContent = '';
        }

        function teardownAudio() {
            if (micStream) micStream.getTracks().forEach(t => t.stop());
            if (audioCtx) audioCtx.close();
            audioCtx = null;
            micWorklet = null;
            analyserNode = null;
        }

        // ------------------------------------------------------------------
        // Ring the Host
        //
        // This just asks the server to ring -- the actual ring happens on
        // the phone running the server itself (via Termux:API), not in any
        // browser, so it works whether or not anyone has this page open.
        // This button only tracks "did my request go through / get
        // answered", nothing more.
        // ------------------------------------------------------------------
        function ringHost() {
            if (!ws || ws.readyState !== WebSocket.OPEN) return;
            ringBtn.disabled = true;
            ringBtn.textContent = 'RINGING...';
            showRingStatus('Ringing the host phone (up to 15s)...');
            ws.send(JSON.stringify({ type: 'ring' }));
        }

        function showRingStatus(text) {
            ringStatus.textContent = text;
            ringStatus.style.display = text ? 'block' : 'none';
        }

        function resetRingButton() {
            ringBtn.disabled = false;
            ringBtn.innerHTML = '&#128276; RING THE HOST';
        }

        // Smooth 3-stage talk permit chirp
        function playPreambleTone() {
            if (!audioCtx) return;
            const now = audioCtx.currentTime;
            const osc = audioCtx.createOscillator();
            const gain = audioCtx.createGain();

            osc.type = 'sine';
            osc.frequency.setValueAtTime(880, now);
            osc.frequency.setValueAtTime(1244, now + 0.040);
            osc.frequency.setValueAtTime(1760, now + 0.080);

            gain.gain.setValueAtTime(0.55, now);
            gain.gain.setValueAtTime(0.55, now + 0.110);
            gain.gain.exponentialRampToValueAtTime(0.001, now + 0.140);

            osc.connect(gain);
            gain.connect(audioCtx.destination);

            osc.start(now);
            osc.stop(now + 0.140);
        }

        function playCourtesyBeep() {
            if (!audioCtx) return;
            const now = audioCtx.currentTime;
            const osc = audioCtx.createOscillator();
            const gain = audioCtx.createGain();

            osc.type = 'sine';
            osc.frequency.setValueAtTime(800, now);

            gain.gain.setValueAtTime(0.35, now);
            gain.gain.exponentialRampToValueAtTime(0.001, now + 0.06);

            osc.connect(gain);
            gain.connect(audioCtx.destination);

            osc.start(now);
            osc.stop(now + 0.06);
        }

        async function startAudioPipeline() {
            if (audioCtx) {
                return; // already running (e.g. resumed quickly after a drop)
            }
            audioCtx = new (window.AudioContext || window.webkitAudioContext)({ sampleRate: SAMPLE_RATE });
            await audioCtx.resume();
            try {
                micStream = await navigator.mediaDevices.getUserMedia({
                    audio: {
                        echoCancellation: true,
                        noiseSuppression: true,
                        autoGainControl: true,
                        channelCount: 1,
                        sampleRate: { ideal: SAMPLE_RATE },
                    }
                });

                const source = audioCtx.createMediaStreamSource(micStream);

                analyserNode = audioCtx.createAnalyser();
                analyserNode.fftSize = 64;
                source.connect(analyserNode);

                const workletBlob = new Blob([MIC_WORKLET_SRC], { type: 'application/javascript' });
                const workletUrl = URL.createObjectURL(workletBlob);
                await audioCtx.audioWorklet.addModule(workletUrl);
                URL.revokeObjectURL(workletUrl);

                micWorklet = new AudioWorkletNode(audioCtx, 'mic-capture-processor');
                micWorklet.port.onmessage = (e) => {
                    if (isTransmitting && ws && ws.readyState === WebSocket.OPEN) {
                        ws.send(e.data);
                    }
                };
                source.connect(micWorklet);
                micWorklet.connect(audioCtx.destination); // silent -- keeps the node alive, no feedback

                updateVUMeter();
            } catch (err) {
                alert('Audio hardware error: ' + err.message);
                leaveSession(); // bail back to the lobby cleanly rather than a stuck half-state
            }
        }

        function updateVUMeter() {
            if (!analyserNode) return;
            const dataArray = new Uint8Array(analyserNode.frequencyBinCount);
            analyserNode.getByteFrequencyData(dataArray);

            let sum = 0;
            for (let i = 0; i < dataArray.length; i++) sum += dataArray[i];
            const average = sum / dataArray.length;
            const percentage = Math.min(100, Math.round((average / 128) * 100));

            vuBar.style.width = percentage + '%';
            vuValue.textContent = percentage + '%';

            requestAnimationFrame(updateVUMeter);
        }

        // Jitter buffer + anti-backlog guard.
        //
        // Incoming packets are NOT simply queued back-to-back with no
        // ceiling -- if packets piled up during a drop (backgrounded tab,
        // brief wifi loss) and then arrived in a burst, that would happily
        // queue seconds of stale audio and play the whole backlog back,
        // seconds behind live.
        //
        // Instead, once the schedule drifts more than MAX_BACKLOG_SEC ahead
        // of real time, incoming packets are silently dropped (not queued
        // further) until it's back under the cap. Always prefer "live" over
        // "complete".
        function playReceivedPCM(arrayBuffer) {
            if (!audioCtx) return;
            const now = audioCtx.currentTime;

            if (nextStartTime - now > MAX_BACKLOG_SEC) {
                return; // dropping stale backlog audio
            }

            const pcm16 = new Int16Array(arrayBuffer);
            const float32 = new Float32Array(pcm16.length);
            for (let i = 0; i < pcm16.length; i++) float32[i] = pcm16[i] / 32768.0;

            const buffer = audioCtx.createBuffer(1, float32.length, SAMPLE_RATE);
            buffer.getChannelData(0).set(float32);

            const source = audioCtx.createBufferSource();
            source.buffer = buffer;
            source.connect(audioCtx.destination);

            if (nextStartTime < now + 0.01) {
                nextStartTime = now + JITTER_BUFFER_SEC;
            }
            source.start(nextStartTime);
            nextStartTime += buffer.duration;
        }

        function startTransmission() {
            if (isTransmitting) return;
            isTransmitting = true;
            if (micWorklet) micWorklet.port.postMessage({ type: 'set-active', active: true });

            playPreambleTone();
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'tx_start' }));
            }

            talkBtn.classList.add('transmitting');
            talkLabel.textContent = 'TRANSMITTING';
        }

        function stopTransmission() {
            if (!isTransmitting) return;
            isTransmitting = false;
            if (micWorklet) micWorklet.port.postMessage({ type: 'set-active', active: false });

            playCourtesyBeep();
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: 'tx_stop' }));
            }

            talkBtn.classList.remove('transmitting');
            talkLabel.textContent = currentMode === 'ptt' ? 'HOLD TO TALK' : 'TAP TO TRANSMIT';
        }

        function setMode(mode) {
            stopTransmission();
            currentMode = mode;
            if (mode === 'ptt') {
                pttBtn.classList.add('active');
                alwaysBtn.classList.remove('active');
                talkLabel.textContent = 'HOLD TO TALK';
            } else {
                alwaysBtn.classList.add('active');
                pttBtn.classList.remove('active');
                talkLabel.textContent = 'TAP TO TRANSMIT';
            }
        }

        const handlePTTStart = (e) => {
            if (currentMode === 'ptt') {
                e.preventDefault();
                startTransmission();
            }
        };

        const handlePTTEnd = (e) => {
            if (currentMode === 'ptt') {
                e.preventDefault();
                stopTransmission();
            }
        };

        talkBtn.addEventListener('mousedown', handlePTTStart);
        talkBtn.addEventListener('mouseup', handlePTTEnd);
        talkBtn.addEventListener('mouseleave', handlePTTEnd);

        talkBtn.addEventListener('touchstart', handlePTTStart);
        talkBtn.addEventListener('touchend', handlePTTEnd);
        talkBtn.addEventListener('touchcancel', handlePTTEnd);

        talkBtn.addEventListener('click', () => {
            if (currentMode === 'always') {
                if (isTransmitting) stopTransmission();
                else startTransmission();
            }
        });

        // Extra safety net: a backgrounded/locked tab is exactly the
        // scenario that used to cause a burst of stale audio on return.
        // Force an immediate resync the moment the tab is visible again,
        // instead of relying only on the per-packet backlog cap to
        // gradually catch up.
        document.addEventListener('visibilitychange', () => {
            if (!document.hidden && audioCtx) {
                nextStartTime = 0;
            }
        });

        setStatus('initializing');
        connectWebSocket();
    </script>
</body>
</html>
"""


@routes.get('/')
async def index(request):
    return web.Response(text=HTML_PAGE, content_type='text/html')


@routes.get('/ws')
async def websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=MAX_WS_MSG_SIZE)
    await ws.prepare(request)

    ip = request.remote or "unknown"
    conn = ConnState(ws, ip)
    connections[ws] = conn
    log_connect(ip)

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                await handle_text(conn, msg.data)
            elif msg.type == web.WSMsgType.BINARY:
                await handle_binary(conn, msg.data)
            elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSE, web.WSMsgType.CLOSING):
                break
    except Exception:
        log.exception("Unhandled error in websocket handler")
    finally:
        await cleanup_connection(conn)

    return ws


# --------------------------------------------------------------------------
# TLS certificate handling
#
# ssl_context.load_cert_chain('cert.pem', 'key.pem') raises an unhandled
# FileNotFoundError and crashes with a raw traceback if the files don't
# exist. This checks first and, if OpenSSL is available, generates a
# self-signed cert automatically; otherwise it prints clear instructions
# and exits cleanly instead of crashing.
# --------------------------------------------------------------------------

def ensure_certificates(cert_path: str = "cert.pem", key_path: str = "key.pem") -> tuple[str, str]:
    if os.path.exists(cert_path) and os.path.exists(key_path):
        return cert_path, key_path

    log.warning("TLS certificate not found — attempting to generate a self-signed one...")
    try:
        subprocess.run(
            [
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", key_path, "-out", cert_path,
                "-days", "365", "-nodes",
                "-subj", "/CN=tacticom.local",
            ],
            check=True,
            capture_output=True,
        )
        log.info("Self-signed certificate generated: %s / %s", cert_path, key_path)
        return cert_path, key_path
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        log.error("Could not auto-generate a certificate (%s).", exc)
        log.error("Install OpenSSL, or generate cert.pem/key.pem manually, then re-run:")
        log.error(
            '  openssl req -x509 -newkey rsa:2048 -keyout key.pem '
            '-out cert.pem -days 365 -nodes -subj "/CN=tacticom.local"'
        )
        sys.exit(1)


app = web.Application()
app.add_routes(routes)


async def _on_app_shutdown(app: web.Application) -> None:
    # Cancel any in-flight ring so its Termux notification doesn't linger
    # on the phone after the script has already quit.
    if active_ring is not None:
        active_ring["task"].cancel()
        await clear_ring_notification()


app.on_shutdown.append(_on_app_shutdown)

if __name__ == '__main__':
    cert_path, key_path = ensure_certificates()

    ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    ssl_context.load_cert_chain(cert_path, key_path)

    local_ip = get_local_ip()
    print_banner(local_ip)

    try:
        # access_log=None turns off aiohttp's default per-request log line
        # (the raw "IP [date] "GET /ws HTTP/1.1" 101 ..." noise on every
        # connect) -- the log_* functions above replace it with readable
        # join/leave lines instead. print=None turns off aiohttp's own
        # startup banner since print_banner() already covers that.
        web.run_app(app, host='0.0.0.0', port=8443, ssl_context=ssl_context,
                    print=None, access_log=None)
    except KeyboardInterrupt:
        pass
    finally:
        # web.run_app() catches Ctrl+C internally and returns normally, so
        # this finally block is what actually prints on shutdown -- instead
        # of the raw traceback / silent exit from before.
        print_shutdown()
 
