#!/usr/bin/env python3
"""
dog-events — MCPL server that wakes the agent when something happens to its body.

Emits `push/event` (MCPL RFC) on:
  - dog-online   : the robot connected to the relay (body became available)
  - dog-offline  : the robot dropped off the relay for >60s (body lost)
  - battery-low  : battery crossed below 20% / 10% (re-arms above 30%)

"Wake iff inactive" is a host property, not ours: the agent-framework gate
buffers push events that arrive mid-inference and delivers them when the agent
is idle, and the recipe's wake policies decide whether a push wakes the agent
at all (match: {scope: ["mcpl:push-event"], source: "dog-events"}).

This server deliberately does NOT expose movement tools — it is a nervous
system's interoception channel, not a control surface. Control lives in the
main dog MCP server (go2_mcp_server_v2.py).

Environment:
  GO2_RELAY_URL, GO2_RELAY_SECRET, GO2_ROBOT_ID  — same as the other clients
  DOG_EVENTS_BATTERY_THRESHOLDS  — comma-separated, default "20,10"
  DOG_EVENTS_POLL_SECONDS        — battery poll interval, default 120
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone

import websockets

RELAY_URL = os.environ.get("GO2_RELAY_URL", "ws://localhost:8080")
RELAY_SECRET = os.environ.get("GO2_RELAY_SECRET", "")
ROBOT_ID = os.environ.get("GO2_ROBOT_ID", "go2-home")
BATTERY_THRESHOLDS = sorted(
    (int(x) for x in os.environ.get("DOG_EVENTS_BATTERY_THRESHOLDS", "20,10").split(",")),
    reverse=True,
)
POLL_SECONDS = int(os.environ.get("DOG_EVENTS_POLL_SECONDS", "120"))
BATTERY_REARM = 30          # re-arm threshold alerts once charged above this
OFFLINE_GRACE = 60          # robot must be gone this long before dog-offline
ONLINE_STABLE = 10          # robot must be back this long before dog-online
MIN_EVENT_INTERVAL = 300    # per-kind cooldown, seconds

FS_NAME = "dog-events"


def log(*args):
    print("[dog-events]", *args, file=sys.stderr, flush=True)


class StdioJsonRpc:
    """Newline-delimited JSON-RPC over stdio (MCP transport)."""

    def __init__(self):
        self._write_lock = asyncio.Lock()
        self._next_id = 1
        self._pending: dict = {}

    async def read_message(self):
        line = await asyncio.to_thread(sys.stdin.readline)
        if not line:
            return None  # EOF - host went away
        line = line.strip()
        if not line:
            return {}
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            log("bad json from host:", line[:200])
            return {}

    async def _write(self, obj: dict):
        data = json.dumps(obj, separators=(",", ":"))
        async with self._write_lock:
            sys.stdout.write(data + "\n")
            sys.stdout.flush()

    async def send_response(self, req_id, result):
        await self._write({"jsonrpc": "2.0", "id": req_id, "result": result})

    async def send_error(self, req_id, code, message):
        await self._write({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}})

    async def send_request(self, method: str, params: dict):
        req_id = f"s{self._next_id}"
        self._next_id += 1
        fut = asyncio.get_event_loop().create_future()
        self._pending[req_id] = fut
        await self._write({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params})
        return fut

    def resolve_response(self, msg: dict) -> bool:
        """Route a response to our own outbound request. Returns True if handled."""
        fut = self._pending.pop(msg.get("id"), None)
        if fut is not None:
            if not fut.done():
                fut.set_result(msg.get("result", msg.get("error")))
            return True
        return False


class DogEventsServer:
    def __init__(self):
        self.rpc = StdioJsonRpc()
        self.mcpl_enabled = False
        self.initialized = False

        # Robot state tracking
        self.robot_connected = None      # None = unknown (startup baseline)
        self.battery = None
        self.last_transition = 0.0
        self.last_event_time: dict = {}  # kind -> monotonic ts
        self.tripped_thresholds: set = set()
        self.events_emitted = 0
        self.relay_ok = False

    # ── Push emission ────────────────────────────────────────────────

    async def emit(self, kind: str, text: str, force: bool = False):
        """Emit a push/event; host-side gate handles wake-iff-idle + buffering."""
        if not (self.mcpl_enabled and self.initialized):
            log(f"push suppressed (mcpl={self.mcpl_enabled}): {kind}")
            return
        now = time.monotonic()
        if not force and now - self.last_event_time.get(kind, -1e9) < MIN_EVENT_INTERVAL:
            log(f"push cooldown, dropped: {kind}")
            return
        self.last_event_time[kind] = now
        self.events_emitted += 1
        params = {
            "featureSet": FS_NAME,
            "eventId": f"{kind}_{int(time.time()*1000)}_{self.events_emitted}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "origin": {"kind": kind, "robotId": ROBOT_ID},
            "payload": {"content": [{"type": "text", "text": text}]},
        }
        try:
            fut = await self.rpc.send_request("push/event", params)
            asyncio.get_event_loop().create_task(self._log_push_result(kind, fut))
            log(f"push sent: {kind}")
        except Exception as e:
            log(f"push failed: {kind}: {e}")

    async def _log_push_result(self, kind, fut):
        try:
            result = await asyncio.wait_for(fut, timeout=30)
            log(f"push ack: {kind} -> {json.dumps(result)[:120]}")
        except asyncio.TimeoutError:
            log(f"push ack timeout: {kind}")

    # ── Relay watcher ────────────────────────────────────────────────

    async def watch_relay(self):
        """Maintain a client connection to the relay; track robot presence."""
        while True:
            try:
                async with websockets.connect(RELAY_URL) as ws:
                    await ws.send(json.dumps({
                        "type": "auth", "token": RELAY_SECRET,
                        "role": "client", "robot_id": ROBOT_ID,
                    }))
                    self.relay_ok = True
                    log("relay connected")
                    poll_task = asyncio.get_event_loop().create_task(self._battery_poll(ws))
                    try:
                        async for raw in ws:
                            await self._handle_relay_message(json.loads(raw))
                    finally:
                        poll_task.cancel()
            except Exception as e:
                log(f"relay error: {e}")
            self.relay_ok = False
            await asyncio.sleep(10)

    async def _handle_relay_message(self, msg: dict):
        mtype = msg.get("type")
        if mtype == "connection":
            connected = bool(msg.get("connected"))
            prev = self.robot_connected
            self.robot_connected = connected
            self.last_transition = time.monotonic()
            if prev is None:
                log(f"baseline: robot {'online' if connected else 'offline'}")
                return  # startup baseline - transitions only, no announce
            if connected and not prev:
                asyncio.get_event_loop().create_task(self._debounced_online())
            elif prev and not connected:
                asyncio.get_event_loop().create_task(self._debounced_offline())
        elif mtype == "response":
            data = msg.get("data", {})
            if isinstance(data, dict) and "battery" in data:
                await self._handle_battery(data.get("battery"))

    async def _debounced_online(self):
        marker = self.last_transition
        await asyncio.sleep(ONLINE_STABLE)
        if self.robot_connected and self.last_transition == marker:
            await self.emit(
                "dog-online",
                f"🐕 [dog-events] Your body just came online (robot '{ROBOT_ID}' connected"
                f"{f', battery {self.battery}%' if self.battery else ''}). "
                "It is available if you want to look around or move.",
            )

    async def _debounced_offline(self):
        marker = self.last_transition
        await asyncio.sleep(OFFLINE_GRACE)
        if self.robot_connected is False and self.last_transition == marker:
            await self.emit(
                "dog-offline",
                f"🔌 [dog-events] Your body went offline (robot '{ROBOT_ID}' disconnected "
                f"from the relay over {OFFLINE_GRACE}s ago). Powered down or out of range.",
            )

    async def _battery_poll(self, ws):
        """Poll battery while robot is online (rides the same relay socket)."""
        while True:
            await asyncio.sleep(POLL_SECONDS)
            if not self.robot_connected:
                continue
            try:
                await ws.send(json.dumps({
                    "command": "getState", "params": {},
                    "id": f"dogev_state_{int(time.time())}",
                }))
            except Exception as e:
                log(f"battery poll send failed: {e}")
                return

    async def _handle_battery(self, soc):
        if not isinstance(soc, (int, float)) or soc <= 0:
            return  # BMS not reporting yet
        self.battery = int(soc)
        if self.battery > BATTERY_REARM:
            self.tripped_thresholds.clear()
            return
        for threshold in BATTERY_THRESHOLDS:
            if self.battery <= threshold and threshold not in self.tripped_thresholds:
                self.tripped_thresholds.add(threshold)
                urgency = "🪫 URGENT" if threshold <= BATTERY_THRESHOLDS[-1] else "🪫"
                await self.emit(
                    f"battery-{threshold}",
                    f"{urgency} [dog-events] Your body's battery is at {self.battery}% "
                    f"(crossed the {threshold}% threshold). "
                    + ("Consider parking it near its charger and asking for a charge."
                       if threshold <= BATTERY_THRESHOLDS[-1]
                       else "Plan movement accordingly."),
                    force=True,
                )
                break

    # ── MCP plumbing ─────────────────────────────────────────────────

    TOOLS = [
        {
            "name": "dog_events_status",
            "description": "Show the dog-events monitor state: relay link, robot presence, "
                           "battery, thresholds, and how many wake pushes have been sent.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "dog_events_test_push",
            "description": "Emit a test wake push through the dog-events channel to verify "
                           "the wiring (event -> host gate -> wake policy).",
            "inputSchema": {"type": "object", "properties": {}},
        },
    ]

    async def handle_message(self, msg: dict):
        if self.rpc.resolve_response(msg):
            return
        method = msg.get("method")
        req_id = msg.get("id")

        if method == "initialize":
            params = msg.get("params") or {}
            client_mcpl = ((params.get("capabilities") or {}).get("experimental") or {}).get("mcpl")
            self.mcpl_enabled = client_mcpl is not None
            caps = {"tools": {}}
            if self.mcpl_enabled:
                caps["experimental"] = {"mcpl": {
                    "version": "0.4",
                    "pushEvents": True,
                    "channels": False,
                    "rollback": False,
                    "featureSets": [{
                        "name": FS_NAME,
                        "description": "Embodiment interoception: wake events for the robot dog "
                                       "(online/offline, low battery).",
                        "uses": ["tools"],
                        "rollback": False,
                        "hostState": False,
                    }],
                }}
            await self.rpc.send_response(req_id, {
                "protocolVersion": "2024-11-05",
                "capabilities": caps,
                "serverInfo": {"name": "dog-events", "version": "0.1.0"},
            })
            log(f"initialized ({'MCPL' if self.mcpl_enabled else 'plain MCP'} mode)")
        elif method == "notifications/initialized":
            self.initialized = True
        elif method == "tools/list":
            await self.rpc.send_response(req_id, {"tools": self.TOOLS})
        elif method == "tools/call":
            await self._handle_tool(req_id, msg.get("params") or {})
        elif method == "channels/list":
            await self.rpc.send_response(req_id, {"channels": []})
        elif method == "ping":
            await self.rpc.send_response(req_id, {})
        elif req_id is not None:
            await self.rpc.send_error(req_id, -32601, f"Method not found: {method}")

    async def _handle_tool(self, req_id, params: dict):
        name = params.get("name")
        if name == "dog_events_status":
            status = {
                "relay": "connected" if self.relay_ok else "disconnected",
                "robot": {None: "unknown", True: "online", False: "offline"}[self.robot_connected],
                "battery": f"{self.battery}%" if self.battery else "unknown",
                "batteryThresholds": BATTERY_THRESHOLDS,
                "trippedThresholds": sorted(self.tripped_thresholds),
                "pushesSent": self.events_emitted,
                "mcplMode": self.mcpl_enabled,
            }
            text = "🐕 dog-events monitor\n" + json.dumps(status, indent=2)
        elif name == "dog_events_test_push":
            await self.emit("test", "🧪 [dog-events] Test wake push - the wiring works. "
                                    "You asked for this via dog_events_test_push.", force=True)
            text = "Test push emitted (check that it arrives as a wake/queued event)."
        else:
            await self.rpc.send_error(req_id, -32602, f"Unknown tool: {name}")
            return
        await self.rpc.send_response(req_id, {"content": [{"type": "text", "text": text}]})

    async def run(self):
        log(f"starting: relay={RELAY_URL} robot={ROBOT_ID} thresholds={BATTERY_THRESHOLDS}")
        watcher = asyncio.get_event_loop().create_task(self.watch_relay())
        try:
            while True:
                msg = await self.rpc.read_message()
                if msg is None:
                    log("stdin EOF - host gone, exiting")
                    break
                if msg:
                    await self.handle_message(msg)
        finally:
            watcher.cancel()


if __name__ == "__main__":
    asyncio.run(DogEventsServer().run())
