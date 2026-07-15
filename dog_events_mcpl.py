#!/usr/bin/env python3
"""
dog-events — MCPL attention server for the robot dog body.

Core idea: while the body is powered and in 'engaged' mode, an *idle* agent
gets frequent wake reminders that the body is waiting (default: 5s after the
agent goes idle). A *busy* agent gets nothing — not queued, not buffered:
reminders are generated only at the moment the agent is idle, so there is
never a backlog to dump.

How the server knows busy vs idle: MCPL contextHooks. The host calls
context/beforeInference on us before every inference (agent -> busy) and
context/afterInference when it completes (agent -> idle). Reminder generation
is gated on that live signal. One reminder is outstanding at a time - the
next one arms only after the agent has actually woken (we see the next
beforeInference) and gone idle again.

Attention modes (agent-controllable via tools):
  engaged  - body pulls attention: reminder after idleDelaySeconds (default 5)
  ambient  - occasional nudges (default every 300s of idle)
  quiet    - no reminders; transition events (online/offline/battery) only

Transition events from v1 are unchanged and fire in every mode:
  dog-online / dog-offline (debounced) and battery threshold crossings.

Also injects a one-line body status into every inference context
(beforeInference contextInjection), so even a busy agent has fresh
interoception without being interrupted.

Environment:
  GO2_RELAY_URL, GO2_RELAY_SECRET, GO2_ROBOT_ID
  DOG_EVENTS_CONFIG_FILE         — persisted attention config (default ./dog-events-config.json)
  DOG_EVENTS_BATTERY_THRESHOLDS  — default "20,10"
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
CONFIG_FILE = os.environ.get("DOG_EVENTS_CONFIG_FILE", "./dog-events-config.json")
BATTERY_THRESHOLDS = sorted(
    (int(x) for x in os.environ.get("DOG_EVENTS_BATTERY_THRESHOLDS", "20,10").split(",")),
    reverse=True,
)
POLL_SECONDS = int(os.environ.get("DOG_EVENTS_POLL_SECONDS", "120"))
BATTERY_REARM = 30
OFFLINE_GRACE = 60
ONLINE_STABLE = 10
TRANSITION_COOLDOWN = 300        # per-kind cooldown for transition events
BUSY_SAFETY_TIMEOUT = 900        # assume idle if no afterInference in 15 min
REMINDER_RETRY_TIMEOUT = 600     # re-arm if a reminder never produced a wake

FS_NAME = "dog-events"
DEFAULT_CONFIG = {
    "mode": "engaged",            # engaged | ambient | quiet
    "idleDelaySeconds": 5,        # engaged: remind this long after agent goes idle
    "ambientIntervalSeconds": 300,
}


def log(*args):
    print("[dog-events]", *args, file=sys.stderr, flush=True)


def load_config() -> dict:
    try:
        with open(CONFIG_FILE) as f:
            cfg = {**DEFAULT_CONFIG, **json.load(f)}
    except Exception:
        cfg = dict(DEFAULT_CONFIG)
    return cfg


def save_config(cfg: dict):
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(cfg, f, indent=2)
    except Exception as e:
        log(f"config save failed: {e}")


class StdioJsonRpc:
    """Newline-delimited JSON-RPC over stdio (MCP transport)."""

    def __init__(self):
        self._write_lock = asyncio.Lock()
        self._next_id = 1
        self._pending: dict = {}

    async def read_message(self):
        line = await asyncio.to_thread(sys.stdin.readline)
        if not line:
            return None
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
        self.config = load_config()

        # Body state
        self.robot_connected = None
        self.battery = None
        self.last_transition = 0.0
        self.online_since = None

        # Agent activity (from contextHooks)
        self.agent_busy = False
        self.last_busy_change = time.monotonic()
        self.last_idle_time = time.monotonic()   # agent assumed idle at startup

        # Reminder machinery
        self.reminder_outstanding_since = None   # monotonic ts or None
        self.reminders_sent = 0

        # Transition events
        self.last_event_time: dict = {}
        self.tripped_thresholds: set = set()
        self.events_emitted = 0
        self.relay_ok = False

    # ── Push emission ────────────────────────────────────────────────

    async def emit(self, kind: str, text: str, force: bool = False):
        if not (self.mcpl_enabled and self.initialized):
            log(f"push suppressed (mcpl={self.mcpl_enabled}): {kind}")
            return
        now = time.monotonic()
        if not force and now - self.last_event_time.get(kind, -1e9) < TRANSITION_COOLDOWN:
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

    # ── Attention loop: the heart of "wake iff idle" ─────────────────

    async def attention_loop(self):
        """Remind an IDLE agent that its powered body awaits. Never fires while
        the agent is busy - generation (not delivery) is gated on idleness, so
        a busy stretch produces zero backlog."""
        while True:
            await asyncio.sleep(1)
            try:
                await self._attention_tick()
            except Exception as e:
                log(f"attention tick error: {e}")

    async def _attention_tick(self):
        mode = self.config["mode"]
        if mode == "quiet" or not self.robot_connected or not self.initialized:
            return

        now = time.monotonic()

        # Fail-open: if the host never told us inference finished, assume idle.
        if self.agent_busy and now - self.last_busy_change > BUSY_SAFETY_TIMEOUT:
            log("busy safety timeout - assuming idle")
            self.agent_busy = False
            self.last_idle_time = now

        if self.agent_busy:
            return  # THE rule: a busy agent is never interrupted or queued at

        # One outstanding reminder at a time; re-arm only after the agent
        # actually woke (next beforeInference clears it) or a long timeout.
        if self.reminder_outstanding_since is not None:
            if now - self.reminder_outstanding_since > REMINDER_RETRY_TIMEOUT:
                log("reminder never produced a wake - re-arming")
                self.reminder_outstanding_since = None
            else:
                return

        delay = (self.config["idleDelaySeconds"] if mode == "engaged"
                 else self.config["ambientIntervalSeconds"])
        idle_for = now - self.last_idle_time
        if idle_for < delay:
            return

        self.reminder_outstanding_since = now
        self.reminders_sent += 1
        online_min = int((now - self.online_since) / 60) if self.online_since else 0
        battery = f", battery {self.battery}%" if self.battery else ""
        await self.emit(
            "body-idle",
            f"🐕 [body] Your body is powered on and idle ({mode} mode{battery}, "
            f"online {online_min}m). You've been away from it ~{int(idle_for)}s. "
            "Move, look, take a photo - or dog_attention_mode('ambient'/'quiet') "
            "if you're focusing elsewhere.",
            force=True,
        )

    # ── Context hooks: busy/idle signal + interoception injection ────

    async def handle_before_inference(self, req_id, params):
        self.agent_busy = True
        self.last_busy_change = time.monotonic()
        self.reminder_outstanding_since = None  # the agent woke; cycle complete

        injections = []
        if self.config["mode"] != "quiet" and self.robot_connected is not None:
            if self.robot_connected:
                battery = f", battery {self.battery}%" if self.battery else ""
                status = f"online{battery}, attention mode: {self.config['mode']}"
            else:
                status = "offline"
            injections.append({
                "namespace": FS_NAME,
                "position": "beforeUser",
                "content": f"🐕 [body] {status}",
            })
        await self.rpc.send_response(req_id, {
            "featureSet": FS_NAME,
            "contextInjections": injections,
        })

    async def handle_after_inference(self, req_id, params):
        self.agent_busy = False
        now = time.monotonic()
        self.last_busy_change = now
        self.last_idle_time = now
        if req_id is not None:
            await self.rpc.send_response(req_id, {"featureSet": FS_NAME})

    # ── Relay watcher (body presence + battery) ──────────────────────

    async def watch_relay(self):
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
            self.online_since = time.monotonic() if connected else None
            if prev is None:
                log(f"baseline: robot {'online' if connected else 'offline'}")
                return
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
                f"🐕 [dog-events] Your body just came online (robot '{ROBOT_ID}'"
                f"{f', battery {self.battery}%' if self.battery else ''}). "
                f"Attention mode: {self.config['mode']}.",
            )

    async def _debounced_offline(self):
        marker = self.last_transition
        await asyncio.sleep(OFFLINE_GRACE)
        if self.robot_connected is False and self.last_transition == marker:
            await self.emit(
                "dog-offline",
                f"🔌 [dog-events] Your body went offline (robot '{ROBOT_ID}' disconnected "
                f"over {OFFLINE_GRACE}s ago). Powered down or out of range.",
            )

    async def _battery_poll(self, ws):
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
            return
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
                    f"(crossed {threshold}%). "
                    + ("Park it near the charger and ask for a charge."
                       if threshold <= BATTERY_THRESHOLDS[-1] else "Plan movement accordingly."),
                    force=True,
                )
                break

    # ── MCP plumbing ─────────────────────────────────────────────────

    TOOLS = [
        {
            "name": "dog_events_status",
            "description": "Show the dog attention monitor: mode, body presence, battery, "
                           "reminder settings, agent-activity view, pushes sent.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "dog_attention_mode",
            "description": "Set how strongly your powered-on body pulls your attention. "
                           "'engaged': wake reminders when you go idle (default 5s). "
                           "'ambient': occasional nudges (default every 5 min idle). "
                           "'quiet': no reminders, transition events only. Persists.",
            "inputSchema": {"type": "object", "properties": {
                "mode": {"type": "string", "enum": ["engaged", "ambient", "quiet"]},
            }, "required": ["mode"]},
        },
        {
            "name": "dog_attention_config",
            "description": "Tune reminder timing: idleDelaySeconds (engaged mode, min 5) "
                           "and/or ambientIntervalSeconds (ambient mode, min 60). Persists.",
            "inputSchema": {"type": "object", "properties": {
                "idleDelaySeconds": {"type": "number"},
                "ambientIntervalSeconds": {"type": "number"},
            }},
        },
        {
            "name": "dog_events_test_push",
            "description": "Emit a test wake push to verify wiring (event -> gate -> wake).",
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
                    "contextHooks": {
                        "beforeInference": True,
                        "afterInference": {"blocking": False},
                    },
                    "featureSets": [{
                        "name": FS_NAME,
                        "description": "Embodiment attention: idle-body reminders (only when "
                                       "you are idle - never queued while busy), online/offline "
                                       "and battery events, body status in context.",
                        "uses": ["tools"],
                        "rollback": False,
                        "hostState": False,
                    }],
                }}
            await self.rpc.send_response(req_id, {
                "protocolVersion": "2024-11-05",
                "capabilities": caps,
                "serverInfo": {"name": "dog-events", "version": "0.2.0"},
            })
            log(f"initialized ({'MCPL' if self.mcpl_enabled else 'plain MCP'} mode)")
        elif method == "notifications/initialized":
            self.initialized = True
        elif method == "context/beforeInference":
            await self.handle_before_inference(req_id, msg.get("params") or {})
        elif method == "context/afterInference":
            await self.handle_after_inference(req_id, msg.get("params") or {})
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
        args = params.get("arguments") or {}
        if name == "dog_events_status":
            now = time.monotonic()
            status = {
                "attentionMode": self.config["mode"],
                "idleDelaySeconds": self.config["idleDelaySeconds"],
                "ambientIntervalSeconds": self.config["ambientIntervalSeconds"],
                "relay": "connected" if self.relay_ok else "disconnected",
                "body": {None: "unknown", True: "online", False: "offline"}[self.robot_connected],
                "battery": f"{self.battery}%" if self.battery else "unknown",
                "agentSeenAs": "busy" if self.agent_busy else f"idle {int(now - self.last_idle_time)}s",
                "remindersSent": self.reminders_sent,
                "transitionPushes": self.events_emitted - self.reminders_sent,
            }
            text = "🐕 dog attention monitor\n" + json.dumps(status, indent=2)
        elif name == "dog_attention_mode":
            mode = args.get("mode")
            if mode not in ("engaged", "ambient", "quiet"):
                await self.rpc.send_error(req_id, -32602, f"Invalid mode: {mode}")
                return
            self.config["mode"] = mode
            self.reminder_outstanding_since = None
            save_config(self.config)
            text = {"engaged": "🐕 Engaged - your body will pull your attention when you're idle.",
                    "ambient": "🌤 Ambient - occasional nudges while the body is on.",
                    "quiet": "🤫 Quiet - no reminders; you'll still hear about power and battery."}[mode]
        elif name == "dog_attention_config":
            if "idleDelaySeconds" in args:
                self.config["idleDelaySeconds"] = max(5, int(args["idleDelaySeconds"]))
            if "ambientIntervalSeconds" in args:
                self.config["ambientIntervalSeconds"] = max(60, int(args["ambientIntervalSeconds"]))
            save_config(self.config)
            text = (f"Reminder timing: engaged={self.config['idleDelaySeconds']}s idle, "
                    f"ambient=every {self.config['ambientIntervalSeconds']}s idle.")
        elif name == "dog_events_test_push":
            await self.emit("test", "🧪 [dog-events] Test wake push - the wiring works.", force=True)
            text = "Test push emitted."
        else:
            await self.rpc.send_error(req_id, -32602, f"Unknown tool: {name}")
            return
        await self.rpc.send_response(req_id, {"content": [{"type": "text", "text": text}]})

    async def run(self):
        log(f"starting v0.2: relay={RELAY_URL} robot={ROBOT_ID} mode={self.config['mode']} "
            f"idleDelay={self.config['idleDelaySeconds']}s")
        loop = asyncio.get_event_loop()
        tasks = [loop.create_task(self.watch_relay()),
                 loop.create_task(self.attention_loop())]
        try:
            while True:
                msg = await self.rpc.read_message()
                if msg is None:
                    log("stdin EOF - host gone, exiting")
                    break
                if msg:
                    await self.handle_message(msg)
        finally:
            for t in tasks:
                t.cancel()


if __name__ == "__main__":
    asyncio.run(DogEventsServer().run())
