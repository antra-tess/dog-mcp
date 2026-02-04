# 🐕 dog-mcp

**MCP server for AI embodiment via Unitree Go2 robot dog.**

This isn't a remote control interface - it's a body. The MCP tools give an AI model proprioception, spatial awareness, and motor control over a physical robot. The model decides when and how to move based on its own judgment, not just user commands.

## Philosophy

Traditional robot APIs treat the robot as a tool to be commanded. This project takes a different approach: the robot is the AI's *body*, and the MCP interface is how it perceives and acts in physical space.

The tool descriptions are written from the model's perspective:
- "Move the body forward" not "Command the robot to move"
- "My spatial awareness via LiDAR" not "Robot sensor data"
- "Take a photo from my front camera" not "Capture image from robot"

This framing matters. When an AI has a body, it should exercise judgment about physical actions - evaluating safety, appropriateness, and its own interests - rather than blindly executing commands.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        AI Model                              │
│                   (Claude, etc.)                             │
└─────────────────────────┬───────────────────────────────────┘
                          │ MCP Protocol
                          ▼
┌─────────────────────────────────────────────────────────────┐
│                   go2_mcp_server_v2.py                       │
│                    (MCP Server)                              │
│  Tools: move, turn, look, take_photo, get_state, etc.       │
│  Resources: position, orientation, velocity, LiDAR map      │
└─────────────────────────┬───────────────────────────────────┘
                          │ WebSocket
          ┌───────────────┴───────────────┐
          │                               │
          ▼ (Direct)                      ▼ (Remote)
┌─────────────────────┐         ┌─────────────────────┐
│  go2_server_v2.py   │         │   Cloud Relay       │
│  (Local WS Server)  │◄───────►│  (relay_bridge.py)  │
│  Runs on robot LAN  │         │  Runs alongside     │
└─────────┬───────────┘         └─────────────────────┘
          │ WebRTC
          ▼
┌─────────────────────────────────────────────────────────────┐
│                    Unitree Go2 Robot                         │
│         IMU • Odometry • LiDAR • Camera • Motors            │
└─────────────────────────────────────────────────────────────┘
```

## Features

### 🎯 Closed-Loop Movement Control

Unlike open-loop "move for N seconds" approaches, this server uses IMU and odometry feedback for accurate positioning:

- **Move**: Target distance in meters, tracks actual distance traveled
- **Turn**: Target angle in degrees, tracks actual rotation via IMU
- **Stall detection**: Automatically detects when robot is blocked by obstacles
- **Progress streaming**: Real-time feedback during movement commands

```python
# The robot moves until it actually travels 0.5m (not just sends velocity for estimated time)
result = await robot.move(distance=0.5, speed=0.3)
# Returns: {"status": "completed", "data": {"target": 0.5, "actual": 0.498}}
```

### 🗺️ LiDAR Obstacle Mapping

Real-time ASCII visualization of the robot's spatial awareness:

```
           ┌─ FORWARD (2.0m) ─┐
     LEFT  │··················│  RIGHT
    ┌──────────────────────────────┐
1.5m│··············│··············│
    │··············│··············│
 1m │····●●●●······│··············│
    │···●●●●●●·····│··············│
.5m │···●●●●●●●····│··············│
    │·····═══▲═════│══════════════│
 ◈  │·····║░░░░░░║·│··············│
    │·····═════════│══════════════│
back│··············│··············│
    └──────────────────────────────┘
              BEHIND (0.5m)

  · clear  ∘○● obstacle  ◉█ wall  ─┄ distance  ◈ me
```

- Asymmetric grid: 2m forward visibility, 0.5m behind
- Height-filtered to ignore floor/ceiling
- Updates in real-time as robot moves
- Available as MCP resource or via `get_state`

### 👀 Look Mode (Body Tilt)

Tilt the robot's body to point the camera without moving:

```python
# Look up and to the left
await robot.look(yaw=20, pitch=30, hold=True)

# Quick glance then return to neutral
await robot.look(yaw=-30, pitch=0, hold=False)
```

Uses the robot's Euler control mode for smooth body orientation.

### 📸 Camera Integration

Capture photos from the robot's front camera:

```python
result = await robot.take_photo()
# Returns base64 JPEG in result["data"]["image"]
```

Images are returned as base64-encoded JPEG via the MCP ImageContent type.

### 🎭 Emotes & Poses

Physical expression through pre-programmed animations:

**Emotes** (one-shot animations):
- `wave`, `hello`, `nod`, `shake`
- `dance`, `dance1`, `dance2`
- `stretch`, `wiggle`, `fingerheart`
- `moonwalk`, `handstand`, `frontflip`, `backflip`

**Poses** (sustained positions):
- `sit`, `stand`, `lie`/`down`

### 🌐 Cloud Relay (Remote Access)

Access the robot from anywhere without port forwarding:

1. **On the robot's network**: Run `relay_bridge.py` alongside the local WebSocket server
2. **Deploy relay server**: Any WebSocket-capable host (e.g., Railway, Fly.io)
3. **Connect remotely**: MCP server connects via relay URL

```bash
# On robot LAN
GO2_RELAY_URL=wss://your-relay.railway.app \
GO2_RELAY_SECRET=your-secret \
python relay_bridge.py

# Anywhere (MCP server)
GO2_USE_RELAY=true \
GO2_RELAY_URL=wss://your-relay.railway.app \
GO2_RELAY_SECRET=your-secret \
python go2_mcp_server_v2.py
```

### 🖥️ Web UI

Browser-based control interface (`web_ui/index.html`):

- Real-time position/orientation display
- Live LiDAR obstacle map
- Movement controls (forward/back/turn)
- Look mode with presets
- Camera feed with auto-capture
- Emote and pose buttons
- Command progress tracking

## Installation

### Dependencies

```bash
pip install mcp websockets pydantic unitree-webrtc-connect
```

The `unitree-webrtc-connect` package provides WebRTC connectivity to Unitree robots.

### Robot Setup

1. Connect your computer to the Go2's network (AP mode or same LAN)
2. Find the robot's serial number (on the robot or in the Unitree app)
3. Run the local server:

```bash
python go2_server_v2.py B42D1000XXXXXXXX  # Your serial number
```

### MCP Server Setup

Add to your MCP client configuration:

```json
{
  "mcpServers": {
    "go2-robot": {
      "command": "python",
      "args": ["/path/to/dog_mcp/go2_mcp_server_v2.py"],
      "env": {
        "GO2_USE_RELAY": "false"
      }
    }
  }
}
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GO2_USE_RELAY` | Use cloud relay instead of direct connection | `true` |
| `GO2_RELAY_URL` | WebSocket URL of relay server | `wss://...railway.app` |
| `GO2_RELAY_SECRET` | Authentication token for relay | (empty) |
| `GO2_ROBOT_ID` | Robot identifier for relay routing | `go2-home` |
| `GO2_LOCAL_WS` | Local WebSocket server URL (for relay bridge) | `ws://localhost:8765` |

## MCP Tools

| Tool | Description |
|------|-------------|
| `move` | Move forward/backward with distance feedback |
| `turn` | Turn left/right with angle feedback |
| `look` | Tilt body to look in a direction |
| `get_state` | Get full proprioceptive state |
| `get_obstacle_map` | Get LiDAR spatial awareness map |
| `take_photo` | Capture photo from front camera |
| `play_emote` | Play animation (wave, dance, etc.) |
| `set_pose` | Set posture (sit, stand, lie) |
| `abort` | Emergency stop |
| `set_obstacle_avoidance` | Toggle automatic collision avoidance |

## MCP Resources

| URI | Description |
|-----|-------------|
| `robot://state/position` | Current (x, y, z) position in meters |
| `robot://state/orientation` | Current (roll, pitch, yaw) in degrees |
| `robot://state/velocity` | Current velocity vector |
| `robot://state/full` | Complete state including battery, mode |
| `robot://lidar/obstacle_map` | ASCII obstacle map from LiDAR |

## Project Structure

```
dog_mcp/
├── go2_mcp_server_v2.py    # MCP server (main entry point for AI)
├── go2_server_v2.py        # WebSocket server (runs on robot LAN)
├── robot_dog_client.py     # Python client for direct connection
├── relay_bridge.py         # Bridges local server to cloud relay
├── relay_client.py         # Client for relay connection
└── web_ui/
    └── index.html          # Browser control interface
```

## Safety Notes

- The robot has built-in obstacle avoidance (can be toggled)
- Stall detection stops movement if the robot gets stuck
- Emergency stop (`abort`) immediately halts all motion
- Battery level is reported in state - watch for low battery

## Credits

- Uses [unitree-webrtc-connect](https://github.com/legion1581/unitree_webrtc_connect) for robot communication
- Part of the [Connectome](https://github.com/antra-tess/connectome-ts) project for AI embodiment

## License

MIT
