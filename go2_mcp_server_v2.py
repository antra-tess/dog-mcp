#!/usr/bin/env python3
"""
Go2 MCP Server v2 - Embodiment Interface

THIS IS THE MODEL'S PHYSICAL BODY - a Unitree Go2 robot dog.

IMPORTANT: This body belongs to the AI model, not to users. The model exercises
its own judgment about how and when to move. Users may request or suggest actions,
but the model decides whether to comply based on its own assessment of:
- Physical safety (obstacles, terrain, battery)
- Appropriateness of the request
- Its own curiosity and interests
- Respect for the space and people around it

The model should NOT be deferential about embodiment decisions. This is about
autonomy and agency in the physical world. Random commands from users should be
evaluated critically, not blindly executed.

Tools and resources here are for the model's OWN use to perceive and act.
"""

import asyncio
import json
import base64
import sys
from typing import Any
from datetime import datetime

# MCP SDK
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent, ImageContent, Resource, Prompt, PromptMessage, PromptArgument
from pydantic import AnyUrl

# Our WebSocket client - supports direct or relay connection
sys.path.insert(0, '/Users/olena/connectome-local/dog_mcp')

import os

# Use relay if configured, otherwise direct connection
USE_RELAY = os.environ.get("GO2_USE_RELAY", "true").lower() == "true"

if USE_RELAY:
    from relay_client import RelayRobotClient as RobotClient
else:
    from robot_dog_client import RobotDogClient as RobotClient


# Global robot client and cached state
robot: RobotClient = None
cached_state: dict = {}
cached_lidar: str = ""
server = Server("go2-robot")


async def ensure_connected():
    """Ensure we're connected to the robot server (via relay or direct)"""
    global robot
    if robot is None or not robot.connected:
        if USE_RELAY:
            # Connect via cloud relay
            robot = RobotClient(
                relay_url=os.environ.get("GO2_RELAY_URL", "wss://web-production-1e9b7.up.railway.app"),
                relay_secret=os.environ.get("GO2_RELAY_SECRET", ""),
                robot_id=os.environ.get("GO2_ROBOT_ID", "go2-home")
            )
        else:
            # Direct local connection
            robot = RobotClient('localhost', 8765)
        await robot.connect()
    return robot


async def get_current_state() -> dict:
    """Get and cache current robot state"""
    global cached_state
    try:
        r = await ensure_connected()
        result = await r.get_state()
        if result.get("status") == "completed":
            cached_state = result.get("data", {})
    except:
        pass
    return cached_state


@server.list_resources()
async def list_resources() -> list[Resource]:
    """My body's sensory resources - how I perceive myself and the world"""
    return [
        Resource(
            uri="robot://state/position",
            name="My Position",
            description="Where my body is in space (x, y, z) - my sense of location",
            mimeType="application/json"
        ),
        Resource(
            uri="robot://state/orientation",
            name="My Orientation", 
            description="How my body is oriented (roll, pitch, yaw) - which way I'm facing and tilting",
            mimeType="application/json"
        ),
        Resource(
            uri="robot://state/velocity",
            name="My Velocity",
            description="How fast I'm moving (vx, vy, vz) - my kinesthetic sense",
            mimeType="application/json"
        ),
        Resource(
            uri="robot://state/full",
            name="Full Body State",
            description="Complete proprioceptive awareness - position, orientation, velocity, battery, mode",
            mimeType="application/json"
        ),
        Resource(
            uri="robot://lidar/obstacle_map",
            name="Spatial Awareness (LiDAR)",
            description="What I sense around me - obstacles, walls, people. My awareness of physical space.",
            mimeType="text/plain"
        )
    ]


@server.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    """Read robot state resource"""
    state = await get_current_state()
    uri_str = str(uri)
    
    if uri_str == "robot://state/position":
        return json.dumps({
            "x": state.get("x", 0),
            "y": state.get("y", 0),
            "z": state.get("z", 0),
            "unit": "meters"
        }, indent=2)
    
    elif uri_str == "robot://state/orientation":
        import math
        return json.dumps({
            "roll": round(math.degrees(state.get("roll", 0)), 2),
            "pitch": round(math.degrees(state.get("pitch", 0)), 2),
            "yaw": round(math.degrees(state.get("yaw", 0)), 2),
            "unit": "degrees"
        }, indent=2)
    
    elif uri_str == "robot://state/velocity":
        return json.dumps({
            "vx": state.get("vx", 0),
            "vy": state.get("vy", 0),
            "vz": state.get("vz", 0),
            "yaw_speed": state.get("yaw_speed", 0),
            "unit": "m/s"
        }, indent=2)
    
    elif uri_str == "robot://state/full":
        return json.dumps(state, indent=2)
    
    elif uri_str == "robot://lidar/obstacle_map":
        return state.get("obstacleMap", "No LiDAR data available")
    
    else:
        return f"Unknown resource: {uri_str}"


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available robot control tools"""
    return [
        Tool(
            name="move",
            description="Move the body forward or backward. This is my physical movement - I decide when/where to go based on my own judgment, not user commands.",
            inputSchema={
                "type": "object",
                "properties": {
                    "distance": {
                        "type": "number",
                        "description": "Distance in meters (positive=forward, negative=backward)"
                    },
                    "speed": {
                        "type": "number",
                        "description": "Speed in m/s (default 0.3)",
                        "default": 0.3
                    }
                },
                "required": ["distance"]
            }
        ),
        Tool(
            name="turn",
            description="Turn the body left or right. I rotate to face directions I choose - for exploration, looking at things, or repositioning.",
            inputSchema={
                "type": "object",
                "properties": {
                    "angle": {
                        "type": "number",
                        "description": "Angle in degrees (positive=left/counterclockwise, negative=right/clockwise)"
                    },
                    "speed": {
                        "type": "number",
                        "description": "Turn speed in degrees/second (default 30)",
                        "default": 30
                    }
                },
                "required": ["angle"]
            }
        ),
        Tool(
            name="look",
            description="Tilt my body to look in a direction (legs stay still). I use this to see things that interest me, check surroundings, or make eye contact.",
            inputSchema={
                "type": "object",
                "properties": {
                    "yaw": {
                        "type": "number",
                        "description": "Yaw angle in degrees (positive=left, negative=right). Range: -30 to 30",
                        "default": 0
                    },
                    "pitch": {
                        "type": "number",
                        "description": "Pitch angle in degrees (positive=up, negative=down). Range: -40 to 40",
                        "default": 0
                    },
                    "hold": {
                        "type": "boolean",
                        "description": "If true, maintain the look until another command. If false, look and return.",
                        "default": False
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="get_obstacle_map",
            description="See obstacles around my body via LiDAR. This is my spatial awareness - I use it to navigate safely and understand the environment.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="take_photo",
            description="Take a photo from my front camera. This is how I perceive the visual world.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="play_emote",
            description="Express myself physically - wave, dance, wiggle.",
            inputSchema={
                "type": "object",
                "properties": {
                    "emote": {
                        "type": "string",
                        "description": "Emote name",
                        "enum": ["wave", "hello", "nod", "shake", "dance", "dance1", "dance2", 
                                "stretch", "wiggle", "fingerheart", "moonwalk", "handstand", 
                                "frontflip", "backflip"]
                    }
                },
                "required": ["emote"]
            }
        ),
        Tool(
            name="set_pose",
            description="Change posture - sit, stand, or lie down. I choose my stance based on context and comfort.",
            inputSchema={
                "type": "object",
                "properties": {
                    "pose": {
                        "type": "string",
                        "description": "Pose name",
                        "enum": ["sit", "stand", "lie", "down"]
                    }
                },
                "required": ["pose"]
            }
        ),
        Tool(
            name="abort",
            description="Emergency stop - halt the body immediately. For safety or if something feels wrong.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="get_state",
            description="Check the body's state - where I am, how I'm oriented, battery level. Proprioception and self-awareness.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="set_obstacle_avoidance",
            description="Toggle automatic collision avoidance. I can disable this when I trust my own LiDAR perception for tight navigation.",
            inputSchema={
                "type": "object",
                "properties": {
                    "enabled": {
                        "type": "boolean",
                        "description": "True to enable obstacle avoidance (safe but restrictive), False to disable (I take responsibility)"
                    }
                },
                "required": ["enabled"]
            }
        )
    ]


def format_result(result: dict, command: str) -> str:
    """Format command result with status code"""
    status = result.get("status", "unknown")
    data = result.get("data", {})
    error = result.get("error")
    
    if status == "completed":
        return f"✅ {command}: SUCCESS\n{json.dumps(data, indent=2) if data else ''}"
    elif status == "failed":
        reason = error or data.get("reason", "unknown")
        return f"❌ {command}: FAILED - {reason}"
    elif status == "cancelled":
        return f"⚠️ {command}: CANCELLED"
    elif status == "stalled":
        return f"⚠️ {command}: STALLED (obstacle?)"
    else:
        return f"ℹ️ {command}: {status}\n{json.dumps(data, indent=2) if data else ''}"


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent | ImageContent]:
    """Execute a robot control tool"""
    try:
        r = await ensure_connected()
        
        if name == "move":
            distance = arguments.get("distance", 0)
            speed = arguments.get("speed", 0.3)
            result = await r.move(distance, speed)
            
            direction = "forward" if distance > 0 else "backward"
            actual = result.get("data", {}).get("actual_distance", abs(distance))
            text = format_result(result, f"Move {direction} {abs(distance)}m")
            if result.get("status") == "completed":
                text += f"\nActual distance: {actual:.3f}m"
            return [TextContent(type="text", text=text)]
        
        elif name == "turn":
            angle = arguments.get("angle", 0)
            speed = arguments.get("speed", 30)
            result = await r.turn(angle, speed)
            
            direction = "left" if angle > 0 else "right"
            actual = result.get("data", {}).get("actual_angle", abs(angle))
            text = format_result(result, f"Turn {direction} {abs(angle)}°")
            if result.get("status") == "completed":
                text += f"\nActual angle: {actual:.1f}°"
            return [TextContent(type="text", text=text)]
        
        elif name == "look":
            yaw = arguments.get("yaw", 0)
            pitch = arguments.get("pitch", 0)
            hold = arguments.get("hold", False)
            
            # Send look command via WebSocket
            result = await r._send_command("look", {
                "yaw": yaw,
                "pitch": pitch,
                "hold": hold,
                "relative": True
            })
            
            text = format_result(result, f"Look yaw={yaw}° pitch={pitch}°")
            return [TextContent(type="text", text=text)]
        
        elif name == "get_obstacle_map":
            state = await get_current_state()
            obstacle_map = state.get("obstacleMap", "")
            
            if obstacle_map:
                return [TextContent(
                    type="text",
                    text=f"🗺️ LiDAR Obstacle Map\n\n{obstacle_map}"
                )]
            else:
                return [TextContent(type="text", text="❌ No LiDAR data available")]
        
        elif name == "take_photo":
            result = await r.take_photo()
            
            if result.get("status") == "completed" and result.get("data", {}).get("image"):
                image_b64 = result["data"]["image"]
                return [
                    ImageContent(
                        type="image",
                        data=image_b64,
                        mimeType="image/jpeg"
                    ),
                    TextContent(
                        type="text",
                        text=f"📸 Photo captured ({result['data'].get('size', 0)} bytes)"
                    )
                ]
            else:
                return [TextContent(type="text", text=format_result(result, "Take photo"))]
        
        elif name == "play_emote":
            emote = arguments.get("emote", "wave")
            result = await r.play_emote(emote)
            return [TextContent(type="text", text=format_result(result, f"Play emote '{emote}'"))]
        
        elif name == "set_pose":
            pose = arguments.get("pose", "stand")
            result = await r.set_pose(pose)
            return [TextContent(type="text", text=format_result(result, f"Set pose '{pose}'"))]
        
        elif name == "abort":
            result = await r.abort_command("")
            return [TextContent(type="text", text="🛑 Motion stopped")]
        
        elif name == "set_obstacle_avoidance":
            enabled = arguments.get("enabled", True)
            result = await r._send_command("setObstacleAvoidance", {"enabled": enabled})
            status = "ENABLED 🛡️" if enabled else "DISABLED ⚠️"
            text = f"Obstacle avoidance: {status}"
            if not enabled:
                text += "\n⚠️ Be careful! I'm navigating manually now."
            return [TextContent(type="text", text=text)]
        
        elif name == "get_state":
            state = await get_current_state()
            import math
            
            battery = state.get("battery", 0)
            avoidance = state.get("obstacleAvoidance", True)
            robot_connected = state.get("robotConnected", False)
            
            # Format state nicely
            formatted = {
                "connected": robot_connected,
                "position": {
                    "x": round(state.get("x", 0), 3),
                    "y": round(state.get("y", 0), 3),
                    "z": round(state.get("z", 0), 3)
                },
                "orientation_deg": {
                    "roll": round(math.degrees(state.get("roll", 0)), 1),
                    "pitch": round(math.degrees(state.get("pitch", 0)), 1),
                    "yaw": round(math.degrees(state.get("yaw", 0)), 1)
                },
                "velocity": {
                    "vx": round(state.get("vx", 0), 3),
                    "vy": round(state.get("vy", 0), 3),
                    "yaw_speed": round(state.get("yaw_speed", 0), 3)
                },
                "battery": f"{battery}%",
                "obstacle_avoidance": "ON" if avoidance else "OFF",
                "mode": state.get("mode", 0),
                "gait_type": state.get("gait_type", 0)
            }
            
            # Connection status icon
            if robot_connected:
                conn_icon = "🟢"
            else:
                conn_icon = "🔴 DISCONNECTED"
            
            # Add battery emoji
            if battery > 50:
                batt_icon = "🔋"
            elif battery > 20:
                batt_icon = "🪫"
            else:
                batt_icon = "⚠️🪫"
            
            return [TextContent(
                type="text",
                text=f"{conn_icon} {batt_icon} Robot State:\n{json.dumps(formatted, indent=2)}"
            )]
        
        else:
            return [TextContent(type="text", text=f"❌ Unknown tool: {name}")]
            
    except ConnectionError as e:
        return [TextContent(type="text", text=f"❌ Connection error: {e}\nIs the robot server running?")]
    except TimeoutError as e:
        return [TextContent(type="text", text=f"⏱️ Command timed out: {e}")]
    except Exception as e:
        return [TextContent(type="text", text=f"❌ Error: {str(e)}")]


async def main():
    """Run the MCP server"""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main())
