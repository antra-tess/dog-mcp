#!/usr/bin/env python3
"""
Go2 MCP Server

Exposes Unitree Go2 robot control as MCP tools.
Connects through the WebSocket server for shared robot access.
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
from mcp.types import Tool, TextContent, ImageContent

# Our WebSocket client
sys.path.insert(0, '/Users/olena/connectome-local/dog_mcp')
from robot_dog_client import RobotDogClient


# Global robot client
robot: RobotDogClient = None
server = Server("go2-robot")


async def ensure_connected():
    """Ensure we're connected to the robot server"""
    global robot
    if robot is None or not robot.connected:
        robot = RobotDogClient('localhost', 8765)
        await robot.connect()
    return robot


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available robot control tools"""
    return [
        Tool(
            name="take_photo",
            description="Take a photo from the robot dog's front camera. Returns the image.",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="move",
            description="Move the robot forward or backward. Positive distance = forward, negative = backward.",
            inputSchema={
                "type": "object",
                "properties": {
                    "distance": {
                        "type": "number",
                        "description": "Distance in meters (positive=forward, negative=backward)"
                    },
                    "speed": {
                        "type": "number",
                        "description": "Speed in m/s (default 0.5)",
                        "default": 0.5
                    }
                },
                "required": ["distance"]
            }
        ),
        Tool(
            name="turn",
            description="Turn the robot left or right. Positive angle = left, negative = right.",
            inputSchema={
                "type": "object",
                "properties": {
                    "angle": {
                        "type": "number",
                        "description": "Angle in degrees (positive=left, negative=right)"
                    },
                    "speed": {
                        "type": "number",
                        "description": "Turn speed in degrees/second (default 45)",
                        "default": 45
                    }
                },
                "required": ["angle"]
            }
        ),
        Tool(
            name="play_emote",
            description="Play an animation/emote. Available: wave, hello, nod, shake, dance, dance1, dance2, stretch, wiggle, fingerheart, moonwalk, handstand, frontflip, backflip",
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
            description="Set the robot's pose: sit, stand, or lie down",
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
            name="set_headlamp",
            description="Control the robot's LED headlamp color and pattern",
            inputSchema={
                "type": "object",
                "properties": {
                    "color": {
                        "type": "string",
                        "description": "Color as hex (e.g. #FF0000 for red)",
                        "default": "#FFFFFF"
                    },
                    "brightness": {
                        "type": "number",
                        "description": "Brightness 0.0-1.0",
                        "default": 1.0
                    },
                    "pattern": {
                        "type": "string",
                        "description": "Light pattern",
                        "enum": ["solid", "pulse", "blink"],
                        "default": "solid"
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="set_body_tilt",
            description="Tilt the robot's body (pitch forward/back, roll left/right)",
            inputSchema={
                "type": "object",
                "properties": {
                    "pitch": {
                        "type": "number",
                        "description": "Pitch angle in degrees (positive=nose down)",
                        "default": 0
                    },
                    "roll": {
                        "type": "number",
                        "description": "Roll angle in degrees (positive=lean right)",
                        "default": 0
                    }
                },
                "required": []
            }
        ),
        Tool(
            name="get_state",
            description="Get current robot state (motion channel, pose, battery, etc.)",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="abort",
            description="Stop any currently executing motion command",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent | ImageContent]:
    """Execute a robot control tool"""
    try:
        robot = await ensure_connected()
        
        if name == "take_photo":
            # Take photo and return as image
            result = await robot.take_photo()
            
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
                return [TextContent(type="text", text=f"❌ Photo capture failed: {result}")]
        
        elif name == "move":
            distance = arguments.get("distance", 0)
            speed = arguments.get("speed", 0.5)
            result = await robot.move(distance, speed)
            return [TextContent(
                type="text",
                text=f"🚶 Moved {'forward' if distance > 0 else 'backward'} {abs(distance)}m at {speed}m/s\nResult: {result.get('status')}"
            )]
        
        elif name == "turn":
            angle = arguments.get("angle", 0)
            speed = arguments.get("speed", 45)
            result = await robot.turn(angle, speed)
            return [TextContent(
                type="text",
                text=f"🔄 Turned {'left' if angle > 0 else 'right'} {abs(angle)}°\nResult: {result.get('status')}"
            )]
        
        elif name == "play_emote":
            emote = arguments.get("emote", "wave")
            result = await robot.play_emote(emote)
            return [TextContent(
                type="text",
                text=f"🎭 Playing emote: {emote}\nResult: {result.get('status')}"
            )]
        
        elif name == "set_pose":
            pose = arguments.get("pose", "stand")
            result = await robot.set_pose(pose)
            return [TextContent(
                type="text",
                text=f"🐕 Set pose: {pose}\nResult: {result.get('status')}"
            )]
        
        elif name == "set_headlamp":
            color = arguments.get("color", "#FFFFFF")
            brightness = arguments.get("brightness", 1.0)
            pattern = arguments.get("pattern", "solid")
            result = await robot.set_headlamp(color, brightness, pattern)
            return [TextContent(
                type="text",
                text=f"💡 Headlamp: {color} at {brightness*100}% ({pattern})\nResult: {result.get('status')}"
            )]
        
        elif name == "set_body_tilt":
            pitch = arguments.get("pitch", 0)
            roll = arguments.get("roll", 0)
            result = await robot.set_body_tilt(pitch, roll)
            return [TextContent(
                type="text",
                text=f"↗️ Body tilt: pitch={pitch}°, roll={roll}°\nResult: {result.get('status')}"
            )]
        
        elif name == "get_state":
            result = await robot.get_state()
            state = result.get("data", {})
            return [TextContent(
                type="text",
                text=f"📊 Robot State:\n{json.dumps(state, indent=2)}"
            )]
        
        elif name == "abort":
            result = await robot.abort_command("")
            return [TextContent(
                type="text",
                text=f"🛑 Motion stopped\nResult: {result.get('status')}"
            )]
        
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
            
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
