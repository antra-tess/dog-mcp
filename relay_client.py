#!/usr/bin/env python3
"""
Go2 Relay Client

A WebSocket client that connects to a Go2 robot through the cloud relay.
Used by MCP servers and other remote clients that can't reach the robot directly.

Environment variables:
  - GO2_RELAY_URL: WebSocket URL of relay server (e.g., wss://your-app.railway.app)
  - GO2_RELAY_SECRET: Shared authentication token
  - GO2_ROBOT_ID: Robot to connect to (default: "go2-home")
"""

import asyncio
import json
import os
import logging
from typing import Dict, Optional, Callable, Any
from datetime import datetime

import websockets

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RelayRobotClient:
    """
    Client for Go2 robot via cloud relay.
    API-compatible with RobotDogClient for easy swapping.
    """
    
    def __init__(
        self,
        relay_url: str = None,
        relay_secret: str = None,
        robot_id: str = None
    ):
        # Config from args or environment
        self.relay_url = relay_url or os.environ.get("GO2_RELAY_URL", "ws://localhost:8080")
        self.relay_secret = relay_secret or os.environ.get("GO2_RELAY_SECRET", "")
        self.robot_id = robot_id or os.environ.get("GO2_ROBOT_ID", "go2-home")
        
        self.websocket = None
        self.connected = False
        self.robot_connected = False
        
        # Command tracking
        self.pending_commands: Dict[str, asyncio.Future] = {}
        self.current_state = {}
        
        self.logger = logging.getLogger(__name__)
    
    async def connect(self):
        """Connect to relay and authenticate as client"""
        try:
            self.logger.info(f"Connecting to relay: {self.relay_url}")
            self.websocket = await websockets.connect(self.relay_url)
            
            # Authenticate
            auth_msg = json.dumps({
                "type": "auth",
                "token": self.relay_secret,
                "role": "client",
                "robot_id": self.robot_id
            })
            await self.websocket.send(auth_msg)
            
            # Wait for connection status
            response = await asyncio.wait_for(self.websocket.recv(), timeout=5)
            status = json.loads(response)
            
            if status.get("type") == "connection":
                self.robot_connected = status.get("connected", False)
                self.connected = True
                self.logger.info(f"Connected to relay, robot online: {self.robot_connected}")
            
            # Start message listener
            asyncio.create_task(self._listen())
            
        except Exception as e:
            self.logger.error(f"Failed to connect to relay: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from relay"""
        if self.websocket:
            await self.websocket.close()
        self.connected = False
        self.robot_connected = False
    
    async def _listen(self):
        """Listen for messages from relay"""
        try:
            async for message in self.websocket:
                data = json.loads(message)
                await self._handle_message(data)
        except websockets.ConnectionClosed:
            self.connected = False
            self.robot_connected = False
            self.logger.info("Relay connection closed")
        except Exception as e:
            self.logger.error(f"Error in relay listener: {e}")
    
    async def _handle_message(self, data: Dict):
        """Handle incoming message"""
        msg_type = data.get("type")
        
        if msg_type == "connection":
            self.robot_connected = data.get("connected", False)
            self.logger.info(f"Robot connection status: {self.robot_connected}")
            
        elif msg_type == "error":
            self.logger.error(f"Relay error: {data.get('error')}")
            
        elif msg_type == "response":
            command_id = data.get("commandId")
            if command_id in self.pending_commands:
                future = self.pending_commands[command_id]
                status = data.get("status")
                if status in ("completed", "failed"):
                    if not future.done():
                        future.set_result(data)
                    del self.pending_commands[command_id]
                    
        elif msg_type in ("state", "connected"):
            self.current_state = data.get("data", data)
    
    async def _send_command(self, command: str, params: Dict = None, timeout: float = 30) -> Dict:
        """Send command and wait for response"""
        if not self.connected:
            raise ConnectionError("Not connected to relay")
        if not self.robot_connected:
            raise ConnectionError("Robot not connected")
            
        command_id = f"{command}_{datetime.now().timestamp()}"
        
        msg = json.dumps({
            "command": command,
            "params": params or {},
            "id": command_id
        })
        
        future = asyncio.get_event_loop().create_future()
        self.pending_commands[command_id] = future
        
        await self.websocket.send(msg)
        
        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            self.pending_commands.pop(command_id, None)
            raise TimeoutError(f"Command {command} timed out")
    
    # High-level API (same as RobotDogClient)
    
    async def move(self, distance: float, speed: float = 0.3) -> Dict:
        """Move forward/backward"""
        return await self._send_command("move", {"distance": distance, "speed": speed}, timeout=60)
    
    async def turn(self, angle: float, speed: float = 30) -> Dict:
        """Turn left/right"""
        return await self._send_command("turn", {"angle": angle, "speed": speed}, timeout=60)
    
    async def look(self, yaw: float = 0, pitch: float = 0, hold: bool = True) -> Dict:
        """Tilt body to look"""
        return await self._send_command("look", {"yaw": yaw, "pitch": pitch, "hold": hold})
    
    async def get_state(self) -> Dict:
        """Get robot state"""
        return await self._send_command("getState", {})
    
    async def take_photo(self) -> Dict:
        """Capture photo"""
        return await self._send_command("takePhoto", {}, timeout=10)
    
    async def play_emote(self, emote: str) -> Dict:
        """Play animation"""
        return await self._send_command("playEmote", {"emote": emote})
    
    async def set_pose(self, pose: str) -> Dict:
        """Set pose (sit/stand/lie)"""
        return await self._send_command("setPose", {"pose": pose})
    
    async def abort(self) -> Dict:
        """Emergency stop"""
        return await self._send_command("abort", {})
    
    async def set_obstacle_avoidance(self, enabled: bool) -> Dict:
        """Toggle obstacle avoidance"""
        return await self._send_command("setObstacleAvoidance", {"enabled": enabled})


# Test
async def test():
    client = RelayRobotClient()
    await client.connect()
    
    if client.robot_connected:
        state = await client.get_state()
        print(f"Robot state: {state}")
    else:
        print("Robot not connected to relay")
    
    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(test())
