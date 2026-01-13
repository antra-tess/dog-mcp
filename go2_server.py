#!/usr/bin/env python3
"""
Go2 WebSocket Server

Bridges the high-level robot_dog_client API to the low-level unitree_webrtc_connect driver.
Supports multiple WebSocket clients with a single persistent robot connection.
"""

# Apply monkeypatch FIRST
import unitree_webrtc_connect

import asyncio
import json
import logging
import struct
import base64
from typing import Dict, Optional, Callable, Any, Set
from datetime import datetime
import websockets
from websockets.server import WebSocketServerProtocol

from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
from unitree_webrtc_connect.constants import RTC_TOPIC, SPORT_CMD, VUI_COLOR

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Go2Server:
    """WebSocket server that controls the Go2 robot"""
    
    def __init__(self, serial_number: str, host: str = "0.0.0.0", port: int = 8080):
        self.serial_number = serial_number
        self.host = host
        self.port = port
        
        # Robot connection
        self.robot: Optional[UnitreeWebRTCConnection] = None
        self.robot_connected = False
        
        # WebSocket clients
        self.clients: Set[WebSocketServerProtocol] = set()
        
        # State tracking
        self.current_state = {
            "motion": {"channel": "idle", "currentCommand": None},
            "bodyModification": {"channel": "idle", "currentTilt": {"pitch": 0, "roll": 0}},
            "configuration": {
                "gait": "trot",
                "objectAvoidance": False,
                "headlamp": {"color": "#FFFFFF", "brightness": 0.5, "pattern": "solid"}
            },
            "pose": "stand",
            "battery": 0.0,
            "activeSubscriptions": []
        }
        
        # Active subscriptions per client
        self.subscriptions: Dict[WebSocketServerProtocol, Set[str]] = {}
        
        # Command handlers
        self.command_handlers = {
            # Configuration
            "setGait": self.handle_set_gait,
            "setObjectAvoidance": self.handle_set_object_avoidance,
            "setHeadlamp": self.handle_set_headlamp,
            
            # Motion
            "move": self.handle_move,
            "turn": self.handle_turn,
            "faceHeading": self.handle_face_heading,
            "hold": self.handle_hold,
            
            # Body modification
            "setBodyTilt": self.handle_set_body_tilt,
            "resetBodyTilt": self.handle_reset_body_tilt,
            "setBodyPose": self.handle_set_body_pose,
            
            # Emotes
            "playEmote": self.handle_play_emote,
            "setPose": self.handle_set_pose,
            "exitPose": self.handle_exit_pose,
            
            # Command control
            "abort": self.handle_abort,
            
            # Subscriptions
            "subscribeLocation": self.handle_subscribe_location,
            "subscribeLidarDistance": self.handle_subscribe_lidar_distance,
            "subscribeVideoSnapshots": self.handle_subscribe_video,
            "subscribeBattery": self.handle_subscribe_battery,
            "unsubscribe": self.handle_unsubscribe,
            
            # State
            "getState": self.handle_get_state,
            
            # Camera
            "takePhoto": self.handle_take_photo,
        }
        
        # Photo capture state
        self._photo_chunks: Dict[int, bytes] = {}
        self._photo_total_chunks = 0
        self._photo_event: Optional[asyncio.Event] = None
        
        # Gait mapping
        self.gait_map = {
            "walk": 0,
            "trot": 1,
            "pace": 2,
            "bound": 3
        }
        
        # Emote mapping to SPORT_CMD
        self.emote_map = {
            "wave": "Hello",
            "hello": "Hello",
            "nod": "Content",
            "shake": "WiggleHips",
            "dance": "Dance1",
            "dance1": "Dance1",
            "dance2": "Dance2",
            "stretch": "Stretch",
            "scrape": "Scrape",
            "wiggle": "WiggleHips",
            "fingerheart": "FingerHeart",
            "moonwalk": "MoonWalk",
            "handstand": "Handstand",
            "frontflip": "FrontFlip",
            "backflip": "BackFlip",
        }
        
        # Pose mapping
        self.pose_map = {
            "sit": "Sit",
            "stand": "StandUp",
            "lie": "StandDown",
            "down": "StandDown",
        }
    
    async def connect_to_robot(self, max_retries: int = 3):
        """Connect to the Go2 robot with retries"""
        for attempt in range(max_retries):
            logger.info(f"Connecting to Go2 (serial: {self.serial_number})... attempt {attempt + 1}/{max_retries}")
            
            try:
                self.robot = UnitreeWebRTCConnection(
                    WebRTCConnectionMethod.LocalSTA,
                    serialNumber=self.serial_number
                )
                await self.robot.connect()
                self.robot_connected = True
                logger.info("✅ Connected to Go2!")
                
                # Get initial motion mode
                await self._get_motion_mode()
                return
                
            except Exception as e:
                logger.error(f"❌ Connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    logger.info("Retrying in 5 seconds...")
                    await asyncio.sleep(5)
                else:
                    logger.error("All connection attempts failed!")
                    raise
    
    async def _get_motion_mode(self):
        """Get current motion mode from robot"""
        try:
            response = await self.robot.datachannel.pub_sub.publish_request_new(
                RTC_TOPIC["MOTION_SWITCHER"],
                {"api_id": 1001}
            )
            if response['data']['header']['status']['code'] == 0:
                data = json.loads(response['data']['data'])
                logger.info(f"Robot motion mode: {data['name']}")
        except Exception as e:
            logger.warning(f"Could not get motion mode: {e}")
    
    async def _send_sport_command(self, cmd_name: str, params: Dict = None):
        """Send a sport mode command to the robot"""
        if not self.robot_connected:
            raise ConnectionError("Not connected to robot")
        
        cmd_id = SPORT_CMD.get(cmd_name)
        if cmd_id is None:
            raise ValueError(f"Unknown command: {cmd_name}")
        
        request = {"api_id": cmd_id}
        if params:
            request["parameter"] = params
        
        logger.info(f"Sending sport command: {cmd_name} ({cmd_id}) with params: {params}")
        
        response = await self.robot.datachannel.pub_sub.publish_request_new(
            RTC_TOPIC["SPORT_MOD"],
            request
        )
        return response
    
    async def _ensure_normal_mode(self):
        """Ensure robot is in normal mode for movement commands"""
        try:
            response = await self.robot.datachannel.pub_sub.publish_request_new(
                RTC_TOPIC["MOTION_SWITCHER"],
                {"api_id": 1001}
            )
            if response['data']['header']['status']['code'] == 0:
                data = json.loads(response['data']['data'])
                if data['name'] != "normal":
                    logger.info(f"Switching from {data['name']} to normal mode...")
                    await self.robot.datachannel.pub_sub.publish_request_new(
                        RTC_TOPIC["MOTION_SWITCHER"],
                        {"api_id": 1002, "parameter": {"name": "normal"}}
                    )
                    await asyncio.sleep(3)  # Wait for mode switch
        except Exception as e:
            logger.warning(f"Could not check/switch mode: {e}")
    
    # ============ Command Handlers ============
    
    async def handle_set_gait(self, params: Dict, command_id: str) -> Dict:
        """Set robot gait"""
        gait = params.get("gait", "trot")
        gait_id = self.gait_map.get(gait, 1)
        
        await self._send_sport_command("SwitchGait", {"data": gait_id})
        self.current_state["configuration"]["gait"] = gait
        
        return {"status": "completed", "data": {"gait": gait}}
    
    async def handle_set_object_avoidance(self, params: Dict, command_id: str) -> Dict:
        """Toggle object avoidance"""
        enabled = params.get("enabled", True)
        
        await self.robot.datachannel.pub_sub.publish_request_new(
            RTC_TOPIC["OBSTACLES_AVOID"],
            {"api_id": 1001 if enabled else 1002}
        )
        self.current_state["configuration"]["objectAvoidance"] = enabled
        
        return {"status": "completed", "data": {"enabled": enabled}}
    
    async def handle_set_headlamp(self, params: Dict, command_id: str) -> Dict:
        """Set LED headlamp"""
        color = params.get("color", "#FFFFFF")
        brightness = params.get("brightness", 1.0)
        pattern = params.get("pattern", "solid")
        
        # Map color to VUI color
        color_map = {
            "#FFFFFF": "white", "#FF0000": "red", "#00FF00": "green",
            "#0000FF": "blue", "#FFFF00": "yellow", "#00FFFF": "cyan",
            "#FF00FF": "purple", "#8000FF": "purple"
        }
        vui_color = color_map.get(color.upper(), "white")
        
        # Blink mode: 0 = solid, 1 = slow blink, 2 = fast blink
        blink_mode = {"solid": 0, "pulse": 1, "blink": 2}.get(pattern, 0)
        
        await self.robot.datachannel.pub_sub.publish_request_new(
            RTC_TOPIC["VUI"],
            {
                "api_id": 1003,
                "parameter": {
                    "color": vui_color,
                    "blink": blink_mode,
                    "brightness": int(brightness * 100)
                }
            }
        )
        
        self.current_state["configuration"]["headlamp"] = {
            "color": color, "brightness": brightness, "pattern": pattern
        }
        
        return {"status": "completed"}
    
    async def handle_move(self, params: Dict, command_id: str) -> Dict:
        """Move forward/backward"""
        distance = params.get("distance", 0)
        speed = params.get("speed", 0.5)
        
        await self._ensure_normal_mode()
        
        # Calculate movement parameters
        # x = forward/backward velocity (-1 to 1)
        # y = left/right velocity (-1 to 1)
        # z = rotation velocity (-1 to 1)
        x_vel = speed if distance > 0 else -speed
        duration = abs(distance) / speed
        
        self.current_state["motion"]["channel"] = "executing"
        self.current_state["motion"]["currentCommand"] = {"id": command_id, "command": "move"}
        
        # Send move command
        await self._send_sport_command("Move", {"x": x_vel, "y": 0, "z": 0})
        
        # Wait for duration
        await asyncio.sleep(duration)
        
        # Stop
        await self._send_sport_command("StopMove")
        
        self.current_state["motion"]["channel"] = "idle"
        self.current_state["motion"]["currentCommand"] = None
        
        return {"status": "completed", "data": {"distance": distance}}
    
    async def handle_turn(self, params: Dict, command_id: str) -> Dict:
        """Turn left/right"""
        angle = params.get("angle", 0)
        speed = params.get("speed", 45)  # degrees per second
        
        await self._ensure_normal_mode()
        
        # z = rotation velocity (positive = left)
        z_vel = 0.5 if angle > 0 else -0.5
        duration = abs(angle) / speed
        
        self.current_state["motion"]["channel"] = "executing"
        self.current_state["motion"]["currentCommand"] = {"id": command_id, "command": "turn"}
        
        await self._send_sport_command("Move", {"x": 0, "y": 0, "z": z_vel})
        await asyncio.sleep(duration)
        await self._send_sport_command("StopMove")
        
        self.current_state["motion"]["channel"] = "idle"
        self.current_state["motion"]["currentCommand"] = None
        
        return {"status": "completed", "data": {"angle": angle}}
    
    async def handle_face_heading(self, params: Dict, command_id: str) -> Dict:
        """Face absolute heading (simplified - just turns)"""
        heading = params.get("heading", 0)
        # This would need IMU data to properly implement
        # For now, treat as relative turn
        return await self.handle_turn({"angle": heading}, command_id)
    
    async def handle_hold(self, params: Dict, command_id: str) -> Dict:
        """Hold position"""
        duration = params.get("duration")
        
        await self._ensure_normal_mode()
        await self._send_sport_command("BalanceStand")
        
        self.current_state["motion"]["channel"] = "executing"
        self.current_state["motion"]["currentCommand"] = {"id": command_id, "command": "hold"}
        
        if duration:
            await asyncio.sleep(duration)
            self.current_state["motion"]["channel"] = "idle"
            self.current_state["motion"]["currentCommand"] = None
        
        return {"status": "completed"}
    
    async def handle_set_body_tilt(self, params: Dict, command_id: str) -> Dict:
        """Set body tilt"""
        pitch = params.get("pitch", 0)
        roll = params.get("roll", 0)
        duration = params.get("duration", 1.0)
        
        # Euler command: pitch and roll in radians
        import math
        pitch_rad = math.radians(pitch)
        roll_rad = math.radians(roll)
        
        await self._send_sport_command("Euler", {
            "x": roll_rad,
            "y": pitch_rad,
            "z": 0
        })
        
        self.current_state["bodyModification"]["currentTilt"] = {"pitch": pitch, "roll": roll}
        
        return {"status": "completed"}
    
    async def handle_reset_body_tilt(self, params: Dict, command_id: str) -> Dict:
        """Reset body tilt"""
        await self._send_sport_command("Euler", {"x": 0, "y": 0, "z": 0})
        self.current_state["bodyModification"]["currentTilt"] = {"pitch": 0, "roll": 0}
        return {"status": "completed"}
    
    async def handle_set_body_pose(self, params: Dict, command_id: str) -> Dict:
        """Set advanced body pose"""
        pitch = params.get("pitch", 0)
        roll = params.get("roll", 0)
        yaw = params.get("yaw", 0)
        height = params.get("height", 0)
        
        import math
        
        # Set body height if specified
        if height != 0:
            await self._send_sport_command("BodyHeight", {"data": height})
        
        # Set euler angles
        await self._send_sport_command("Euler", {
            "x": math.radians(roll),
            "y": math.radians(pitch),
            "z": math.radians(yaw)
        })
        
        return {"status": "completed"}
    
    async def handle_play_emote(self, params: Dict, command_id: str) -> Dict:
        """Play animation emote"""
        emote = params.get("emote", "wave").lower()
        cmd_name = self.emote_map.get(emote)
        
        if not cmd_name:
            return {"status": "failed", "error": f"Unknown emote: {emote}"}
        
        await self._ensure_normal_mode()
        await self._send_sport_command(cmd_name)
        
        return {"status": "completed", "data": {"emote": emote}}
    
    async def handle_set_pose(self, params: Dict, command_id: str) -> Dict:
        """Set persistent pose"""
        pose = params.get("pose", "stand").lower()
        cmd_name = self.pose_map.get(pose)
        
        if not cmd_name:
            return {"status": "failed", "error": f"Unknown pose: {pose}"}
        
        await self._send_sport_command(cmd_name)
        self.current_state["pose"] = pose
        
        return {"status": "completed", "data": {"pose": pose}}
    
    async def handle_exit_pose(self, params: Dict, command_id: str) -> Dict:
        """Exit current pose"""
        await self._send_sport_command("StandUp")
        self.current_state["pose"] = "stand"
        return {"status": "completed"}
    
    async def handle_abort(self, params: Dict, command_id: str) -> Dict:
        """Abort a running command"""
        await self._send_sport_command("StopMove")
        self.current_state["motion"]["channel"] = "idle"
        self.current_state["motion"]["currentCommand"] = None
        return {"status": "completed"}
    
    async def handle_subscribe_location(self, params: Dict, command_id: str) -> Dict:
        """Subscribe to location updates (placeholder)"""
        # Would need SLAM integration
        return {"status": "completed", "data": {"subscription": "location"}}
    
    async def handle_subscribe_lidar_distance(self, params: Dict, command_id: str) -> Dict:
        """Subscribe to LIDAR distance"""
        # Enable lidar streaming
        await self.robot.datachannel.disableTrafficSaving(True)
        return {"status": "completed", "data": {"subscription": "lidarDistance"}}
    
    async def handle_subscribe_video(self, params: Dict, command_id: str) -> Dict:
        """Subscribe to video snapshots"""
        self.robot.datachannel.switchVideoChannel(True)
        return {"status": "completed", "data": {"subscription": "video"}}
    
    async def handle_subscribe_battery(self, params: Dict, command_id: str) -> Dict:
        """Subscribe to battery status (would need periodic polling)"""
        return {"status": "completed", "data": {"subscription": "battery"}}
    
    async def handle_unsubscribe(self, params: Dict, command_id: str) -> Dict:
        """Unsubscribe from data stream"""
        subscription = params.get("subscription")
        if subscription == "video":
            self.robot.datachannel.switchVideoChannel(False)
        return {"status": "completed"}
    
    async def handle_get_state(self, params: Dict, command_id: str) -> Dict:
        """Get current robot state"""
        return {"status": "completed", "data": self.current_state}
    
    async def handle_take_photo(self, params: Dict, command_id: str) -> Dict:
        """Capture a photo from the robot's camera"""
        if not self.robot_connected:
            return {"status": "failed", "error": "Not connected to robot"}
        
        # Reset photo capture state
        self._photo_chunks = {}
        self._photo_total_chunks = 0
        self._photo_event = asyncio.Event()
        
        # Set up handler to capture binary photo response
        @self.robot.datachannel.channel.on("message")
        async def capture_photo_response(message):
            if isinstance(message, bytes) and len(message) > 100:
                try:
                    header_length = struct.unpack_from('<H', message, 0)[0]
                    json_data = message[4:4 + header_length]
                    binary_data = message[4 + header_length:]
                    
                    parsed = json.loads(json_data.decode('utf-8'))
                    
                    if parsed.get('type') == 'res' and 'videohub' in parsed.get('topic', ''):
                        content_info = parsed.get('data', {}).get('content_info', {})
                        
                        if content_info.get('enable_chunking'):
                            chunk_index = content_info.get('chunk_index', 0)
                            self._photo_total_chunks = content_info.get('total_chunk_num', 1)
                            self._photo_chunks[chunk_index] = binary_data
                            
                            if len(self._photo_chunks) >= self._photo_total_chunks:
                                self._photo_event.set()
                        else:
                            self._photo_chunks[1] = binary_data
                            self._photo_total_chunks = 1
                            self._photo_event.set()
                except Exception as e:
                    logger.warning(f"Error parsing photo response: {e}")
        
        # Request photo
        logger.info("📸 Requesting photo from robot...")
        
        try:
            # Send request (will timeout but we capture in handler)
            await asyncio.wait_for(
                self.robot.datachannel.pub_sub.publish_request_new(
                    RTC_TOPIC["FRONT_PHOTO_REQ"],
                    {"api_id": 1001}
                ),
                timeout=2
            )
        except asyncio.TimeoutError:
            pass
        
        # Wait for chunks
        try:
            await asyncio.wait_for(self._photo_event.wait(), timeout=10)
            
            # Combine chunks
            full_image = b''
            for i in range(1, self._photo_total_chunks + 1):
                if i in self._photo_chunks:
                    full_image += self._photo_chunks[i]
            
            if full_image:
                # Return as base64
                image_b64 = base64.b64encode(full_image).decode('utf-8')
                logger.info(f"✅ Photo captured ({len(full_image)} bytes)")
                
                return {
                    "status": "completed",
                    "data": {
                        "image": image_b64,
                        "format": "jpeg",
                        "size": len(full_image)
                    }
                }
            
        except asyncio.TimeoutError:
            logger.warning("Photo capture timed out")
            return {"status": "failed", "error": "Photo capture timed out"}
        
        return {"status": "failed", "error": "No image data received"}
    
    # ============ WebSocket Server ============
    
    async def handle_client(self, websocket: WebSocketServerProtocol):
        """Handle a connected WebSocket client"""
        self.clients.add(websocket)
        self.subscriptions[websocket] = set()
        
        client_addr = websocket.remote_address
        logger.info(f"Client connected: {client_addr}")
        
        # Send initial state
        await websocket.send(json.dumps({
            "type": "update",
            "event": "connected",
            "data": self.current_state,
            "timestamp": datetime.now().isoformat()
        }))
        
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self.handle_command(websocket, data)
                except json.JSONDecodeError:
                    await self.send_error(websocket, "INVALID_JSON", "Invalid JSON message")
                except Exception as e:
                    logger.exception(f"Error handling message: {e}")
                    await self.send_error(websocket, "INTERNAL_ERROR", str(e))
        
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"Client disconnected: {client_addr}")
        finally:
            self.clients.discard(websocket)
            if websocket in self.subscriptions:
                del self.subscriptions[websocket]
    
    async def handle_command(self, websocket: WebSocketServerProtocol, data: Dict):
        """Handle incoming command from client"""
        msg_type = data.get("type")
        
        if msg_type != "command":
            await self.send_error(websocket, "INVALID_MESSAGE", "Expected type: command")
            return
        
        command = data.get("command")
        command_id = data.get("id", "unknown")
        params = data.get("params", {})
        
        handler = self.command_handlers.get(command)
        
        if not handler:
            await self.send_error(websocket, "INVALID_COMMAND", f"Unknown command: {command}", command_id)
            return
        
        # Send queued response
        await websocket.send(json.dumps({
            "type": "response",
            "commandId": command_id,
            "status": "queued",
            "timestamp": datetime.now().isoformat()
        }))
        
        try:
            # Execute command
            result = await handler(params, command_id)
            
            # Send completion response
            await websocket.send(json.dumps({
                "type": "response",
                "commandId": command_id,
                "status": result.get("status", "completed"),
                "data": result.get("data"),
                "timestamp": datetime.now().isoformat()
            }))
            
        except Exception as e:
            logger.exception(f"Command {command} failed: {e}")
            await self.send_error(websocket, "COMMAND_FAILED", str(e), command_id)
    
    async def send_error(self, websocket: WebSocketServerProtocol, code: str, message: str, command_id: str = None):
        """Send error message to client"""
        error_msg = {
            "type": "error",
            "error": {"code": code, "message": message},
            "timestamp": datetime.now().isoformat()
        }
        if command_id:
            error_msg["commandId"] = command_id
        
        await websocket.send(json.dumps(error_msg))
    
    async def broadcast(self, message: Dict):
        """Broadcast message to all connected clients"""
        if self.clients:
            msg = json.dumps(message)
            await asyncio.gather(*[client.send(msg) for client in self.clients])
    
    async def run(self):
        """Run the server"""
        # Connect to robot first
        await self.connect_to_robot()
        
        # Start WebSocket server
        logger.info(f"Starting WebSocket server on ws://{self.host}:{self.port}/api/v1/control")
        
        async with websockets.serve(
            self.handle_client,
            self.host,
            self.port,
            ping_interval=20,
            ping_timeout=20
        ):
            logger.info("🚀 Go2 Server running!")
            logger.info(f"   Connect with: ws://localhost:{self.port}")
            await asyncio.Future()  # Run forever


async def main():
    import sys
    
    # Get serial number from args or use default
    serial = sys.argv[1] if len(sys.argv) > 1 else "B42D1000P57B6K09"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8765
    
    server = Go2Server(serial_number=serial, port=port)
    await server.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Server stopped")
