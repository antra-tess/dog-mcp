import asyncio
import json
import uuid
from typing import Dict, List, Optional, Callable, Any
import websockets
from datetime import datetime
import logging

class RobotDogClient:
    """
    Python client for the Robot Dog WebSocket API
    """
    
    def __init__(self, host: str = "localhost", port: int = 8765):
        self.host = host
        self.port = port
        self.websocket = None
        self.url = f"ws://{host}:{port}/api/v1/control"
        
        # Command tracking
        self.pending_commands: Dict[str, asyncio.Future] = {}
        self.subscriptions: Dict[str, Callable] = {}
        self.last_command_id: Optional[str] = None
        
        # Event handlers
        self.on_command_update: Optional[Callable] = None
        self.on_error: Optional[Callable] = None
        
        # State
        self.connected = False
        self.current_state = None
        
        # Logger
        self.logger = logging.getLogger(__name__)
    
    async def connect(self):
        """Connect to the robot dog WebSocket API"""
        try:
            self.websocket = await websockets.connect(self.url)
            self.connected = True
            self.logger.info(f"Connected to robot dog at {self.url}")
            
            # Start listening for messages
            asyncio.create_task(self._listen_for_messages())
            
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from the robot dog"""
        if self.websocket:
            await self.websocket.close()
        self.connected = False
        self.logger.info("Disconnected from robot dog")
    
    async def _listen_for_messages(self):
        """Listen for incoming messages from the robot"""
        try:
            async for message in self.websocket:
                data = json.loads(message)
                await self._handle_message(data)
        except websockets.exceptions.ConnectionClosed:
            self.connected = False
            self.logger.info("WebSocket connection closed")
        except Exception as e:
            self.logger.error(f"Error listening for messages: {e}")
    
    async def _handle_message(self, data: Dict):
        """Handle incoming message from robot"""
        msg_type = data.get("type")
        
        if msg_type == "response":
            # Handle command response
            command_id = data.get("commandId")
            status = data.get("status")
            
            if command_id in self.pending_commands:
                future = self.pending_commands[command_id]
                # Only resolve on final status (not queued)
                if not future.done() and status != "queued":
                    future.set_result(data)
        
        elif msg_type == "update":
            # Handle command update or subscription data
            if "subscription" in data:
                # Subscription data
                subscription = data["subscription"]
                if subscription in self.subscriptions:
                    callback = self.subscriptions[subscription]
                    if callback:
                        await callback(data["data"])
            elif "commandId" in data:
                # Command progress update
                if self.on_command_update:
                    await self.on_command_update(data)
        
        elif msg_type == "error":
            # Handle error
            if self.on_error:
                await self.on_error(data)
            else:
                self.logger.error(f"Robot error: {data}")
    
    async def _send_command(self, command: str, params: Dict = None, schedule: Dict = None) -> Dict:
        """Send a command and wait for response"""
        if not self.connected:
            raise ConnectionError("Not connected to robot")
        
        command_id = str(uuid.uuid4())
        message = {
            "type": "command",
            "command": command,
            "id": command_id,
            "params": params or {}
        }
        
        if schedule:
            message["schedule"] = schedule
        
        # Track last command ID for convenience methods
        self.last_command_id = command_id
        
        # Create future for response
        future = asyncio.Future()
        self.pending_commands[command_id] = future
        
        # Send message
        await self.websocket.send(json.dumps(message))
        
        try:
            # Wait for response (30 second timeout for slower commands)
            response = await asyncio.wait_for(future, timeout=30.0)
            return response
        except asyncio.TimeoutError:
            raise TimeoutError(f"Command {command} timed out")
        finally:
            # Clean up
            if command_id in self.pending_commands:
                del self.pending_commands[command_id]
    
    # Configuration commands
    async def set_gait(self, gait: str) -> Dict:
        """Set robot gait (walk, trot, pace, bound)"""
        return await self._send_command("setGait", {"gait": gait})
    
    async def set_object_avoidance(self, enabled: bool) -> Dict:
        """Enable or disable object avoidance"""
        return await self._send_command("setObjectAvoidance", {"enabled": enabled})
    
    async def set_headlamp(self, color: str, brightness: float = 1.0, pattern: str = "solid") -> Dict:
        """Set LED headlamp color and pattern"""
        return await self._send_command("setHeadlamp", {
            "color": color,
            "brightness": brightness,
            "pattern": pattern
        })
    
    # Motion commands
    async def move(self, distance: float, speed: float = None, 
                   schedule_after: str = None, schedule_delay: float = None, 
                   schedule_offset: float = None) -> Dict:
        """Move forward (positive) or backward (negative) distance in meters"""
        params = {"distance": distance}
        if speed is not None:
            params["speed"] = speed
        
        schedule = self._build_schedule(schedule_after, schedule_delay, schedule_offset)
        return await self._send_command("move", params, schedule)
    
    async def turn(self, angle: float, speed: float = None,
                   schedule_after: str = None, schedule_delay: float = None,
                   schedule_offset: float = None) -> Dict:
        """Turn left (positive) or right (negative) angle in degrees"""
        params = {"angle": angle}
        if speed is not None:
            params["speed"] = speed
        
        schedule = self._build_schedule(schedule_after, schedule_delay, schedule_offset)
        return await self._send_command("turn", params, schedule)
    
    async def face_heading(self, heading: float, speed: float = None,
                          schedule_after: str = None, schedule_delay: float = None,
                          schedule_offset: float = None) -> Dict:
        """Face absolute heading in degrees (0 = north)"""
        params = {"heading": heading}
        if speed is not None:
            params["speed"] = speed
        
        schedule = self._build_schedule(schedule_after, schedule_delay, schedule_offset)
        return await self._send_command("faceHeading", params, schedule)
    
    async def hold(self, duration: float = None,
                   schedule_after: str = None, schedule_delay: float = None,
                   schedule_offset: float = None) -> Dict:
        """Hold position (None = indefinite)"""
        schedule = self._build_schedule(schedule_after, schedule_delay, schedule_offset)
        return await self._send_command("hold", {"duration": duration}, schedule)
    
    # Body modification commands
    async def set_body_tilt(self, pitch: float, roll: float, duration: float = 1.0) -> Dict:
        """Set body tilt in degrees"""
        return await self._send_command("setBodyTilt", {
            "pitch": pitch,
            "roll": roll,
            "duration": duration
        })
    
    async def reset_body_tilt(self, duration: float = 1.0) -> Dict:
        """Reset body tilt to default"""
        return await self._send_command("resetBodyTilt", {"duration": duration})
    
    async def set_body_pose(self, pitch: float, roll: float, yaw: float = None, 
                           height: float = None, duration: float = 1.0) -> Dict:
        """Set advanced body pose (yaw and height only available during hold)"""
        params = {"pitch": pitch, "roll": roll, "duration": duration}
        if yaw is not None:
            params["yaw"] = yaw
        if height is not None:
            params["height"] = height
        return await self._send_command("setBodyPose", params)
    
    # Emote commands
    async def play_emote(self, emote: str,
                        schedule_after: str = None, schedule_delay: float = None,
                        schedule_offset: float = None) -> Dict:
        """Play animation emote (wave, nod, shake, dance, stretch)"""
        schedule = self._build_schedule(schedule_after, schedule_delay, schedule_offset)
        return await self._send_command("playEmote", {"emote": emote}, schedule)
    
    async def set_pose(self, pose: str,
                      schedule_after: str = None, schedule_delay: float = None,
                      schedule_offset: float = None) -> Dict:
        """Set persistent pose (sit, lie, stand)"""
        schedule = self._build_schedule(schedule_after, schedule_delay, schedule_offset)
        return await self._send_command("setPose", {"pose": pose}, schedule)
    
    async def exit_pose(self,
                       schedule_after: str = None, schedule_delay: float = None,
                       schedule_offset: float = None) -> Dict:
        """Exit current pose"""
        schedule = self._build_schedule(schedule_after, schedule_delay, schedule_offset)
        return await self._send_command("exitPose", {}, schedule)
    
    # Command control
    async def abort_command(self, command_id: str) -> Dict:
        """Abort a running command"""
        return await self._send_command("abort", {"targetCommandId": command_id})
    
    # Subscriptions
    async def subscribe_location(self, frequency: int = 10, callback: Callable = None) -> Dict:
        """Subscribe to location updates"""
        if callback:
            self.subscriptions["location"] = callback
        return await self._send_command("subscribeLocation", {"frequency": frequency})
    
    async def subscribe_lidar_distance(self, frequency: int = 20, 
                                     zones: List[str] = None, callback: Callable = None) -> Dict:
        """Subscribe to LIDAR distance updates"""
        if callback:
            self.subscriptions["lidarDistance"] = callback
        zones = zones or ["front", "left", "right", "back"]
        return await self._send_command("subscribeLidarDistance", {
            "frequency": frequency,
            "zones": zones
        })
    
    async def subscribe_video_snapshots(self, frequency: int = 1, resolution: str = "640x480",
                                      camera: str = "front", callback: Callable = None) -> Dict:
        """Subscribe to video snapshots"""
        if callback:
            self.subscriptions["video"] = callback
        return await self._send_command("subscribeVideoSnapshots", {
            "frequency": frequency,
            "resolution": resolution,
            "format": "jpeg",
            "quality": 80,
            "camera": camera
        })
    
    async def subscribe_battery(self, frequency: float = 1.0, callback: Callable = None) -> Dict:
        """Subscribe to battery status updates"""
        if callback:
            self.subscriptions["battery"] = callback
        return await self._send_command("subscribeBattery", {"frequency": frequency})
    
    async def unsubscribe(self, subscription: str) -> Dict:
        """Unsubscribe from data stream"""
        if subscription in self.subscriptions:
            del self.subscriptions[subscription]
        return await self._send_command("unsubscribe", {"subscription": subscription})
    
    # State query
    async def get_state(self) -> Dict:
        """Get current robot state"""
        response = await self._send_command("getState")
        if response.get("type") == "response":
            self.current_state = response.get("data")
        return response
    
    # Camera
    async def take_photo(self, save_path: str = None) -> Dict:
        """
        Take a photo from the robot's camera
        
        Args:
            save_path: Optional path to save the image. If provided, saves JPEG.
        
        Returns:
            Response dict with image data (base64) or saved file path
        """
        response = await self._send_command("takePhoto")
        
        # Save to file if path provided
        if save_path and response.get("data", {}).get("image"):
            import base64
            image_data = base64.b64decode(response["data"]["image"])
            with open(save_path, 'wb') as f:
                f.write(image_data)
            response["data"]["saved_path"] = save_path
        
        return response
    
    # Helper methods
    def is_motion_busy(self) -> bool:
        """Check if motion channel is busy"""
        if self.current_state:
            return self.current_state.get("motion", {}).get("channel") == "executing"
        return False
    
    def is_body_modification_busy(self) -> bool:
        """Check if body modification channel is busy"""
        if self.current_state:
            return self.current_state.get("bodyModification", {}).get("channel") == "executing"
        return False
    
    def get_battery_level(self) -> float:
        """Get current battery level (0.0-1.0)"""
        if self.current_state:
            return self.current_state.get("battery", 0.0)
        return 0.0
    
    def _build_schedule(self, schedule_after: str = None, schedule_delay: float = None, 
                       schedule_offset: float = None) -> Dict:
        """Build schedule dict for command"""
        if schedule_after:
            schedule = {
                "type": "after",
                "reference": schedule_after
            }
            if schedule_offset is not None:
                schedule["offset"] = schedule_offset
            return schedule
        elif schedule_delay:
            return {
                "type": "delay",
                "delay": schedule_delay
            }
        return None
    
    # Convenience methods for command queuing
    async def queue_after_last(self, command: str, params: Dict = None, offset: float = None) -> Dict:
        """Queue a command to execute after the last submitted command"""
        if not self.last_command_id:
            raise ValueError("No previous command to queue after")
        
        schedule = {
            "type": "after",
            "reference": self.last_command_id
        }
        if offset is not None:
            schedule["offset"] = offset
        
        return await self._send_command(command, params, schedule)
    
    async def queue_move_after_last(self, distance: float, speed: float = None, 
                                   offset: float = None) -> Dict:
        """Convenience method to queue move after last command"""
        return await self.move(distance, speed, schedule_after=self.last_command_id, 
                              schedule_offset=offset)
    
    async def queue_turn_after_last(self, angle: float, speed: float = None, 
                                   offset: float = None) -> Dict:
        """Convenience method to queue turn after last command"""
        return await self.turn(angle, speed, schedule_after=self.last_command_id, 
                              schedule_offset=offset) 