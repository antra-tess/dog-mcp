#!/usr/bin/env python3
"""
Go2 WebSocket Server v2 - With Closed-Loop Control

Features:
- Sensor feedback for accurate movement
- Live state streaming to connected clients
- Position/orientation tracking via IMU
"""

# Apply monkeypatch FIRST
import unitree_webrtc_connect

import asyncio
import json
import logging
import struct
import base64
import math
from typing import Dict, Optional, Any, Set
from datetime import datetime
from dataclasses import dataclass, asdict
import websockets
from websockets.server import WebSocketServerProtocol

from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
from unitree_webrtc_connect.constants import RTC_TOPIC, SPORT_CMD

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class RobotState:
    """Live robot state from sensors"""
    # Position (meters, from odometry)
    x: float = 0.0
    y: float = 0.0
    z: float = 0.0
    
    # Orientation (radians, from IMU)
    roll: float = 0.0
    pitch: float = 0.0
    yaw: float = 0.0
    
    # Velocity (m/s)
    vx: float = 0.0
    vy: float = 0.0
    vz: float = 0.0
    yaw_speed: float = 0.0
    
    # Status
    mode: int = 0
    gait_type: int = 0
    body_height: float = 0.0
    foot_raise_height: float = 0.0
    
    # Timestamps
    last_update: float = 0.0


class Go2ServerV2:
    """WebSocket server with closed-loop control"""
    
    def __init__(self, serial_number: str, host: str = "0.0.0.0", port: int = 8765):
        self.serial_number = serial_number
        self.host = host
        self.port = port
        
        # Robot connection
        self.robot: Optional[UnitreeWebRTCConnection] = None
        self.robot_connected = False
        self._reconnect_task: Optional[asyncio.Task] = None
        self._reconnect_interval = 5  # seconds
        
        # Live robot state from sensors
        self.robot_state = RobotState()
        self._state_lock = asyncio.Lock()
        self._last_state_time = 0
        self._connection_timeout = 10  # seconds without state update = disconnected
        
        # WebSocket clients
        self.clients: Set[WebSocketServerProtocol] = set()
        
        # State streaming
        self._stream_task: Optional[asyncio.Task] = None
        self._streaming = False
        
        # Command execution
        self._current_command_task: Optional[asyncio.Task] = None
        self._abort_requested = False
        self._movement_task: Optional[asyncio.Task] = None  # Tracks move/turn specifically
        
        # Photo capture
        self._photo_chunks: Dict[int, bytes] = {}
        self._photo_total_chunks = 0
        self._photo_event: Optional[asyncio.Event] = None
        
        # LiDAR obstacle map
        self._obstacle_grid: str = ""
        self._lidar_enabled = False
        self._grid_size = 30  # 30x30 characters
        self._grid_range = 2.0  # 2m x 2m area
        
        # Command handlers
        self.command_handlers = {
            "move": self.handle_move,
            "turn": self.handle_turn,
            "look": self.handle_look,
            "playEmote": self.handle_play_emote,
            "setPose": self.handle_set_pose,
            "setHeadlamp": self.handle_set_headlamp,
            "setBodyTilt": self.handle_set_body_tilt,
            "takePhoto": self.handle_take_photo,
            "getState": self.handle_get_state,
            "abort": self.handle_abort,
            "startStateStream": self.handle_start_stream,
            "stopStateStream": self.handle_stop_stream,
        }
        
        # Look mode state
        self._look_mode_active = False
        
        # Emote mapping
        self.emote_map = {
            "wave": "Hello", "hello": "Hello", "nod": "Content",
            "shake": "WiggleHips", "dance": "Dance1", "dance1": "Dance1",
            "dance2": "Dance2", "stretch": "Stretch", "wiggle": "WiggleHips",
            "fingerheart": "FingerHeart", "moonwalk": "MoonWalk",
            "handstand": "Handstand", "frontflip": "FrontFlip", "backflip": "BackFlip",
        }
        
        # Pose mapping
        self.pose_map = {"sit": "Sit", "stand": "StandUp", "lie": "StandDown", "down": "StandDown"}
    
    async def connect_to_robot(self) -> bool:
        """Connect to the Go2 robot (single attempt)"""
        logger.info(f"Connecting to Go2 (serial: {self.serial_number})...")
        try:
            self.robot = UnitreeWebRTCConnection(
                WebRTCConnectionMethod.LocalSTA,
                serialNumber=self.serial_number
            )
            await self.robot.connect()
            self.robot_connected = True
            self._last_state_time = asyncio.get_event_loop().time()
            logger.info("✅ Connected to Go2!")
            
            # Subscribe to state updates
            await self._setup_state_subscription()
            
            # Notify clients
            await self._broadcast_connection_status(True)
            return True
            
        except SystemExit as e:
            # The driver sometimes calls sys.exit() on connection failure - catch it
            logger.warning(f"❌ Connection failed (driver exit): {e}")
            self.robot_connected = False
            self.robot = None
            return False
        except Exception as e:
            logger.warning(f"❌ Connection failed: {e}")
            self.robot_connected = False
            self.robot = None
            return False
    
    async def disconnect_robot(self):
        """Disconnect from robot"""
        if self.robot:
            try:
                await self.robot.disconnect()
            except:
                pass
            self.robot = None
        self.robot_connected = False
        await self._broadcast_connection_status(False)
    
    async def _broadcast_connection_status(self, connected: bool):
        """Notify all clients of connection status change"""
        msg = json.dumps({
            "type": "connection",
            "connected": connected,
            "timestamp": datetime.now().isoformat()
        })
        for client in list(self.clients):
            try:
                await client.send(msg)
            except:
                pass
    
    def _is_connection_healthy(self) -> bool:
        """Check if robot connection is still healthy"""
        if not self.robot_connected or not self.robot:
            return False
        
        # Check if we've received state updates recently
        now = asyncio.get_event_loop().time()
        if now - self._last_state_time > self._connection_timeout:
            logger.warning("Connection unhealthy: no state updates")
            return False
        
        return True
    
    async def _reconnect_loop(self):
        """Background task to maintain robot connection"""
        logger.info(f"🔄 Auto-reconnect enabled (polling every {self._reconnect_interval}s)")
        
        while True:
            try:
                await asyncio.sleep(self._reconnect_interval)
                
                # Check if connection is healthy
                if self._is_connection_healthy():
                    continue
                
                # Connection lost or unhealthy
                if self.robot_connected:
                    logger.warning("🔌 Connection lost, will attempt reconnect...")
                    await self.disconnect_robot()
                
                # Try to reconnect
                logger.info("🔄 Attempting to reconnect...")
                success = await self.connect_to_robot()
                
                if success:
                    logger.info("🎉 Reconnected successfully!")
                else:
                    logger.info(f"Robot not found, will retry in {self._reconnect_interval}s...")
                    
            except asyncio.CancelledError:
                break
            except SystemExit as e:
                # Driver sometimes calls sys.exit() - don't let it kill the server
                logger.warning(f"Driver exit caught in reconnect loop: {e}")
                self.robot_connected = False
                self.robot = None
                await asyncio.sleep(self._reconnect_interval)
            except Exception as e:
                logger.error(f"Reconnect loop error: {e}")
                await asyncio.sleep(self._reconnect_interval)
    
    async def _setup_state_subscription(self):
        """Subscribe to robot state updates"""
        def state_callback(message):
            try:
                data = message.get('data', {})
                
                # Update position
                pos = data.get('position', [0, 0, 0])
                self.robot_state.x = pos[0] if len(pos) > 0 else 0
                self.robot_state.y = pos[1] if len(pos) > 1 else 0
                self.robot_state.z = pos[2] if len(pos) > 2 else 0
                
                # Update orientation from IMU
                imu = data.get('imu_state', {})
                rpy = imu.get('rpy', [0, 0, 0])
                self.robot_state.roll = rpy[0] if len(rpy) > 0 else 0
                self.robot_state.pitch = rpy[1] if len(rpy) > 1 else 0
                self.robot_state.yaw = rpy[2] if len(rpy) > 2 else 0
                
                # Update velocity
                vel = data.get('velocity', [0, 0, 0])
                self.robot_state.vx = vel[0] if len(vel) > 0 else 0
                self.robot_state.vy = vel[1] if len(vel) > 1 else 0
                self.robot_state.vz = vel[2] if len(vel) > 2 else 0
                self.robot_state.yaw_speed = data.get('yaw_speed', 0)
                
                # Update status
                self.robot_state.mode = data.get('mode', 0)
                self.robot_state.gait_type = data.get('gait_type', 0)
                self.robot_state.body_height = data.get('body_height', 0)
                self.robot_state.foot_raise_height = data.get('foot_raise_height', 0)
                
                now = asyncio.get_event_loop().time()
                self.robot_state.last_update = now
                self._last_state_time = now
                
            except Exception as e:
                logger.warning(f"Error parsing state: {e}")
        
        self.robot.datachannel.pub_sub.subscribe(RTC_TOPIC['LF_SPORT_MOD_STATE'], state_callback)
        logger.info("📡 Subscribed to robot state updates")
        
        # Subscribe to LiDAR data
        await self._setup_lidar_subscription()
    
    async def _get_current_state(self) -> RobotState:
        """Get current robot state (thread-safe)"""
        async with self._state_lock:
            return RobotState(**asdict(self.robot_state))
    
    async def _setup_lidar_subscription(self):
        """Subscribe to LiDAR point cloud data"""
        # Enable traffic saving bypass for LiDAR data
        await self.robot.datachannel.disableTrafficSaving(True)
        
        # Set decoder - libvoxel gives positions array, native gives points function
        self.robot.datachannel.set_decoder(decoder_type='libvoxel')
        
        # Turn on LiDAR
        self.robot.datachannel.pub_sub.publish_without_callback("rt/utlidar/switch", "on")
        self._lidar_enabled = True
        
        def lidar_callback(message):
            try:
                # Debug: log message structure once
                if not hasattr(self, '_lidar_debug_logged'):
                    self._lidar_debug_logged = True
                    logger.info(f"LiDAR message keys: {message.keys() if isinstance(message, dict) else type(message)}")
                    if isinstance(message, dict):
                        msg_data = message.get('data', {})
                        logger.info(f"LiDAR data keys: {msg_data.keys() if isinstance(msg_data, dict) else type(msg_data)}")
                        if isinstance(msg_data, dict) and 'data' in msg_data:
                            inner = msg_data.get('data', {})
                            logger.info(f"LiDAR inner data keys: {inner.keys() if isinstance(inner, dict) else type(inner)}")
                
                # Structure is message["data"] contains metadata and message["data"]["data"] has decoded positions
                msg_data = message.get('data', {})
                if not isinstance(msg_data, dict):
                    return
                
                # Get voxel grid metadata - might be in different places
                # Try msg_data first, then msg_data['data']
                origin = msg_data.get('origin')
                resolution = msg_data.get('resolution', 0.05)
                
                # If not found at top level, check inner data
                inner_data = msg_data.get('data', {})
                if origin is None and isinstance(inner_data, dict):
                    origin = inner_data.get('origin', [0, 0, 0])
                    resolution = inner_data.get('resolution', resolution)
                
                if origin is None:
                    origin = [0, 0, 0]  # Default fallback
                if not isinstance(inner_data, dict):
                    return
                
                positions = inner_data.get('positions', None)
                if positions is None or len(positions) == 0:
                    return
                
                # Convert voxel indices to world coordinates
                # positions are flat array of voxel indices [vx, vy, vz, vx, vy, vz, ...]
                # world_coord = origin + voxel_index * resolution
                points = []
                for i in range(0, len(positions), 3):
                    if i + 2 < len(positions):
                        vx, vy, vz = float(positions[i]), float(positions[i+1]), float(positions[i+2])
                        # Convert voxel indices to world coordinates
                        wx = origin[0] + vx * resolution
                        wy = origin[1] + vy * resolution
                        wz = origin[2] + vz * resolution
                        points.append((wx, wy, wz))
                
                if len(points) > 0:
                    self._obstacle_grid = self._points_to_ascii_grid(points)
                    
            except Exception as e:
                import traceback
                logger.warning(f"LiDAR parse error: {e}\n{traceback.format_exc()}")
        
        self.robot.datachannel.pub_sub.subscribe("rt/utlidar/voxel_map_compressed", lidar_callback)
        logger.info("📡 Subscribed to LiDAR data")
    
    def _points_to_ascii_grid(self, points) -> str:
        """Convert 3D point cloud to 2D ASCII obstacle map (bird's eye view)
        
        Grid is centered on robot, showing obstacles in a 2m x 2m area
        Enhanced with distance markers and clearer semantics
        """
        grid_size = self._grid_size
        grid_range = self._grid_range
        half_range = grid_range / 2
        cell_size = grid_range / grid_size
        
        # Get robot's current position and orientation to make points robot-relative
        robot_x = self.robot_state.x
        robot_y = self.robot_state.y
        robot_z = self.robot_state.z
        robot_yaw = self.robot_state.yaw
        
        # Precompute rotation
        cos_yaw = math.cos(-robot_yaw)
        sin_yaw = math.sin(-robot_yaw)
        
        # Initialize grid and density
        grid = [[' ' for _ in range(grid_size)] for _ in range(grid_size)]
        density = [[0 for _ in range(grid_size)] for _ in range(grid_size)]
        
        # Height filter
        min_height = robot_z - 0.2
        max_height = robot_z + 0.5
        
        for point in points:
            if len(point) < 3:
                continue
            dx = point[0] - robot_x
            dy = point[1] - robot_y
            z = point[2]
            
            if z < min_height or z > max_height:
                continue
            
            x = dx * cos_yaw - dy * sin_yaw
            y = -(dx * sin_yaw + dy * cos_yaw)
            
            gx = int((x + half_range) / cell_size)
            gy = int((y + half_range) / cell_size)
            
            if 0 <= gx < grid_size and 0 <= gy < grid_size:
                density[gx][gy] += 1
        
        center = grid_size // 2
        
        # Convert density to semantic characters
        # Using characters that convey meaning:
        #   · = clear/safe to traverse
        #   ○ = sparse detection (maybe noise or edge)
        #   ● = solid obstacle detection
        #   ▪ = dense obstacle  
        #   ■ = wall/solid barrier
        for i in range(grid_size):
            for j in range(grid_size):
                d = density[i][j]
                if d == 0:
                    grid[i][j] = '·'  # Clear
                elif d < 3:
                    grid[i][j] = '○'  # Sparse - might be passable
                elif d < 8:
                    grid[i][j] = '●'  # Obstacle detected
                elif d < 15:
                    grid[i][j] = '▪'  # Dense obstacle
                else:
                    grid[i][j] = '■'  # Solid wall
        
        # Draw distance ring at ~0.5m (about 7-8 cells from center)
        # This helps gauge "immediate vicinity" vs "further away"
        ring_radius = int(0.5 / cell_size)  # 0.5m ring
        for angle in range(0, 360, 10):
            rad = math.radians(angle)
            ri = center + int(ring_radius * math.cos(rad))
            rj = center + int(ring_radius * math.sin(rad))
            if 0 <= ri < grid_size and 0 <= rj < grid_size:
                if grid[ri][rj] == '·':
                    grid[ri][rj] = '+'  # 0.5m distance marker
        
        # Draw robot body (simplified but clear)
        robot_forward = 4
        robot_back = 4
        robot_left = 2
        robot_right = 2
        
        for dx in range(-robot_back, robot_forward + 1):
            for dy in range(-robot_left, robot_right + 1):
                gx = center + dx
                gy = center + dy
                if 0 <= gx < grid_size and 0 <= gy < grid_size:
                    if dx == robot_forward:
                        grid[gx][gy] = '▲' if dy == 0 else '='
                    elif dx == -robot_back:
                        grid[gx][gy] = '='
                    elif dy == -robot_left or dy == robot_right:
                        grid[gx][gy] = '|'
                    else:
                        grid[gx][gy] = ' '
        
        grid[center][center] = '@'  # Robot center - @ is intuitive "you are here"
        
        # Build output with scale and direction labels
        lines = []
        
        # Top label
        lines.append(f"         FORWARD (+1m)")
        lines.append(f"    ←LEFT    ·    RIGHT→")
        lines.append(f"    ┌{'─' * grid_size}┐")
        
        # Grid rows (reversed so forward is up)
        for i, row in enumerate(reversed(grid)):
            row_idx = grid_size - 1 - i
            dist_from_center = (row_idx - center) * cell_size
            
            # Add distance label at key positions
            if abs(dist_from_center - 0.5) < cell_size:
                label = "+.5m"
            elif abs(dist_from_center + 0.5) < cell_size:
                label = "-.5m"
            elif row_idx == center:
                label = "  0 "
            else:
                label = "    "
            
            lines.append(f"{label}│{''.join(row)}│")
        
        lines.append(f"    └{'─' * grid_size}┘")
        lines.append(f"          BACK (-1m)")
        lines.append(f"")
        lines.append(f"  · clear  ○ sparse  ● obstacle  ▪ dense  ■ wall")
        lines.append(f"  + 0.5m ring    @ robot center    ▲ front")
        
        return '\n'.join(lines)
    
    async def _send_sport_command(self, cmd_name: str, params: Dict = None):
        """Send a sport mode command"""
        if not self.robot_connected:
            raise ConnectionError("Not connected to robot")
        
        cmd_id = SPORT_CMD.get(cmd_name)
        if cmd_id is None:
            raise ValueError(f"Unknown command: {cmd_name}")
        
        request = {"api_id": cmd_id}
        if params:
            request["parameter"] = params
        
        logger.info(f"📤 Sending: {cmd_name} ({cmd_id}) params={params}")
        
        response = await self.robot.datachannel.pub_sub.publish_request_new(
            RTC_TOPIC["SPORT_MOD"], request
        )
        logger.info(f"📥 Response: {response}")
        return response
    
    # ============ Closed-Loop Movement ============
    
    async def _cancel_existing_movement(self):
        """Cancel any existing move/turn command before starting a new one"""
        if self._movement_task and not self._movement_task.done():
            logger.info("⚡ Cancelling existing movement for new command")
            self._abort_requested = True
            try:
                await asyncio.wait_for(self._send_sport_command("StopMove"), timeout=1.0)
            except Exception as e:
                logger.warning(f"StopMove during cancel failed: {e}")
            
            self._movement_task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(self._movement_task), timeout=2.0)
            except asyncio.CancelledError:
                logger.info("Previous movement task cancelled")
            except asyncio.TimeoutError:
                logger.warning("Previous movement task didn't stop in time")
            except Exception as e:
                logger.warning(f"Error waiting for cancelled task: {e}")
            
            self._abort_requested = False
            self._movement_task = None
            logger.info("✅ Ready for new movement")
    
    async def handle_move(self, params: Dict, command_id: str) -> Dict:
        """Move with closed-loop feedback - continuously sends velocity commands"""
        target_distance = params.get("distance", 0)
        speed = min(abs(params.get("speed", 0.3)), 0.5)  # Cap speed
        timeout = params.get("timeout", 30)
        stall_threshold = 0.02  # Must move at least 2cm per stall check period
        stall_check_period = 1.0  # Check for stall every second
        
        if abs(target_distance) < 0.01:
            return {"status": "completed", "data": {"distance": 0, "actual": 0}}
        
        # Get initial position
        initial = await self._get_current_state()
        initial_x, initial_y = initial.x, initial.y
        
        logger.info(f"🚶 Moving {target_distance}m from ({initial_x:.3f}, {initial_y:.3f})")
        
        direction = 1 if target_distance > 0 else -1
        start_time = asyncio.get_event_loop().time()
        actual_distance = 0
        last_log_time = 0
        last_cmd_time = 0
        
        # Stall detection
        last_stall_check_time = start_time
        last_stall_check_distance = 0
        stalled = False
        
        try:
            while True:
                current_time = asyncio.get_event_loop().time()
                elapsed = current_time - start_time
                
                # Get current position
                current = await self._get_current_state()
                actual_distance = math.sqrt(
                    (current.x - initial_x) ** 2 + 
                    (current.y - initial_y) ** 2
                )
                
                # Check if target reached
                if actual_distance >= abs(target_distance):
                    logger.info(f"✅ Target reached: {actual_distance:.3f}m")
                    break
                
                # Check for abort
                if self._abort_requested:
                    logger.info(f"🛑 Move aborted at {actual_distance:.3f}m")
                    return {
                        "status": "failed",
                        "error": "aborted",
                        "data": {"target": target_distance, "actual": actual_distance * direction, "reason": "Aborted by user"}
                    }
                
                # Check timeout
                if elapsed > timeout:
                    logger.warning(f"⏱️ Timeout after {elapsed:.1f}s, traveled {actual_distance:.3f}m")
                    break
                
                # Stall detection - check if we're making progress
                if current_time - last_stall_check_time >= stall_check_period:
                    distance_since_check = actual_distance - last_stall_check_distance
                    if distance_since_check < stall_threshold and elapsed > 1.0:
                        logger.warning(f"🛑 Stalled! Only moved {distance_since_check:.3f}m in {stall_check_period}s (obstacle?)")
                        stalled = True
                        break
                    last_stall_check_time = current_time
                    last_stall_check_distance = actual_distance
                
                # Send move command every 100ms to maintain velocity
                if current_time - last_cmd_time >= 0.1:
                    await self._send_sport_command("Move", {"x": speed * direction, "y": 0, "z": 0})
                    last_cmd_time = current_time
                
                # Log every second
                if elapsed - last_log_time >= 1.0:
                    logger.info(f"  📍 t={elapsed:.1f}s pos=({current.x:.3f}, {current.y:.3f}) dist={actual_distance:.3f}m")
                    last_log_time = elapsed
                
                # Broadcast progress
                await self._broadcast_progress(command_id, "move", {
                    "target": target_distance,
                    "actual": actual_distance * direction,
                    "elapsed": elapsed
                })
                
                await asyncio.sleep(0.05)  # 20Hz control loop
        
        finally:
            # Always stop
            await self._send_sport_command("StopMove")
        
        if stalled:
            logger.info(f"🚫 Move failed (stalled): target={target_distance:.2f}m actual={actual_distance:.2f}m")
            return {
                "status": "failed",
                "error": "stalled",
                "data": {
                    "target": target_distance,
                    "actual": actual_distance * direction,
                    "position": {"x": current.x, "y": current.y},
                    "reason": "Robot stalled - possible obstacle"
                }
            }
        
        logger.info(f"🏁 Move complete: target={target_distance:.2f}m actual={actual_distance:.2f}m")
        
        return {
            "status": "completed",
            "data": {
                "target": target_distance,
                "actual": actual_distance * direction,
                "position": {"x": current.x, "y": current.y}
            }
        }
    
    async def handle_turn(self, params: Dict, command_id: str) -> Dict:
        """Turn with closed-loop feedback - continuously sends rotation commands"""
        target_angle_deg = params.get("angle", 0)
        speed = min(abs(params.get("speed", 0.5)), 1.0)
        timeout = params.get("timeout", 30)
        stall_threshold_deg = 2.0  # Must rotate at least 2° per stall check period
        stall_check_period = 1.0  # Check for stall every second
        
        if abs(target_angle_deg) < 1:
            return {"status": "completed", "data": {"angle": 0, "actual": 0}}
        
        target_angle = math.radians(target_angle_deg)
        stall_threshold = math.radians(stall_threshold_deg)
        
        # Get initial orientation
        initial = await self._get_current_state()
        initial_yaw = initial.yaw
        
        logger.info(f"🔄 Turning {target_angle_deg}° from {math.degrees(initial_yaw):.1f}°")
        
        direction = 1 if target_angle > 0 else -1
        start_time = asyncio.get_event_loop().time()
        actual_angle = 0
        last_cmd_time = 0
        last_log_time = 0
        
        # Stall detection
        last_stall_check_time = start_time
        last_stall_check_angle = 0
        stalled = False
        
        try:
            while True:
                current_time = asyncio.get_event_loop().time()
                elapsed = current_time - start_time
                
                current = await self._get_current_state()
                
                # Calculate angle turned (handle wraparound)
                delta = current.yaw - initial_yaw
                while delta > math.pi:
                    delta -= 2 * math.pi
                while delta < -math.pi:
                    delta += 2 * math.pi
                actual_angle = delta
                
                # Check if target reached
                if abs(actual_angle) >= abs(target_angle):
                    logger.info(f"✅ Target reached: {math.degrees(actual_angle):.1f}°")
                    break
                
                # Check for abort
                if self._abort_requested:
                    logger.info(f"🛑 Turn aborted at {math.degrees(actual_angle):.1f}°")
                    return {
                        "status": "failed",
                        "error": "aborted",
                        "data": {"target": target_angle_deg, "actual": math.degrees(actual_angle), "reason": "Aborted by user"}
                    }
                
                # Check timeout
                if elapsed > timeout:
                    logger.warning(f"⏱️ Timeout after {elapsed:.1f}s, turned {math.degrees(actual_angle):.1f}°")
                    break
                
                # Stall detection - check if we're making progress
                if current_time - last_stall_check_time >= stall_check_period:
                    angle_since_check = abs(actual_angle) - abs(last_stall_check_angle)
                    if angle_since_check < stall_threshold and elapsed > 1.0:
                        logger.warning(f"🛑 Stalled! Only turned {math.degrees(angle_since_check):.1f}° in {stall_check_period}s (obstacle?)")
                        stalled = True
                        break
                    last_stall_check_time = current_time
                    last_stall_check_angle = actual_angle
                
                # Send turn command every 100ms to maintain rotation
                if current_time - last_cmd_time >= 0.1:
                    await self._send_sport_command("Move", {"x": 0, "y": 0, "z": speed * direction})
                    last_cmd_time = current_time
                
                # Log every second
                if elapsed - last_log_time >= 1.0:
                    logger.info(f"  🔄 t={elapsed:.1f}s yaw={math.degrees(current.yaw):.1f}° turned={math.degrees(actual_angle):.1f}°")
                    last_log_time = elapsed
                
                # Broadcast progress
                await self._broadcast_progress(command_id, "turn", {
                    "target": target_angle_deg,
                    "actual": math.degrees(actual_angle),
                    "elapsed": elapsed
                })
                
                await asyncio.sleep(0.05)  # 20Hz control loop
        
        finally:
            await self._send_sport_command("StopMove")
        
        if stalled:
            logger.info(f"🚫 Turn failed (stalled): target={target_angle_deg:.1f}° actual={math.degrees(actual_angle):.1f}°")
            return {
                "status": "failed",
                "error": "stalled",
                "data": {
                    "target": target_angle_deg,
                    "actual": math.degrees(actual_angle),
                    "yaw": math.degrees(current.yaw),
                    "reason": "Robot stalled - possible obstacle"
                }
            }
        
        logger.info(f"🏁 Turn complete: target={target_angle_deg:.1f}° actual={math.degrees(actual_angle):.1f}°")
        
        return {
            "status": "completed",
            "data": {
                "target": target_angle_deg,
                "actual": math.degrees(actual_angle),
                "yaw": math.degrees(current.yaw)
            }
        }
    
    async def handle_look(self, params: Dict, command_id: str) -> Dict:
        """Look mode - tilt body to point camera in a direction (legs stay stationary)
        
        Parameters:
            yaw: target yaw angle in degrees (positive = left, negative = right)
            pitch: target pitch angle in degrees (positive = up, negative = down)
            relative: if True, angles are relative to current orientation (default: True)
            hold: if True, maintain the look until cancelled (default: True)
        """
        target_yaw_deg = params.get("yaw", 0)
        target_pitch_deg = params.get("pitch", 0)
        relative = params.get("relative", True)
        hold = params.get("hold", True)
        timeout = params.get("timeout", 60)  # Default 60s for look mode
        
        # Clamp to safe ranges (Euler limits: pitch/roll ±0.75 rad ≈ ±43°, yaw ±0.6 rad ≈ ±34°)
        target_pitch_deg = max(-40, min(40, target_pitch_deg))
        target_yaw_deg = max(-30, min(30, target_yaw_deg))
        
        logger.info(f"👀 Look mode: yaw={target_yaw_deg}°, pitch={target_pitch_deg}° (relative={relative}, hold={hold})")
        
        self._look_mode_active = True
        
        target_yaw = target_yaw_deg
        target_pitch = target_pitch_deg
        
        start_time = asyncio.get_event_loop().time()
        last_cmd_time = 0
        
        try:
            # Enter Pose mode first (required for body tilting)
            logger.info("  👀 Entering Pose mode...")
            await self._send_sport_command("Pose", {"data": True})
            await asyncio.sleep(0.2)  # Give it time to enter pose mode
            
            # Convert to radians for Euler command (negate pitch - positive = look up)
            yaw_rad = math.radians(target_yaw)
            pitch_rad = math.radians(-target_pitch)  # Negate: positive input = look up
            roll_rad = 0  # Keep roll at 0
            
            # Send Euler command to set the look direction
            await self._send_sport_command("Euler", {"x": roll_rad, "y": pitch_rad, "z": yaw_rad})
            logger.info(f"  👀 Set body orientation: yaw={target_yaw:.1f}°, pitch={target_pitch:.1f}°")
            
            if not hold:
                # One-shot mode - just set and return (stay in pose mode)
                return {
                    "status": "completed",
                    "data": {
                        "yaw": target_yaw,
                        "pitch": target_pitch,
                        "mode": "one-shot"
                    }
                }
            
            # Hold mode - keep sending commands to maintain orientation
            while True:
                current_time = asyncio.get_event_loop().time()
                elapsed = current_time - start_time
                
                # Check for abort
                if self._abort_requested:
                    logger.info(f"🛑 Look aborted")
                    break
                
                # Check timeout
                if elapsed > timeout:
                    logger.info(f"⏱️ Look timeout after {elapsed:.1f}s")
                    break
                
                # Re-send Euler command periodically to maintain position
                if current_time - last_cmd_time >= 0.5:
                    await self._send_sport_command("Euler", {"x": roll_rad, "y": pitch_rad, "z": yaw_rad})
                    last_cmd_time = current_time
                
                # Broadcast progress
                await self._broadcast_progress(command_id, "look", {
                    "yaw": target_yaw,
                    "pitch": target_pitch,
                    "elapsed": elapsed
                })
                
                await asyncio.sleep(0.1)
        
        except asyncio.CancelledError:
            # Cancelled by another command - exit pose mode but don't fully reset
            logger.info("👀 Look cancelled (superseded)")
            self._look_mode_active = False
            # Don't exit pose mode here - let the next command handle it
            raise  # Re-raise to be handled by handle_command
        finally:
            self._look_mode_active = False
        
        # Only reset to neutral and exit pose mode if we completed normally (timeout or abort)
        logger.info("  👀 Exiting Pose mode...")
        try:
            await self._send_sport_command("Euler", {"x": 0, "y": 0, "z": 0})
            await asyncio.sleep(0.1)
            await self._send_sport_command("Pose", {"data": False})
        except:
            pass
        
        return {
            "status": "completed",
            "data": {
                "yaw": target_yaw,
                "pitch": target_pitch,
                "mode": "held"
            }
        }
    
    async def _broadcast_progress(self, command_id: str, command: str, data: Dict):
        """Broadcast command progress to all clients"""
        msg = json.dumps({
            "type": "progress",
            "commandId": command_id,
            "command": command,
            "data": data,
            "timestamp": datetime.now().isoformat()
        })
        for client in self.clients:
            try:
                await client.send(msg)
            except:
                pass
    
    # ============ Other Commands ============
    
    async def handle_play_emote(self, params: Dict, command_id: str) -> Dict:
        emote = params.get("emote", "wave").lower()
        cmd_name = self.emote_map.get(emote)
        if not cmd_name:
            return {"status": "failed", "error": f"Unknown emote: {emote}"}
        await self._send_sport_command(cmd_name)
        return {"status": "completed", "data": {"emote": emote}}
    
    async def handle_set_pose(self, params: Dict, command_id: str) -> Dict:
        pose = params.get("pose", "stand").lower()
        cmd_name = self.pose_map.get(pose)
        if not cmd_name:
            return {"status": "failed", "error": f"Unknown pose: {pose}"}
        await self._send_sport_command(cmd_name)
        return {"status": "completed", "data": {"pose": pose}}
    
    async def handle_set_headlamp(self, params: Dict, command_id: str) -> Dict:
        color = params.get("color", "white")
        brightness = int(params.get("brightness", 1.0) * 100)
        blink = {"solid": 0, "pulse": 1, "blink": 2}.get(params.get("pattern", "solid"), 0)
        
        await self.robot.datachannel.pub_sub.publish_request_new(
            RTC_TOPIC["VUI"],
            {"api_id": 1003, "parameter": {"color": color, "blink": blink, "brightness": brightness}}
        )
        return {"status": "completed"}
    
    async def handle_set_body_tilt(self, params: Dict, command_id: str) -> Dict:
        pitch = math.radians(params.get("pitch", 0))
        roll = math.radians(params.get("roll", 0))
        await self._send_sport_command("Euler", {"x": roll, "y": pitch, "z": 0})
        return {"status": "completed"}
    
    async def _handle_abort_immediate(self, websocket, data: Dict):
        """Handle abort immediately - bypasses command queue"""
        command_id = data.get("id", "unknown")
        logger.info("🛑 ABORT requested - stopping immediately")
        
        # Set abort flag for any running command to check
        self._abort_requested = True
        
        # Cancel any running command task
        if self._current_command_task and not self._current_command_task.done():
            self._current_command_task.cancel()
            try:
                await self._current_command_task
            except asyncio.CancelledError:
                pass
        
        # Send stop command to robot
        try:
            await self._send_sport_command("StopMove")
        except:
            pass
        
        # Reset abort flag
        self._abort_requested = False
        
        await websocket.send(json.dumps({
            "type": "response",
            "commandId": command_id,
            "status": "completed",
            "timestamp": datetime.now().isoformat()
        }))
    
    async def handle_abort(self, params: Dict, command_id: str) -> Dict:
        """Legacy abort handler (shouldn't be called directly now)"""
        self._abort_requested = True
        await self._send_sport_command("StopMove")
        self._abort_requested = False
        return {"status": "completed"}
    
    async def handle_get_state(self, params: Dict, command_id: str) -> Dict:
        state = await self._get_current_state()
        data = asdict(state)
        data["obstacleMap"] = self._obstacle_grid  # Include LiDAR map
        return {"status": "completed", "data": data}
    
    async def handle_take_photo(self, params: Dict, command_id: str) -> Dict:
        """Capture photo"""
        self._photo_chunks = {}
        self._photo_total_chunks = 0
        self._photo_event = asyncio.Event()
        
        @self.robot.datachannel.channel.on("message")
        async def capture_response(message):
            if isinstance(message, bytes) and len(message) > 100:
                try:
                    header_len = struct.unpack_from('<H', message, 0)[0]
                    json_data = message[4:4 + header_len]
                    binary_data = message[4 + header_len:]
                    parsed = json.loads(json_data.decode('utf-8'))
                    
                    if parsed.get('type') == 'res' and 'videohub' in parsed.get('topic', ''):
                        info = parsed.get('data', {}).get('content_info', {})
                        if info.get('enable_chunking'):
                            idx = info.get('chunk_index', 0)
                            self._photo_total_chunks = info.get('total_chunk_num', 1)
                            self._photo_chunks[idx] = binary_data
                            if len(self._photo_chunks) >= self._photo_total_chunks:
                                self._photo_event.set()
                except:
                    pass
        
        try:
            await asyncio.wait_for(
                self.robot.datachannel.pub_sub.publish_request_new(
                    RTC_TOPIC["FRONT_PHOTO_REQ"], {"api_id": 1001}
                ), timeout=2
            )
        except asyncio.TimeoutError:
            pass
        
        try:
            await asyncio.wait_for(self._photo_event.wait(), timeout=10)
            image = b''.join(self._photo_chunks[i] for i in range(1, self._photo_total_chunks + 1))
            return {
                "status": "completed",
                "data": {"image": base64.b64encode(image).decode(), "size": len(image)}
            }
        except asyncio.TimeoutError:
            return {"status": "failed", "error": "Photo capture timed out"}
    
    # ============ State Streaming ============
    
    async def handle_start_stream(self, params: Dict, command_id: str) -> Dict:
        """Start streaming state to clients"""
        if not self._streaming:
            self._streaming = True
            self._stream_task = asyncio.create_task(self._state_stream_loop())
        return {"status": "completed"}
    
    async def handle_stop_stream(self, params: Dict, command_id: str) -> Dict:
        """Stop streaming state"""
        self._streaming = False
        if self._stream_task:
            self._stream_task.cancel()
        return {"status": "completed"}
    
    async def _state_stream_loop(self):
        """Stream state at 10Hz"""
        while self._streaming:
            try:
                state = await self._get_current_state()
                msg = json.dumps({
                    "type": "state",
                    "robotConnected": self.robot_connected,
                    "data": asdict(state),
                    "obstacleMap": self._obstacle_grid,
                    "timestamp": datetime.now().isoformat()
                })
                for client in list(self.clients):
                    try:
                        await client.send(msg)
                    except:
                        pass
                await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Stream error: {e}")
                await asyncio.sleep(0.5)
    
    # ============ WebSocket Server ============
    
    async def handle_client(self, websocket: WebSocketServerProtocol):
        self.clients.add(websocket)
        logger.info(f"Client connected: {websocket.remote_address}")
        
        # Send initial state and connection status
        state = await self._get_current_state()
        await websocket.send(json.dumps({
            "type": "connected",
            "robotConnected": self.robot_connected,
            "data": asdict(state),
            "timestamp": datetime.now().isoformat()
        }))
        
        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    command = data.get("command")
                    
                    # Abort is handled immediately, not queued
                    if command == "abort":
                        await self._handle_abort_immediate(websocket, data)
                    else:
                        # Run command as background task so message loop continues
                        # This allows abort to be received during long commands
                        self._current_command_task = asyncio.create_task(
                            self.handle_command(websocket, data)
                        )
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({"type": "error", "error": "Invalid JSON"}))
                except Exception as e:
                    logger.exception(f"Error: {e}")
                    await websocket.send(json.dumps({"type": "error", "error": str(e)}))
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self.clients.discard(websocket)
            logger.info(f"Client disconnected")
    
    async def handle_command(self, websocket: WebSocketServerProtocol, data: Dict):
        command = data.get("command")
        command_id = data.get("id", "unknown")
        params = data.get("params", {})
        
        handler = self.command_handlers.get(command)
        if not handler:
            await websocket.send(json.dumps({
                "type": "error",
                "commandId": command_id,
                "error": f"Unknown command: {command}"
            }))
            return
        
        # Movement commands cancel any existing movement
        is_movement = command in ("move", "turn", "look")
        if is_movement:
            await self._cancel_existing_movement()
        
        # Send queued
        await websocket.send(json.dumps({
            "type": "response",
            "commandId": command_id,
            "status": "queued"
        }))
        
        try:
            # Wrap movement commands so we can track/cancel them
            if is_movement:
                self._movement_task = asyncio.current_task()
            
            result = await handler(params, command_id)
            await websocket.send(json.dumps({
                "type": "response",
                "commandId": command_id,
                **result,
                "timestamp": datetime.now().isoformat()
            }))
        except asyncio.CancelledError:
            # Movement was cancelled by a new command - this is expected
            logger.info(f"Command {command} cancelled (superseded by new command)")
            await websocket.send(json.dumps({
                "type": "response",
                "commandId": command_id,
                "status": "failed",
                "error": "cancelled",
                "data": {"reason": "Superseded by new command"},
                "timestamp": datetime.now().isoformat()
            }))
        except Exception as e:
            await websocket.send(json.dumps({
                "type": "error",
                "commandId": command_id,
                "error": str(e)
            }))
        finally:
            if is_movement:
                self._movement_task = None
    
    async def run(self):
        # Try initial connection (don't fail if robot not available)
        try:
            await self.connect_to_robot()
        except SystemExit:
            logger.warning("Initial connection failed (driver exit), will retry...")
        
        # Start reconnect loop
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())
        
        logger.info(f"Starting WebSocket server on ws://{self.host}:{self.port}")
        
        async with websockets.serve(self.handle_client, self.host, self.port, ping_interval=20):
            logger.info("🚀 Go2 Server v2 running!")
            if not self.robot_connected:
                logger.info("⏳ Waiting for robot to come online...")
            await asyncio.Future()


async def main():
    import sys
    serial = sys.argv[1] if len(sys.argv) > 1 else "B42D1000P57B6K09"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8765
    
    server = Go2ServerV2(serial_number=serial, port=port)
    await server.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Server stopped")
