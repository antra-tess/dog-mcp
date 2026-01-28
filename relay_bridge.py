#!/usr/bin/env python3
"""
Go2 Relay Bridge

Runs alongside the robot's local WebSocket server. Connects outbound to
the cloud relay and forwards messages bidirectionally.

This enables remote access without port forwarding or NAT configuration.

Environment variables:
  - GO2_RELAY_URL: WebSocket URL of relay server (e.g., wss://your-app.railway.app)
  - GO2_RELAY_SECRET: Shared authentication token
  - GO2_ROBOT_ID: Unique identifier for this robot (default: "go2-home")
  - GO2_LOCAL_WS: Local WebSocket server URL (default: ws://localhost:8765)
"""

import asyncio
import json
import os
import logging
from datetime import datetime

import websockets

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment
RELAY_URL = os.environ.get("GO2_RELAY_URL", "ws://localhost:8080")
RELAY_SECRET = os.environ.get("GO2_RELAY_SECRET", "change-me-in-production")
ROBOT_ID = os.environ.get("GO2_ROBOT_ID", "go2-home")
LOCAL_WS = os.environ.get("GO2_LOCAL_WS", "ws://localhost:8765")

RECONNECT_DELAY = 5  # seconds


class RelayBridge:
    def __init__(self):
        self.relay_ws = None
        self.local_ws = None
        self.running = True
        
    async def connect_relay(self):
        """Connect to cloud relay as robot role"""
        while self.running:
            try:
                logger.info(f"🌐 Connecting to relay: {RELAY_URL}")
                async with websockets.connect(RELAY_URL) as ws:
                    self.relay_ws = ws
                    
                    # Authenticate
                    auth_msg = json.dumps({
                        "type": "auth",
                        "token": RELAY_SECRET,
                        "role": "robot",
                        "robot_id": ROBOT_ID
                    })
                    await ws.send(auth_msg)
                    logger.info(f"🔐 Authenticated as robot '{ROBOT_ID}'")
                    
                    # Forward messages from relay to local server
                    async for message in ws:
                        if self.local_ws:
                            try:
                                await self.local_ws.send(message)
                            except Exception as e:
                                logger.error(f"Failed to forward to local: {e}")
                        else:
                            logger.warning("Received message but local not connected")
                            
            except websockets.ConnectionClosed as e:
                logger.warning(f"🌐 Relay connection closed: {e}")
            except Exception as e:
                logger.error(f"🌐 Relay connection error: {e}")
            finally:
                self.relay_ws = None
                
            if self.running:
                logger.info(f"🔄 Reconnecting to relay in {RECONNECT_DELAY}s...")
                await asyncio.sleep(RECONNECT_DELAY)
    
    async def connect_local(self):
        """Connect to local robot WebSocket server"""
        while self.running:
            try:
                logger.info(f"🤖 Connecting to local server: {LOCAL_WS}")
                async with websockets.connect(LOCAL_WS) as ws:
                    self.local_ws = ws
                    logger.info("🤖 Connected to local robot server")
                    
                    # Forward messages from local to relay
                    async for message in ws:
                        if self.relay_ws:
                            try:
                                await self.relay_ws.send(message)
                            except Exception as e:
                                logger.error(f"Failed to forward to relay: {e}")
                        # Messages still flow even if relay disconnected
                        # (they just won't reach remote clients)
                            
            except websockets.ConnectionClosed as e:
                logger.warning(f"🤖 Local connection closed: {e}")
            except Exception as e:
                logger.error(f"🤖 Local connection error: {e}")
            finally:
                self.local_ws = None
                
            if self.running:
                logger.info(f"🔄 Reconnecting to local in {RECONNECT_DELAY}s...")
                await asyncio.sleep(RECONNECT_DELAY)
    
    async def run(self):
        """Run both connections concurrently"""
        logger.info("🌉 Go2 Relay Bridge starting")
        logger.info(f"   Relay: {RELAY_URL}")
        logger.info(f"   Local: {LOCAL_WS}")
        logger.info(f"   Robot ID: {ROBOT_ID}")
        
        try:
            await asyncio.gather(
                self.connect_relay(),
                self.connect_local()
            )
        except KeyboardInterrupt:
            logger.info("🛑 Shutting down...")
            self.running = False


async def main():
    bridge = RelayBridge()
    await bridge.run()


if __name__ == "__main__":
    asyncio.run(main())
