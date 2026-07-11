#!/usr/bin/env python3
"""
Test the Go2 server by sending commands via the client
"""

import asyncio
import logging
from robot_dog_client import RobotDogClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    # Connect to local server on port 8765
    robot = RobotDogClient("localhost", 8765)
    
    try:
        print("🔌 Connecting to Go2 Server...")
        await robot.connect()
        print("✅ Connected!")
        
        # Get current state
        print("\n📊 Getting state...")
        state = await robot.get_state()
        print(f"   State: {state}")
        
        # Make the dog wave!
        print("\n👋 Playing wave emote...")
        result = await robot.play_emote("wave")
        print(f"   Result: {result}")
        
        await asyncio.sleep(3)
        
        print("\n✅ Test complete!")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await robot.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
