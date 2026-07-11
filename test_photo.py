#!/usr/bin/env python3
"""Test photo capture through the server"""

import asyncio
import sys
sys.path.insert(0, '/Users/olena/connectome-local/dog_mcp')

from robot_dog_client import RobotDogClient

async def main():
    robot = RobotDogClient('localhost', 8765)
    
    print("🔌 Connecting to Go2 server...")
    await robot.connect()
    print("✅ Connected!")
    
    print("\n📸 Taking photo...")
    result = await robot.take_photo(save_path="/Users/olena/connectome-local/dog_mcp/test_photo.jpg")
    print(f"Result: {result.get('status')}")
    
    if result.get('data', {}).get('saved_path'):
        print(f"📷 Photo saved to: {result['data']['saved_path']}")
    
    await robot.disconnect()
    print("✅ Done!")

if __name__ == "__main__":
    asyncio.run(main())
