#!/usr/bin/env python3
"""
Simple test - based on official example
"""

import asyncio
import logging
import json
import sys
from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
from unitree_webrtc_connect.constants import RTC_TOPIC, SPORT_CMD

# Logging for debugging
logging.basicConfig(level=logging.INFO)

async def main():
    try:
        # Connect using serial number discovery
        conn = UnitreeWebRTCConnection(WebRTCConnectionMethod.LocalSTA, serialNumber="B42D1000P57B6K09")
        
        # Connect
        await conn.connect()

        print("🎉 CONNECTED!")
        
        # Check motion mode
        print("📊 Checking motion mode...")
        response = await conn.datachannel.pub_sub.publish_request_new(
            RTC_TOPIC["MOTION_SWITCHER"], 
            {"api_id": 1001}
        )

        if response['data']['header']['status']['code'] == 0:
            data = json.loads(response['data']['data'])
            print(f"   Current mode: {data['name']}")

        # Keep alive
        print("\n✅ Success! Press Ctrl+C to exit")
        await asyncio.sleep(60)
    
    except Exception as e:
        logging.error(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bye!")
        sys.exit(0)
