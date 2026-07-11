#!/usr/bin/env python3
"""
Connection test with proper monkeypatch order
"""

# CRITICAL: Import unitree_webrtc_connect FIRST to apply monkeypatches
import unitree_webrtc_connect

# Verify the patch was applied
import aiortc.rtcdtlstransport as dtls
print(f"X509_DIGEST_ALGORITHMS: {list(dtls.X509_DIGEST_ALGORITHMS.keys())}")

import asyncio
import logging
import json
import sys
from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
from unitree_webrtc_connect.constants import RTC_TOPIC

logging.basicConfig(level=logging.INFO)

SERIAL = "B42D1000P57B6K09"

async def main():
    print("=" * 50)
    print(f"🐕 Go2 Connection Test (patched)")
    print("=" * 50)
    
    try:
        conn = UnitreeWebRTCConnection(WebRTCConnectionMethod.LocalSTA, serialNumber=SERIAL)
        await conn.connect()
        
        print("🎉 CONNECTED!")
        
        # Check motion mode
        response = await conn.datachannel.pub_sub.publish_request_new(
            RTC_TOPIC["MOTION_SWITCHER"], 
            {"api_id": 1001}
        )
        if response['data']['header']['status']['code'] == 0:
            data = json.loads(response['data']['data'])
            print(f"📊 Motion mode: {data['name']}")
        
        print("\n✅ Success! Ctrl+C to exit")
        await asyncio.sleep(60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Bye!")
