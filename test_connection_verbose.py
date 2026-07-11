#!/usr/bin/env python3
"""
Verbose connection test with extended timeout
"""

import asyncio
import logging
import sys
import json

# Enable verbose logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
from unitree_webrtc_connect.multicast_scanner import discover_ip_sn

SERIAL_NUMBER = "B42D1000P57B6K09"

async def main():
    print("=" * 60)
    print("🐕 Verbose Go2 Connection Test")
    print("=" * 60)
    
    # Discover IP
    print("\n🔍 Discovering Go2...")
    discovered = discover_ip_sn(timeout=3)
    if not discovered or SERIAL_NUMBER not in discovered:
        print("❌ Go2 not found!")
        return
    
    ip = discovered[SERIAL_NUMBER]
    print(f"✅ Found at {ip}")
    
    # Create connection - but we'll manage the timeout ourselves
    print(f"\n🔌 Connecting...")
    conn = UnitreeWebRTCConnection(WebRTCConnectionMethod.LocalSTA, ip=ip)
    
    # Monkey-patch the datachannel timeout to be longer
    original_wait = conn.__class__.__bases__
    
    try:
        # Start connection but handle timeout ourselves
        print("Starting WebRTC connection...")
        
        # Call connect parts manually with longer timeout
        from unitree_webrtc_connect.util import print_status
        print_status("WebRTC connection", "🟡 started")
        
        await conn.init_webrtc(ip=ip)
        
        # Wait longer for data channel
        print("\n⏳ Waiting for data channel (up to 15 seconds)...")
        for i in range(150):  # 15 seconds
            if conn.datachannel.data_channel_opened:
                break
            await asyncio.sleep(0.1)
            if i % 10 == 0:
                print(f"   {i/10}s... channel state: {conn.datachannel.channel.readyState}")
        
        if conn.datachannel.data_channel_opened:
            print("\n✅ DATA CHANNEL OPEN!")
            conn.isConnected = True
            
            # Get motion status
            print("\n📊 Getting motion mode...")
            from unitree_webrtc_connect.constants import RTC_TOPIC
            response = await conn.datachannel.pub_sub.publish_request_new(
                RTC_TOPIC["MOTION_SWITCHER"], 
                {"api_id": 1001}
            )
            if response['data']['header']['status']['code'] == 0:
                data = json.loads(response['data']['data'])
                print(f"   Motion mode: {data['name']}")
            
            print("\n🎉 SUCCESS! Connected to Go2")
            print("Press Ctrl+C to disconnect...")
            await asyncio.sleep(30)
        else:
            print(f"\n❌ Data channel did not open")
            print(f"   Channel state: {conn.datachannel.channel.readyState}")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn.pc:
            await conn.pc.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Disconnected")
