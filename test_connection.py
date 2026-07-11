#!/usr/bin/env python3
"""
Simple connection test for Unitree Go2

Tests connection methods in order:
1. LocalSTA with serial number (multicast discovery)
2. LocalAP (if connected directly to dog's WiFi)
"""

import asyncio
import logging
import sys
from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
from unitree_webrtc_connect.multicast_scanner import discover_ip_sn

# Your Go2 serial number
SERIAL_NUMBER = "B42D1000P57B6K09"

# Set logging to INFO for connection status
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def test_multicast_discovery():
    """Try to discover the Go2 on the local network"""
    print("\n🔍 Scanning for Go2 on local network (multicast discovery)...")
    try:
        discovered = discover_ip_sn()
        if discovered:
            print(f"✅ Found devices: {discovered}")
            if SERIAL_NUMBER in discovered:
                print(f"✅ Your Go2 found at: {discovered[SERIAL_NUMBER]}")
                return discovered[SERIAL_NUMBER]
            else:
                print(f"⚠️  Your serial {SERIAL_NUMBER} not found in discovered devices")
        else:
            print("❌ No Go2 devices found on local network")
    except Exception as e:
        print(f"❌ Multicast scan error: {e}")
    return None

async def test_connection_sta(ip: str):
    """Test LocalSTA connection with known IP"""
    print(f"\n🔌 Attempting LocalSTA connection to {ip}...")
    try:
        conn = UnitreeWebRTCConnection(WebRTCConnectionMethod.LocalSTA, ip=ip)
        await asyncio.wait_for(conn.connect(), timeout=30)
        
        if conn.isConnected:
            print("✅ Successfully connected via LocalSTA!")
            return conn
        else:
            print("❌ Connection did not complete")
    except asyncio.TimeoutError:
        print("❌ Connection timed out (30s)")
    except Exception as e:
        print(f"❌ LocalSTA connection error: {e}")
    return None

async def test_connection_ap():
    """Test LocalAP connection (dog's hotspot)"""
    print("\n🔌 Attempting LocalAP connection (192.168.12.1)...")
    print("   (This requires you to be connected to the Go2's WiFi hotspot)")
    try:
        conn = UnitreeWebRTCConnection(WebRTCConnectionMethod.LocalAP)
        await asyncio.wait_for(conn.connect(), timeout=30)
        
        if conn.isConnected:
            print("✅ Successfully connected via LocalAP!")
            return conn
        else:
            print("❌ Connection did not complete")
    except asyncio.TimeoutError:
        print("❌ Connection timed out (30s)")
    except Exception as e:
        print(f"❌ LocalAP connection error: {e}")
    return None

async def test_connection_serial():
    """Test LocalSTA connection using serial number discovery"""
    print(f"\n🔌 Attempting LocalSTA connection using serial number {SERIAL_NUMBER}...")
    try:
        conn = UnitreeWebRTCConnection(WebRTCConnectionMethod.LocalSTA, serialNumber=SERIAL_NUMBER)
        await asyncio.wait_for(conn.connect(), timeout=30)
        
        if conn.isConnected:
            print("✅ Successfully connected via LocalSTA (serial discovery)!")
            return conn
        else:
            print("❌ Connection did not complete")
    except asyncio.TimeoutError:
        print("❌ Connection timed out (30s)")
    except Exception as e:
        print(f"❌ Serial connection error: {e}")
    return None

async def main():
    print("=" * 60)
    print("🐕 Unitree Go2 Connection Test")
    print(f"   Serial: {SERIAL_NUMBER}")
    print("=" * 60)
    
    conn = None
    
    # Method 1: Try multicast discovery first to find IP
    ip = await test_multicast_discovery()
    
    if ip:
        # Method 2: Connect with discovered IP
        conn = await test_connection_sta(ip)
    else:
        # Method 3: Try serial number based connection
        conn = await test_connection_serial()
    
    if not conn:
        # Method 4: Fall back to AP mode
        conn = await test_connection_ap()
    
    if conn:
        print("\n" + "=" * 60)
        print("🎉 CONNECTION SUCCESSFUL!")
        print("=" * 60)
        
        # Quick status check
        print("\n📊 Checking motion mode...")
        try:
            from unitree_webrtc_connect.constants import RTC_TOPIC
            import json
            
            response = await conn.datachannel.pub_sub.publish_request_new(
                RTC_TOPIC["MOTION_SWITCHER"], 
                {"api_id": 1001}
            )
            
            if response['data']['header']['status']['code'] == 0:
                data = json.loads(response['data']['data'])
                print(f"   Current motion mode: {data['name']}")
        except Exception as e:
            print(f"   Could not get motion mode: {e}")
        
        print("\n✅ Test complete! Press Ctrl+C to disconnect.")
        
        # Keep connection alive briefly
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            pass
        
        await conn.disconnect()
    else:
        print("\n" + "=" * 60)
        print("❌ COULD NOT CONNECT")
        print("=" * 60)
        print("\nTroubleshooting:")
        print("  1. Is the Go2 powered on?")
        print("  2. Is it on the same network as this computer?")
        print("  3. Try connecting to the Go2's WiFi hotspot directly")
        print("  4. Make sure no other app (Unitree app) is connected")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n👋 Disconnected by user")
        sys.exit(0)
