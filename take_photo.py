#!/usr/bin/env python3
"""
Capture a photo from Go2's camera
Simpler approach - save raw response
"""

import unitree_webrtc_connect
import asyncio
import json
import struct
import logging
from datetime import datetime

logging.basicConfig(level=logging.WARNING)

async def main():
    from unitree_webrtc_connect.webrtc_driver import UnitreeWebRTCConnection, WebRTCConnectionMethod
    from unitree_webrtc_connect.constants import RTC_TOPIC
    
    print("📷 Connecting to Go2...")
    conn = UnitreeWebRTCConnection(
        WebRTCConnectionMethod.LocalSTA, 
        serialNumber="B42D1000P57B6K09"
    )
    
    await conn.connect()
    print("✅ Connected!")
    
    # Storage for image chunks
    image_chunks = {}
    total_chunks_needed = [0]
    image_ready = asyncio.Event()
    
    # Add custom handler for binary messages
    original_on_message = None
    
    @conn.datachannel.channel.on("message")
    async def capture_photo_response(message):
        if isinstance(message, bytes) and len(message) > 100:
            # Parse binary message
            header_length = struct.unpack_from('<H', message, 0)[0]
            json_data = message[4:4 + header_length]
            binary_data = message[4 + header_length:]
            
            try:
                parsed = json.loads(json_data.decode('utf-8'))
                
                if parsed.get('type') == 'res' and 'videohub' in parsed.get('topic', ''):
                    content_info = parsed.get('data', {}).get('content_info', {})
                    
                    if content_info.get('enable_chunking'):
                        chunk_index = content_info.get('chunk_index', 0)
                        total_chunks_needed[0] = content_info.get('total_chunk_num', 1)
                        
                        print(f"  📦 Chunk {chunk_index}/{total_chunks_needed[0]} ({len(binary_data)} bytes)")
                        image_chunks[chunk_index] = binary_data
                        
                        if len(image_chunks) >= total_chunks_needed[0]:
                            image_ready.set()
                    else:
                        image_chunks[1] = binary_data
                        total_chunks_needed[0] = 1
                        image_ready.set()
            except Exception as e:
                pass
    
    # Request photo
    print("📸 Requesting photo...")
    
    # Use pub_sub to send request
    try:
        # This will timeout but that's OK - we're capturing in the handler
        response = await asyncio.wait_for(
            conn.datachannel.pub_sub.publish_request_new(
                RTC_TOPIC["FRONT_PHOTO_REQ"],
                {"api_id": 1001}
            ),
            timeout=3
        )
    except asyncio.TimeoutError:
        pass  # Expected - response is binary not JSON
    
    # Wait for chunks
    print("⏳ Waiting for image chunks...")
    try:
        await asyncio.wait_for(image_ready.wait(), timeout=10)
        
        # Combine chunks in order
        full_image = b''
        for i in range(1, total_chunks_needed[0] + 1):
            if i in image_chunks:
                full_image += image_chunks[i]
        
        # Save
        if full_image:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"/Users/olena/connectome-local/dog_mcp/photo_{timestamp}.jpg"
            with open(filename, 'wb') as f:
                f.write(full_image)
            print(f"✅ Photo saved: {filename} ({len(full_image)} bytes)")
            await conn.disconnect()
            return filename
        
    except asyncio.TimeoutError:
        print("❌ Timeout")
    
    await conn.disconnect()
    return None

if __name__ == "__main__":
    result = asyncio.run(main())
    if result:
        print(f"\n📷 Photo: {result}")
