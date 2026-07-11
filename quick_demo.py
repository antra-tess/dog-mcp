#!/usr/bin/env python3
"""
Quick demo of the Robot Dog API

This script shows a simple, practical example of how to use the robot dog client.
"""

import asyncio
import logging
from robot_dog_client import RobotDogClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def distance_callback(distances):
    """Callback for LIDAR distance updates"""
    front_distance = distances.get("front", 0)
    if front_distance < 0.5:
        logger.warning(f"Obstacle detected! Front distance: {front_distance:.2f}m")

async def patrol_demo():
    """Simple patrol pattern with obstacle avoidance"""
    robot = RobotDogClient("localhost", 8080)
    
    try:
        # Connect to robot
        await robot.connect()
        logger.info("Connected to robot dog")
        
        # Enable obstacle avoidance and set up monitoring
        await robot.set_object_avoidance(True)
        await robot.subscribe_lidar_distance(
            frequency=10, 
            zones=["front"], 
            callback=distance_callback
        )
        
        # Set blue headlamp for patrol mode
        await robot.set_headlamp("#0080FF", brightness=0.8, pattern="pulse")
        
        # Patrol pattern: square with 2m sides
        for i in range(4):
            logger.info(f"Patrol leg {i+1}/4")
            
            # Move forward 2 meters
            await robot.move(2.0, speed=0.5)
            
            # Turn 90 degrees left (makes a square)
            await robot.turn(90)
            
            # Add some body movement for style
            await robot.set_body_tilt(pitch=5, roll=0, duration=1)
            await asyncio.sleep(1)
            await robot.reset_body_tilt(duration=1)
        
        # End patrol with a wave
        await robot.play_emote("wave")
        logger.info("Patrol complete!")
        
    except Exception as e:
        logger.error(f"Error during patrol: {e}")
    finally:
        await robot.disconnect()

async def dance_demo():
    """Fun dance sequence"""
    robot = RobotDogClient("localhost", 8080)
    
    try:
        await robot.connect()
        
        # Rainbow headlamp sequence
        colors = ["#FF0000", "#FF8000", "#FFFF00", "#00FF00", "#0080FF", "#8000FF"]
        
        for color in colors:
            await robot.set_headlamp(color, brightness=1.0, pattern="blink")
            await robot.set_body_tilt(pitch=10, roll=10, duration=0.5)
            await asyncio.sleep(0.5)
            await robot.reset_body_tilt(duration=0.5)
            await asyncio.sleep(0.5)
        
        # Finish with dance emote
        await robot.play_emote("dance")
        
    finally:
        await robot.disconnect()

async def simple_control():
    """Simple manual control example"""
    robot = RobotDogClient("localhost", 8080)
    
    try:
        await robot.connect()
        
        # Get current state
        state = await robot.get_state()
        battery = state["data"]["battery"]
        logger.info(f"Battery: {battery:.1%}")
        
        # Simple command sequence
        await robot.set_gait("trot")
        await robot.move(1.0)
        await robot.turn(45)
        await robot.move(1.0)
        await robot.turn(-45)
        await robot.move(-1.0)
        
        # Sit down
        await robot.set_pose("sit")
        await asyncio.sleep(2)
        await robot.exit_pose()
        
    finally:
        await robot.disconnect()

if __name__ == "__main__":
    # Choose which demo to run
    import sys
    
    if len(sys.argv) > 1:
        demo_type = sys.argv[1]
        if demo_type == "patrol":
            asyncio.run(patrol_demo())
        elif demo_type == "dance":
            asyncio.run(dance_demo())
        elif demo_type == "simple":
            asyncio.run(simple_control())
        else:
            print("Available demos: patrol, dance, simple")
    else:
        print("Usage: python quick_demo.py [patrol|dance|simple]")
        print("Running simple demo by default...")
        asyncio.run(simple_control()) 