import json
import os
import time
import threading
from datetime import datetime, timezone
from typing import Dict, Any
import redis
import requests
from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
import uvicorn
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="Scheduler API", version="1.0.0")

API_TOKEN = os.getenv('API_TOKEN')

# Store active timers to manage them
active_timers = {}
timer_lock = threading.Lock()

def verify_token(authorization: str = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Authorization header required")
    
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization format")
    
    token = authorization.replace("Bearer ", "")
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    return token

redis_client = redis.Redis(
    host=os.getenv('REDIS_HOST', 'localhost'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    password=os.getenv('REDIS_PASSWORD'),
    decode_responses=True
)

class ScheduleMessage(BaseModel):
    id: str
    scheduleTo: str
    payload: Dict[str, Any]
    webhookUrl: str

def fire_webhook(message_id: str, webhook_url: str, payload: Dict[str, Any]):
    try:
        response = requests.post(webhook_url, json=payload, timeout=30)
        response.raise_for_status()
        print(f"[{datetime.now().isoformat()}] Webhook fired successfully for message {message_id}")
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Failed to fire webhook for message {message_id}: {e}")
    finally:
        # Clean up Redis and timer tracking
        redis_client.delete(f"message:{message_id}")
        with timer_lock:
            if message_id in active_timers:
                del active_timers[message_id]
        print(f"[{datetime.now().isoformat()}] Message {message_id} cleaned from Redis and timer tracking")

def parse_schedule_time(schedule_timestamp: str) -> datetime:
    """Parse schedule timestamp and handle different formats"""
    # Handle both Z and without Z
    if schedule_timestamp.endswith('Z'):
        schedule_timestamp = schedule_timestamp[:-1] + '+00:00'
    elif '+' not in schedule_timestamp and '-' not in schedule_timestamp[-6:]:
        # No timezone info, assume UTC
        schedule_timestamp += '+00:00'
    
    return datetime.fromisoformat(schedule_timestamp)

def schedule_message(message_id: str, schedule_timestamp: str, webhook_url: str, payload: Dict[str, Any]):
    try:
        schedule_time = parse_schedule_time(schedule_timestamp)
        current_time = datetime.now(timezone.utc)
        
        print(f"[{datetime.now().isoformat()}] Scheduling message {message_id}")
        print(f"[{datetime.now().isoformat()}] Schedule time: {schedule_time}")
        print(f"[{datetime.now().isoformat()}] Current time: {current_time}")
        
        # If scheduled time is in the past, execute immediately
        if schedule_time <= current_time:
            print(f"[{datetime.now().isoformat()}] Schedule time is in the past, executing immediately")
            fire_webhook(message_id, webhook_url, payload)
            return
        
        # Calculate delay in seconds
        delay = (schedule_time - current_time).total_seconds()
        print(f"[{datetime.now().isoformat()}] Scheduling webhook to fire in {delay} seconds")
        
        def delayed_execution():
            print(f"[{datetime.now().isoformat()}] Executing scheduled webhook for message {message_id}")
            fire_webhook(message_id, webhook_url, payload)
        
        # Create and start timer
        timer = threading.Timer(delay, delayed_execution)
        timer.start()
        
        # Store timer reference for management
        with timer_lock:
            active_timers[message_id] = {
                'timer': timer,
                'schedule_time': schedule_time,
                'webhook_url': webhook_url,
                'payload': payload
            }
        
        print(f"[{datetime.now().isoformat()}] Timer created and started for message {message_id}")
        
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Error scheduling message {message_id}: {e}")
        raise

def restore_scheduled_messages():
    """Restore scheduled messages from Redis on server restart"""
    try:
        keys = redis_client.keys("message:*")
        restored_count = 0
        
        for key in keys:
            try:
                message_data = json.loads(redis_client.get(key))
                message_id = message_data["id"]
                schedule_to = message_data["scheduleTo"]
                webhook_url = message_data["webhookUrl"]
                payload = message_data["payload"]
                
                schedule_message(message_id, schedule_to, webhook_url, payload)
                restored_count += 1
                print(f"[{datetime.now().isoformat()}] Restored scheduled message - ID: {message_id}")
                
            except Exception as e:
                print(f"[{datetime.now().isoformat()}] Failed to restore message {key}: {e}")
                # Remove corrupted message from Redis
                redis_client.delete(key)
        
        print(f"[{datetime.now().isoformat()}] Restored {restored_count} scheduled messages from Redis")
        
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Error restoring messages: {e}")

# Restore messages on startup
restore_scheduled_messages()

@app.post("/messages")
async def create_scheduled_message(message: ScheduleMessage, token: str = Depends(verify_token)):
    try:
        redis_key = f"message:{message.id}"
        
        # Check if message already exists
        if redis_client.exists(redis_key):
            print(f"[{datetime.now().isoformat()}] Message already exists in Redis - ID: {message.id}")
            raise HTTPException(status_code=409, detail="Message with this ID already exists")
        
        # Validate schedule time format
        try:
            parse_schedule_time(message.scheduleTo)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid scheduleTo format: {str(e)}")
        
        # Store message data in Redis
        message_data = {
            "id": message.id,
            "scheduleTo": message.scheduleTo,
            "payload": message.payload,
            "webhookUrl": message.webhookUrl
        }
        
        redis_client.set(redis_key, json.dumps(message_data))
        print(f"[{datetime.now().isoformat()}] Message inserted to Redis - ID: {message.id}")
        
        # Schedule the message
        schedule_message(message.id, message.scheduleTo, message.webhookUrl, message.payload)
        
        return {"status": "scheduled", "messageId": message.id}
    
    except HTTPException as http_exc:
        print(f"[{datetime.now().isoformat()}] HTTPException in create: {http_exc.status_code} - {http_exc.detail}")
        raise
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Unexpected exception in create: {type(e).__name__}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to schedule message: {str(e)}")

@app.delete("/messages/{message_id}")
async def delete_scheduled_message(message_id: str, token: str = Depends(verify_token)):
    try:
        redis_key = f"message:{message_id}"
        
        # Check if message exists in Redis
        if not redis_client.exists(redis_key):
            print(f"[{datetime.now().isoformat()}] Message not found in Redis - ID: {message_id}")
            raise HTTPException(status_code=404, detail="Message not found")
        
        # Remove from Redis
        redis_client.delete(redis_key)
        print(f"[{datetime.now().isoformat()}] Message deleted from Redis - ID: {message_id}")
        
        # Cancel timer if exists
        with timer_lock:
            if message_id in active_timers:
                active_timers[message_id]['timer'].cancel()
                del active_timers[message_id]
                print(f"[{datetime.now().isoformat()}] Timer cancelled for message - ID: {message_id}")
        
        return {"status": "deleted", "messageId": message_id}
    
    except HTTPException as http_exc:
        print(f"[{datetime.now().isoformat()}] HTTPException in delete: {http_exc.status_code} - {http_exc.detail}")
        raise
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Unexpected exception in delete: {type(e).__name__}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to delete message: {str(e)}")

@app.get("/messages")
async def list_scheduled_messages(token: str = Depends(verify_token)):
    try:
        jobs = []
        
        with timer_lock:
            for message_id, timer_info in active_timers.items():
                # Check if timer is still active
                if timer_info['timer'].is_alive():
                    jobs.append({
                        "messageId": message_id,
                        "nextRun": timer_info['schedule_time'].isoformat(),
                        "status": "scheduled",
                        "webhookUrl": timer_info['webhook_url']
                    })
        
        return {"scheduledJobs": jobs, "count": len(jobs)}
    
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] Error listing jobs: {type(e).__name__}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to list jobs: {str(e)}")

@app.get("/health")
async def health_check():
    try:
        redis_client.ping()
        return {
            "status": "healthy", 
            "redis": "connected",
            "active_timers": len(active_timers),
            "server_time": datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy", 
            "redis": "disconnected", 
            "error": str(e),
            "server_time": datetime.now(timezone.utc).isoformat()
        }

if __name__ == "__main__":
    print(f"[{datetime.now().isoformat()}] Starting Scheduler API server")
    print(f"[{datetime.now().isoformat()}] Server timezone: {datetime.now().astimezone().tzinfo}")
    print(f"[{datetime.now().isoformat()}] Server time (UTC): {datetime.now(timezone.utc).isoformat()}")
    uvicorn.run(app, host="0.0.0.0", port=8000)
