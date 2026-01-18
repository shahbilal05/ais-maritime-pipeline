import json
import logging
import time
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
import redis
import requests
from utils import haversine_distance, calculate_cpa, is_collision_risk

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "ais-positions"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")  

# store recent vessel data
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def process_vessel(vessel_data):
    mmsi = vessel_data['mmsi']
    redis_client.setex(f"vessel:{mmsi}", 300, json.dumps(vessel_data)) # store key-val pair & deletes in 5 mins
    
    nearby_vessels = get_nearby_vessels(vessel_data)
    
    for other_vessel in nearby_vessels:
        if other_vessel['mmsi'] == mmsi:
            continue
        
        cpa_distance, time_to_cpa = calculate_cpa(vessel_data, other_vessel)
        
        if is_collision_risk(cpa_distance, time_to_cpa, vessel_data, other_vessel):
            alert_collision_risk(vessel_data, other_vessel, cpa_distance, time_to_cpa)

# check which nearby vessels are within 10 nm for collision risk
def get_nearby_vessels(vessel):
    nearby = []
    for key in redis_client.scan_iter("vessel:*", count=100): # iterate over vessel keys in Redis
        try:
            other_vessel_json = redis_client.get(key)
            if not other_vessel_json:
                continue
            
            other_vessel = json.loads(other_vessel_json)
            distance = haversine_distance(
                vessel['lat'], vessel['lon'],
                other_vessel['lat'], other_vessel['lon']
            )
            
            if distance < 10:
                nearby.append(other_vessel)
        except Exception as e:
            logger.error(f"Error: {e}")
    return nearby

def alert_collision_risk(vessel_a, vessel_b, cpa_distance, time_to_cpa):
    pair_key = f"{min(vessel_a['mmsi'], vessel_b['mmsi'])}_{max(vessel_a['mmsi'], vessel_b['mmsi'])}"
    
    if redis_client.exists(f"alert:{pair_key}"):
        return
    
    redis_client.setex(f"alert:{pair_key}", 3600, "1")
    
    # Slack message
    message = {
        "text": f"*COLLISION RISK DETECTED*",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "Maritime Collision Alert!"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Vessel A:*\n{vessel_a['ship_name']} (MMSI: {vessel_a['mmsi']})"},
                    {"type": "mrkdwn", "text": f"*Vessel B:*\n{vessel_b['ship_name']} (MMSI: {vessel_b['mmsi']})"},
                    {"type": "mrkdwn", "text": f"*CPA Distance:*\n{cpa_distance:.2f} NM"},
                    {"type": "mrkdwn", "text": f"*Time to CPA:*\n{time_to_cpa:.1f} minutes"}
                ]
            }
        ]
    }
    
    try:
        requests.post(SLACK_WEBHOOK_URL, json=message, timeout=5)
        logger.warning(f" ALERT: {vessel_a['ship_name']} and {vessel_b['ship_name']} - {cpa_distance:.2f} NM in {time_to_cpa:.1f} mins")
    except Exception as e:
        logger.error(f"Slack error: {e}")

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='collision-detector',
        auto_offset_reset='latest'
    )
    
    logger.info("Collision detector started")
    message_count = 0
    
    for message in consumer:
        try:
            vessel_data = message.value
            process_vessel(vessel_data)
            message_count += 1
            
            if message_count % 100 == 0:
                vessel_count = redis_client.dbsize()
                logger.info(f"Processed {message_count} updates | {vessel_count} vessels tracked")
        except Exception as e:
            logger.error(f"Error: {e}")

if __name__ == '__main__':
    main()