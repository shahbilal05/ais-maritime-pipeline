import json
import logging
import time
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
import redis
import requests
from utils import haversine_distance, calculate_cpa, is_collision_risk, calculate_severity_score

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "ais-positions"
REDIS_HOST = "localhost"
REDIS_PORT = 6379
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")  

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def process_vessel(vessel_data):
    mmsi = vessel_data['mmsi']
    redis_client.setex(f"vessel:{mmsi}", 300, json.dumps(vessel_data)) # Store key-val pair & delete in 5 mins
    
    nearby_vessels = get_nearby_vessels(vessel_data)
    
    for other_vessel in nearby_vessels:
        if other_vessel['mmsi'] == mmsi:
            continue
        
        cpa_distance, time_to_cpa = calculate_cpa(vessel_data, other_vessel)
        
        if is_collision_risk(cpa_distance, time_to_cpa, vessel_data, other_vessel):
            alert_collision_risk(vessel_data, other_vessel, cpa_distance, time_to_cpa)

# Get vessels within collision risk range using distance thresholds
def get_nearby_vessels(vessel):   
    nearby = []
    
    # Determine search radius based on vessel speed
    vessel_speed = vessel.get('sog', 0)
    
    if vessel_speed > 15:  # Fast vessel (>15 knots)
        max_search_radius = 5.0  # 5 NM
    elif vessel_speed > 5:  
        max_search_radius = 3.0  
    else:  
        max_search_radius = 2.0  # 2 NM (COLREGS minimum)
    
    for key in redis_client.scan_iter("vessel:*", count=100):
        try:
            other_vessel_json = redis_client.get(key)
            if not other_vessel_json:
                continue
            
            other_vessel = json.loads(other_vessel_json)
            
            distance = haversine_distance(
                vessel['lat'], vessel['lon'],
                other_vessel['lat'], other_vessel['lon']
            )
            
            # Only include vessels within search radius
            if distance < max_search_radius:
                nearby.append(other_vessel)
        
        except Exception as e:
            logger.error(f"Error: {e}")
    
    return nearby

def alert_collision_risk(vessel_a, vessel_b, cpa_distance, time_to_cpa):
    pair_key = f"{min(vessel_a['mmsi'], vessel_b['mmsi'])}_{max(vessel_a['mmsi'], vessel_b['mmsi'])}"
    
    if redis_client.exists(f"alert:{pair_key}"):
        return
    
    redis_client.setex(f"alert:{pair_key}", 3600, "1")

    severity = calculate_severity_score(vessel_a, vessel_b)

    # Stage 4: Immediate Danger
    if cpa_distance < 0.25 * severity and time_to_cpa < 6:
        risk_level = "IMMEDIATE DANGER"
        stage = "Stage 4"
        color_code = "#FF0000" 

    # Stage 3: Close-Quarters 
    elif cpa_distance < 0.5 * severity and time_to_cpa < 12:
        risk_level = "CLOSE QUARTERS"
        stage = "Stage 3"
        color_code = "#FFD700"  

    # Stage 2: Risk of Collision 
    elif cpa_distance < 1.0 * severity and time_to_cpa < 20:
        risk_level = "RISK OF COLLISION"
        stage = "Stage 2"
        color_code = "#0000FF"

    # Stage 1: No Risk 
    else:
        return  

    message = {
        "text": f"*{risk_level}*",
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{risk_level} - Maritime Collision Alert"}
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*COLREGS {stage}*"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Vessel A:*\n{vessel_a['ship_name']}\n (MMSI: {vessel_a['mmsi']})"},
                    {"type": "mrkdwn", "text": f"*Vessel B:*\n{vessel_b['ship_name']}\n (MMSI: {vessel_b['mmsi']})"},
                    {"type": "mrkdwn", "text": f"*CPA Distance:*\n{cpa_distance:.2f} NM"},
                    {"type": "mrkdwn", "text": f"*Time to CPA:*\n{time_to_cpa:.1f} minutes"}
                ]
            },
        ]
    }
    
    try:
        requests.post(SLACK_WEBHOOK_URL, json=message, timeout=5)
        logger.warning(f"{risk_level} ({stage}): {vessel_a['ship_name']} ({type_a}) and {vessel_b['ship_name']} ({type_b}) - {cpa_distance:.2f} NM in {time_to_cpa:.1f} mins")
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
    
    logger.info("Collision detector started...")
    message_count = 0
    
    for message in consumer:
        try:
            vessel_data = message.value
            process_vessel(vessel_data)
            message_count += 1
            
            if message_count % 100 == 0:
                vessel_count = redis_client.dbsize()
                logger.info(f"Processed {message_count} updates ; {vessel_count} vessels tracked")
        except Exception as e:
            logger.error(f"Error: {e}")

if __name__ == '__main__':
    main()