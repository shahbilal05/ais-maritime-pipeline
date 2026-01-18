from kafka import KafkaProducer
import asyncio
import websockets
import json
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# configure logging (time - severity - message)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY") 
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "ais-positions"
BOUNDING_BOXES = [[[25, -100], [50, -60]]] # required geographical boundaries

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    compression_type='gzip'
)

async def stream_ais():
    url = "wss://stream.aisstream.io/v0/stream"
    
    while True:
        try:
            async with websockets.connect(url) as websocket:
                logger.info("Connected to AISstream.io")
                
                await websocket.send(json.dumps({
                    "APIKey": AISSTREAM_API_KEY,
                    "BoundingBoxes": BOUNDING_BOXES
                }))
                
                logger.info("Awaiting vessel data...")
                message_count = 0
                
                async for message_json in websocket:
                    try:
                        message = json.loads(message_json)
                        
                        # only process messages with vessel positions
                        if message.get("MessageType") == "PositionReport":
                            pos = message["Message"]["PositionReport"]
                            meta = message.get("MetaData", {})
                            
                            # Kafka message payload info
                            record = {
                                "mmsi": pos["UserID"],
                                "timestamp": pos["Timestamp"],
                                "lat": pos["Latitude"],
                                "lon": pos["Longitude"],
                                "sog": pos.get("Sog", 0), # speed over ground in knots; actual speed relative to Earth
                                "cog": pos.get("Cog", 0), # course over ground in degrees from true north; actual travel direction
                                "heading": pos.get("TrueHeading", 0), # direction the vessel is pointing
                                "nav_status": pos.get("NavigationalStatus", 15), # vessel's current state (0=under way, 15=default/Unknown)
                                "ship_type": meta.get("ShipType", 0),
                                "ship_name": meta.get("ShipName", "UNKNOWN")
                            }
                            
                            producer.send(KAFKA_TOPIC, value=record)
                            message_count += 1
                            
                            if message_count % 100 == 0:
                                logger.info(f"Processed {message_count} messages...")
                    except Exception as e:
                        logger.error(f"Error: {e}")
        except Exception as e:
            logger.error(f"Websocket error: {e}. Reconnecting in 10s...")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(stream_ais())