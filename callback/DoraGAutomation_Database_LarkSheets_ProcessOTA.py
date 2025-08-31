import json
from result import Result, Ok, Err
from loguru import logger  

def callback(client, userdata, msg) -> Result[bool, str]:
    topic = msg.topic
    payload = msg.payload.decode("utf-8")
    logger.info(f"Received message id {msg.mid} on topic {topic}")
    logger.info(f"Received message id {msg.mid} payload: {payload}")
    data = json.loads(payload)  
    return Ok(True)
