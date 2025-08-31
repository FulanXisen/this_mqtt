from venv import logger
from result import Ok,Err, Result
from loguru import logger

def callback(client, userdata, msg) -> Result[bool, str]:
    logger.info("true callback")
    logger.info(f"Received message: {msg.payload.decode()} on topic {msg.topic}")
    return Ok(True)
