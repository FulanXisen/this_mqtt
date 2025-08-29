import json
from result import Result, Ok, Err


def callback(topic, payload) -> Result[bool, str]:
    print(f"Received message on topic {topic}: {payload}")
    print(f"Received message on topic {topic}: {type(payload)}")
    data = json.loads(payload)
    raise ValueError("test")
    print(data) 
    return Ok(True)
