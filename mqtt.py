import paho.mqtt.client as mqtt
import json


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, 'python-mqtt-1')
mqttc.connect('127.0.0.1', 1883, 60) 
mqttc.subscribe("testing")
payload = {"Name": "esp32","host ip": "192.168.1.100", "port": 1883, "topic": "paho/temperature", "qos": 0, "retain": False}
mqttc.publish("testing", json.dumps(payload))