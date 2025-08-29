import paho.mqtt.client as mqtt

broker = '127.0.0.1'
port = 1883
topic = "python/mqtt"
client_id = f'python-mqtt-1'

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("paho/temperature")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    
    print(msg.topic+" "+str(msg.payload))

mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id)
mqttc.on_connect = on_connect
# mqttc.on_message = on_message

mqttc.connect(broker, 1883, 60) 

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
# mqttc.loop_forever()
# mqttc.loop_start()
import json
payload = {"Name": "esp32","host ip": "192.168.1.100", "port": 1883, "topic": "paho/temperature", "qos": 0, "retain": False}

mqttc.publish("DoraGAutomation/DataWriteBack/OTA", json.dumps(payload))
# mqttc.loop_stop()