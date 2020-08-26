import paho.mqtt.client as mqtt
from datetime import datetime
from pymongo import MongoClient
import json
import os
import sys
import logging

logging.basicConfig(level=logging.INFO)


class Mqtt:
    """Class for writing mqtt data to mongodb"""

    def __init__(self):
        """Initialize the class"""
        client = MongoClient(os.environ["MONGODB_CLIENT"])
        db = client.iot
        self.db = db.raw
        self.client = mqtt.Client(client_id="kk6gpv-mqtt", clean_session=False)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect

    def run(self):
        """Runs the class object"""
        self.client.connect("broker.hivemq.com", 1883, 60)
        self.client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        """Connects to the eventstream"""
        print("Connected with result code" + str(rc))
        client.subscribe("eventstream/raw")

    def on_message(self, client, userdata, msg):
        """Writes a each message to mongodb"""
        message = msg.payload.decode("utf-8")
        message = json.loads(message)
        ins = message["event_data"]["new_state"]
        ins["timestamp_"] = datetime.utcnow()
        try:
            self.db.insert_one(ins)
            logging.info(ins)
        except Exception:
            pass

    def on_disconnect(self, client, userdata, rc):
        logging.error("exiting...")
        sys.exit(1)


if __name__ == "__main__":
    mq = Mqtt()
    mq.run()
