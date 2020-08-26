import paho.mqtt.client as mqtt
from datetime import datetime
from pymongo import MongoClient
import json
import os
import time
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
        self.fail_count = 0
        self.fail_max = 30
        self.fail_init()
        self.run()

    def fail_init(self):
        """Initialize check for failure"""
        self.fail_count = 0
        with open("/healthy", "w") as fp:
            fp.write("healthy")
            pass

    def fail_check(self):
        """Check for failure count"""
        self.fail_count += 1
        logging.warning(
            "couldn't connect {0} time(s)".format(str(self.fail_count))
        )
        if self.fail_count > self.fail_max - 1:
            logging.error("exiting...")
            os.remove("/healthy")
            sys.exit(1)

    def run(self):
        """Runs the class object"""
        while True:
            try:
                self.client = mqtt.Client(
                    client_id="kk6gpv-mqtt", clean_session=False,
                )
                self.client.on_connect = self.on_connect
                self.client.on_message = self.on_message
                self.client.on_disconnect = self.on_disconnect
                self.client.connect("broker.hivemq.com", 1883, 60)
                self.client.loop_forever()
                self.fail_init()
            except Exception:
                time.sleep(2)
                self.fail_check()

    def on_connect(self, client, userdata, flags, rc):
        """Connects to the eventstream"""
        logging.info("connected with result code {0}".format(str(rc)))
        client.subscribe("eventstream/raw", 2)

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
        """Connects to the eventstream"""
        logging.info("disconnected with result code {0}".format(str(rc)))
        self.run()


if __name__ == "__main__":
    mq = Mqtt()
