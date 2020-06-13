#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  7 18:23:20 2019

@author: areed145
"""

import dns
import ast
import paho.mqtt.client as mqtt
from datetime import datetime
from pymongo import MongoClient
import json
from datetime import datetime
import os
import sys


def on_connect(client, userdata, flags, rc):
    print('Connected with result code'+str(rc))
    client.subscribe('eventstream/raw')


def on_message(client, userdata, msg):
    message = msg.payload.decode('utf-8')
    message = json.loads(message)
    ins = message['event_data']['new_state']
    ins['timestamp_'] = datetime.utcnow()
    try:
        raw.insert_one(ins)
    except:
        sys.exit(1)


# MongoDB client
client = MongoClient(os.environ['MONGODB_CLIENT'])

db = client.iot
raw = db.raw

# Mosquitto client
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.connect('broker.hivemq.com', 1883, 60)
client.loop_forever()
