# adapted from 5th Year MIDS W205 week 13a course content
#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')


def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())


@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('events', default_event)
    return "This is the default response!\n"


@app.route("/purchase_a_sword")
def purchase_a_sword():
    purchase_sword_event = {'event_type': 'purchase_sword'}
    log_to_kafka('events', purchase_sword_event)
    return "Sword Purchased!\n"

@app.route("/join_guild")
def join_guild():
    join_guild_event = {'event_type': "join_guild"}
    log_to_kafka('events', join_guild_event)
    return "Joined Guild!\n"

@app.route("/slay_a_dragon")
def slay_a_dragon():
    slay_dragon_event = {'event_type': 'slay_dragon'}
    log_to_kafka('events', slay_dragon_event)
    return "Dragon Slayed!\n"

@app.route("/catch_a_butterfly")
def catch_a_butterfly():
    catch_butterfly_event = {'event_type': 'catch_butterfly'}
    log_to_kafka('events', catch_butterfly_event)
    return "Butterfly Captured!\n"