import datetime
import json

import connexion
from connexion import NoContent
import swagger_ui_bundle

import mysql.connector 
import pymysql
import yaml
import logging
import logging.config

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from buy import Buy
from sell import Sell

import pykafka
from pykafka import KafkaClient
from pykafka.common import OffsetType

import threading
from threading import Thread

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

DB_ENGINE = create_engine(f"mysql+pymysql://{app_config['user']}:{app_config['password']}@{app_config['hostname']}:{app_config['port']}/{app_config['db']}")
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

def process_messages():
    # TODO: create KafkaClient object assigning hostname and port from app_config to named parameter "hosts"
    # and store it in a variable named 'client'
    client = KafkaClient(hosts=f"{app_config['events']['hostname']}:{app_config['events']['port']}")

    # TODO: index into the client.topics array using topic from app_config
    # and store it in a variable named topic
    topic = client.topics[app_config['events']['topic']]

    # Notes:
    #
    # An 'offset' in Kafka is a number indicating the last record a consumer has read,
    # so that it does not re-read events in the topic
    #
    # When creating a consumer object,
    # reset_offset_on_start = False ensures that for any *existing* topics we will read the latest events
    # auto_offset_reset = OffsetType.LATEST ensures that for any *new* topic we will also only read the latest events
    
    messages = topic.get_simple_consumer( 
        reset_offset_on_start = False, 
        auto_offset_reset = OffsetType.LATEST
    )

    print(messages)

    for msg in messages:
        # This blocks, waiting for any new events to arrive
        # TODO: decode (utf-8) the value property of the message, store in a variable named msg_str
        msg_str = msg.value.decode('utf-8')
        
        # TODO: convert the json string (msg_str) to an object, store in a variable named msg
        msg = json.loads(msg_str)

        # TODO: extract the payload property from the msg object, store in a variable named payload
        payload = msg['payload']

        # TODO: extract the type property from the msg object, store in a variable named msg_type
        msg_type = msg['type']

        # TODO: create a database session
        session = DB_SESSION()

        # TODO: log "CONSUMER::storing buy event"
        logger.info("CONSUMER::storing {} event".format(msg_type))
        # TODO: log the msg object
        logger.debug(msg)

        # TODO: if msg_type equals 'buy', create a Buy object and pass the properties in payload to the constructor
        # if msg_type equals sell, create a Sell object and pass the properties in payload to the constructor
        if msg_type == 'buy':
            buy = Buy(payload['buy_id'], payload['item_name'], payload['item_price'], payload['buy_qty'], payload['trace_id'])
            session.add(buy)
        elif msg_type == 'sell':
            sell = Sell(payload['sell_id'], payload['item_name'], payload['item_price'], payload['sell_qty'], payload['trace_id'])
            session.add(sell)

        # TODO: session.add the object you created in the previous step
        # TODO: commit the session
        session.commit()
        session.close()

    # TODO: call messages.commit_offsets() to store the new read position
    messages.commit_offsets()

# Endpoints
def buy(body):
    # TODO: copy over code from previous version of storage
    # TODO create a session
    session = DB_SESSION()

    # TODO additionally pass trace_id (along with properties from Lab 2) into Buy constructor
    buy = Buy(body["buy_id"], body["item_name"], body["item_price"], body["buy_qty"], body["trace_id"])

    # TODO add, commit, and close the session
    session.add(buy)
    session.commit()
    session.close()    
    # TODO: call logger.debug and pass in message "Stored buy event with trace id <trace_id>"
    logger.debug(f"Stored buy event with trace id {body['trace_id']}")


    # TODO return NoContent, 201
    return NoContent, 201
# end

def get_buys(timestamp):
    # TODO: copy over code from previous version of storage
    # placeholder for future labs
    session = DB_SESSION()
    rows = session.query(Buy).filter(Buy.date_created >= timestamp)
    data = []
    for row in rows:
        row = row.to_dict()
        data.append(row)
    return data, 200

def sell(body):
    # TODO: copy over code from previous version of storage
    # TODO create a session
    session = DB_SESSION()

    # TODO additionally pass trace_id (along with properties from Lab 2) into Sell constructor
    sell = Sell(body["sell_id"], body["item_name"], body["item_price"], body["sell_qty"], body["trace_id"])

    # TODO add, commit, and close the session
    session.add(sell)
    session.commit()
    session.close()

    # TODO: call logger.debug and pass in message "Stored buy event with trace id <trace_id>"
    logger.debug(f"Stored sell event with trace id {body['trace_id']}")

    return NoContent, 201

# end

def get_sells(timestamp):
    # TODO: copy over code from previous version of storage
    # TODO: create a session
    session = DB_SESSION()

    data = []
    rows = session.query(Sell).filter(Sell.date_created >= timestamp)

    #loop through rows, for each row call .to_dict()
    for row in rows:
        row = row.to_dict()
        data.append(row)
    
    print(data)
    return data, 200


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api('openapi.yaml', strict_validation=True, validate_responses=True)

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basic')

if __name__ == "__main__":
    tl = Thread(target=process_messages)
    tl.daemon = True
    tl.start()
    app.add_api("openapi.yml", base_path="/storage", strict_validation=True, validate_responses=True)
    app.run(port=8090)
