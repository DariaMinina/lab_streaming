from __future__ import unicode_literals
import logging
import json
import sys

from tornado.websocket import websocket_connect
from tornado.ioloop import IOLoop
from tornado import gen
from confluent_kafka import Producer

CONFIG_KAFKA = {
    'bootstrap.servers': 'localhost:9092',
    'auto.offset.reset': 'earliest'
}
TOPIC = 'raw_earthquakes_data_topic'
echo_uri = 'wss://www.seismicportal.eu/standing_order/websocket'
PING_INTERVAL = 1

# Проверка, что сообщение долетело до топика
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


# Модифицируем сообщение, как нам удобно
def myprocessing(message):
    try:
        data = json.loads(message)
        print(data)
        info = data['data']['properties']
        info_geometry = data['data']['geometry']
        info['action'] = data['action']
        logging.info('>>>> {action:7} event from {auth:7}, unid:{unid}, T0:{time}, Mag:{mag}, Region: {flynn_region}'.format(**info))
        logging.info('>>>> {action:7} event, source_id: {source_id}, src_cat: {source_catalog}, T0:{time}, Mag:{mag}, Region: {flynn_region}'.format(**info))
        logging.info('>>>> type: {type}, coordinates: {coordinates}'.format(**info_geometry))
        # используем эту функцию при отправке
    except Exception:
        logging.exception("Unable to parse json message")

    message = {
        'action' : info['action'],
        'auth' : info['auth'],
        'geometry_type' : info_geometry['type'],
        'coordinates' : info_geometry['coordinates'],
        'first_time' : info['time'],
        'last_update_time' : info['lastupdate'],
        'evtype' : info['evtype'],
        'mag' : info['mag'],
        'magtype' : info['magtype'],
        'lat' : info['lat'],
        'lon' : info['lon'],
        'depth' : info['depth']
    }

    producer = Producer(CONFIG_KAFKA)

    producer.produce(TOPIC, value=json.dumps(message), callback=delivery_report)
    producer.flush()

@gen.coroutine
def listen(ws):
    while True:
        msg = yield ws.read_message()
        if msg is None:
            logging.info("close")
            self.ws = None
            break
        myprocessing(msg)

@gen.coroutine
def launch_client():
    try:
        logging.info("Open WebSocket connection to %s", echo_uri)
        ws = yield websocket_connect(echo_uri, ping_interval=PING_INTERVAL)
    except Exception:
        logging.exception("connection error")
    else:
        logging.info("Waiting for messages...")
        listen(ws)

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    ioloop = IOLoop.instance()
    launch_client()
    try:
        ioloop.start()
    except KeyboardInterrupt:
        logging.info("Close WebSocket")
        ioloop.stop()
		