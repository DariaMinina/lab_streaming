from confluent_kafka import Producer

config = {
    'bootstrap.servers': 'localhost:9092',
    'auto.offset.reset': 'earliest'
}
topic = 'raw_earthquakes_data_topic'
producer = Producer(config)

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

producer.produce(topic, value='Hello world!')
producer.flush()


# используем эту функцию при отправке
producer.produce('raw_earthquakes_data_topic', 'Hello again!', callback=delivery_report)
producer.flush()