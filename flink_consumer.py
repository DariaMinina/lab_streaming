import logging
import sys
import os
from dotenv import load_dotenv

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer, KafkaSource
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema


# Загружаем переменные окружения из .env файла
load_dotenv()


def read_from_kafka(env):
    lst_fields = ['action', 'id', 'auth', 'geometry_type', 'coordinates', 'first_time',
        'last_update_time', 'evtype', 'mag', 'magtype', 'lat', 'lon', 'depth']
    lst_types = [Types.STRING()] * len(lst_fields)

    #Создание объекта схемы данных для десериализации JSON-строк
    row_type_info = Types.ROW_NAMED(lst_fields, lst_types)
    json_schema = JsonRowDeserializationSchema.builder().type_info(row_type_info).build()

    kafka_consumer = FlinkKafkaConsumer(
        topics='raw_earthquakes_data_topic',
        deserialization_schema=json_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id':'test-consumer-group'}
    )
    kafka_consumer.set_start_from_earliest()

    data_stream = env.add_source(kafka_consumer).print()

    env.execute("run")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()

    # прописываем пути для нужных jar файлов
    env.add_jars(f"file://{os.getenv('FLINK_SQL_CONNECTOR_KAFKA_JAR')}")
    env.add_jars(f"file://{os.getenv('FLINK_CONNECTOR_KAFKA_JAR')}")
    env.add_jars(f"file://{os.getenv('KAFKA_CLIENTS_JAR')}")
    print("start reading data from kafka")
    read_from_kafka(env)