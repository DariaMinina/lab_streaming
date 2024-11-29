import logging
import sys
import os
from typing import Iterable


from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction, WindowFunction
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.configuration import Configuration
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer, KafkaSource, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.state import ValueStateDescriptor


# Определяем функцию для вычисления среднего значения координат
class AverageCoordinatesWindowFunction(WindowFunction):
    def __init__(self):
        self.avg_x = None
        self.avg_y = None
        
    def open(self, runtime_context: RuntimeContext):
        pass
    
    def close(self):
        pass

    def apply(self, window_id: str, elements: Iterable[tuple]):
        self.process(window_id, elements, lambda x: None)
    
    def process(self, window_id: str, elements: Iterable[tuple], out: Iterable[tuple]):
        total_x = sum(float(element['coordinates'][0]) for element in elements)
        total_y = sum(float(element['coordinates'][1]) for element in elements)
        
        count = len(elements)
        
        avg_x = total_x / count if count > 0 else None
        avg_y = total_y / count if count > 0 else None
        
        self.avg_x = avg_x
        self.avg_y = avg_y

        # Создаем словарь с результатами
        result = {
            "window_id": window_id,
            "avg_x": avg_x,
            "avg_y": avg_y
        }
        
        # Преобразуем словарь в JSON-строку
        json_result = json.dumps(result)
        
        out.collect(json_result)


def read_kafka(env):
    lst_fields = ['action', 'id', 'auth', 'geometry_type', 'coordinates', 'first_time',
        'last_update_time', 'evtype', 'mag', 'magtype', 'lat', 'lon', 'depth']
    lst_types = [Types.STRING()] * len(lst_fields)

    #Создание объекта схемы данных для десериализации JSON-строк
    row_type_info = Types.ROW_NAMED(lst_fields, lst_types)
    json_schema = JsonRowDeserializationSchema.builder().type_info(row_type_info).build()


    kafka_source = env.add_source(
    FlinkKafkaConsumer(
        topics='raw_earthquakes_data_topic',
        deserialization_schema=json_schema,
        properties={
            "bootstrap.servers": "localhost:9092",
            "group.id": "test-consumer-group"
        }
    ))

    # Группируем данные по id и создаем окно
    windowed_stream = kafka_source.key_by(lambda x: x['id']).window(
        SlidingEventTimeWindows.of(Time.minutes(240), Time.seconds(5))
    )

    # Применяем функцию вычисления средних координат
    windowed_stream = windowed_stream.process(AverageCoordinatesWindowFunction())

    # Применяем селектор результатов
    # Создаем схему сериализации JSON
    lst_fields_out = ['window_id', 'avg_x', 'avg_y']
    lst_types_out = [Types.STRING(), Types.DOUBLE(), Types.DOUBLE()]
    json_schema = JsonRowSerializationSchema.builder() \
        .with_type_info(Types.ROW_NAMED(
            lst_fields_out, lst_types_out
        )) \
        .build()


    # Add a Kafka producer to output results
    producer = FlinkKafkaProducer(
        topic='result',
        serialization_schema=json_schema,
        producer_config={
            "bootstrap.servers": "localhost:9092"
        }
    )

    # добавляем kafka producer
    windowed_stream.add_sink(producer)

    # Запускаем выполнение
    env.execute("Average Coordinates Window")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()

    print("start reading data from kafka")
    read_kafka(env)

