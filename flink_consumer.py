import logging
import sys
import os
from typing import Iterable

import pyflink.datastream as flink
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction, WindowFunction
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.configuration import Configuration
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer, KafkaSource, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow
# from pyflink.datastream.windowing import SlidingEventTimeWindows, WindowFunction


def calculate_average(window):
    coordinates = window.get_state()
    if len(coordinates) > 0:
        avg_x = sum(coord[0] for coord in coordinates) / len(coordinates)
        avg_y = sum(coord[1] for coord in coordinates) / len(coordinates)
        return f"Average: ({avg_x}, {avg_y})"
    else:
        return "No coordinates in the window"


class CountWindowProcessFunction(ProcessWindowFunction[tuple, tuple, str, TimeWindow]):
    def process(self,
                key: str,
                context: ProcessWindowFunction.Context[TimeWindow],
                elements: Iterable[tuple]) -> Iterable[tuple]:
        return [(key, context.window().start, context.window().end, len([e for e in elements]))]


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value[1])


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
        
        out.collect(f"Window {window_id}: Avg X={avg_x}, Avg Y={avg_y}")



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

    data_stream = env.add_source(kafka_consumer)


    # Настройка слайдинга окна в 15 минут с шагом 5 минут
    #window_assigner = SlidingEventTimeWindows.of(Time.minutes(2), Time.seconds(5))

    # define the watermark strategy
    # watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
    #     .with_timestamp_assigner(MyTimestampAssigner())

    # Группировка по ключу и агрегация
    # result_stream = data_stream.key_by(lambda x: x[0], key_type=Types.STRING()).window(window_assigner).process(CountWindowProcessFunction(),
    #             Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.INT()]))#.process(calculate_average)

    result_stream = data_stream.key_by(lambda x: x[0], key_type=Types.STRING()) \
        .window(SlidingEventTimeWindows.of(Time.milliseconds(5), Time.milliseconds(2))) \
        .process(CountWindowProcessFunction(),
                Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.INT()]))

    result_stream.print()

    env.execute("run")


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
    #.assign_timestamps_and_watermarks(
    #     TimestampAssigner.assign_to_current_time()
    # )

    # Группируем данные по id и создаем окно
    windowed_stream = kafka_source.key_by(lambda x: x['id']).window(
        SlidingEventTimeWindows.of(Time.minutes(240), Time.seconds(5))
    )

    # Применяем функцию вычисления средних координат
    windowed_stream.process(AverageCoordinatesWindowFunction())

    # Запускаем выполнение
    env.execute("Average Coordinates Window")



if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()

    print("start reading data from kafka")
    # read_from_kafka(env)
    read_kafka(env)

