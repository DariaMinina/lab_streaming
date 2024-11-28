import logging
import sys
import os
from typing import Iterable

from pyflink.common import Time, Types, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.common.configuration import Configuration
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer, KafkaSource
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.datastream.window import SlidingEventTimeWindows, TimeWindow


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


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()

    print("start reading data from kafka")
    read_from_kafka(env)

