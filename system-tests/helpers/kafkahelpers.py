from confluent_kafka import Producer
from datetime import datetime


def create_producer():
    producer_config = {'bootstrap.servers': 'localhost:9092',
                       'message.max.bytes': '20000000'}
    producer = Producer(**producer_config)
    return producer


def unix_time_milliseconds(dt):
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0


def send_writer_command(filepath, producer, topic="TEST_writerCommand"):
    with open(filepath, "r") as cmd_file:
        data = cmd_file.read().replace('\n', '')
        start_time = str(int(unix_time_milliseconds(datetime.utcnow())))
        data = data.replace('STARTTIME', start_time)
    producer.produce(topic, data)
