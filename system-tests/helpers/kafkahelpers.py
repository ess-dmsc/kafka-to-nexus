from confluent_kafka import Producer, Consumer, TopicPartition
import uuid

def create_producer():
    producer_config = {'bootstrap.servers': 'localhost:9092',
                       'message.max.bytes': '20000000'}
    producer = Producer(**producer_config)
    return producer


def send_writer_command(filepath, producer, topic="TEST_writerCommand", start_time=None, stop_time=None):
    with open(filepath, "r") as cmd_file:
        data = cmd_file.read().replace('\n', '')
        if start_time is not None:
            data = data.replace('STARTTIME', start_time)
        if stop_time is not None:
            data = data.replace('STOPTIME', stop_time)
    producer.produce(topic, data)


def poll_everything(topic):
    consumer = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': uuid.uuid4()})
    topicpart = TopicPartition(topic, 0, 0)
    consumer.assign([topicpart])
    low, high = consumer.get_watermark_offsets(topicpart)

    return consumer.consume(high-1)
