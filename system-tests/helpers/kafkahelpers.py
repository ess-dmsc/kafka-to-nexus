from confluent_kafka import Consumer, Producer
import uuid


def poll_for_valid_message(consumer):
    """
    Polls the subscribed topics by the consumer and checks the buffer is not empty or malformed.
    :param consumer: The consumer object.
    :return: The message object received from polling.
    """
    msg = consumer.poll()
    assert not msg.error()
    return msg


def create_consumer(offset_reset='smallest'):
    consumer_config = {'bootstrap.servers': 'localhost:9092',
                       'default.topic.config': {'auto.offset.reset': offset_reset},
                       'group.id': uuid.uuid4()}
    consumer = Consumer(**consumer_config)
    return consumer


def create_producer():
    producer_config = {'bootstrap.servers': 'localhost:9092',
                       'message.max.bytes': '20000000'}
    producer = Producer(**producer_config)
    return producer
