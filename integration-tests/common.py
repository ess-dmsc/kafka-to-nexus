import json
import uuid

from confluent_kafka import OFFSET_END, Consumer, TopicPartition
from streaming_data_types import deserialise_x5f2
from streaming_data_types.eventdata_ev44 import serialise_ev44
from streaming_data_types.logdata_f144 import serialise_f144
from streaming_data_types.run_start_pl72 import serialise_pl72
from streaming_data_types.run_stop_6s4t import serialise_6s4t

from conftest import (
    DETECTOR_TOPIC,
    INST_CONTROL_TOPIC_1,
    MOTION_TOPIC,
    POOL_TOPIC,
    get_brokers,
)

NEXUS_STRUCTURE = {
    "children": [
        {
            "type": "group",
            "name": "entry",
            "children": [
                {"module": "mdat", "config": {"items": ["start_time", "end_time"]}},
                {
                    "module": "dataset",
                    "config": {
                        "name": "title",
                        "values": "This is my title",
                        "type": "string",
                    },
                },
                {
                    "name": "detector",
                    "type": "group",
                    "attributes": [
                        {"name": "NX_class", "dtype": "string", "values": "NXdata"}
                    ],
                    "children": [
                        {
                            "module": "ev44",
                            "config": {"topic": DETECTOR_TOPIC, "source": "grace"},
                        }
                    ],
                },
                {
                    "name": "motion",
                    "type": "group",
                    "attributes": [
                        {"name": "NX_class", "dtype": "string", "values": "NXlog"}
                    ],
                    "children": [
                        {
                            "module": "f144",
                            "config": {
                                "topic": MOTION_TOPIC,
                                "source": "axis",
                                "dtype": "double",
                            },
                        }
                    ],
                },
            ],
        }
    ]
}


def generate_ev44(time_start, tofs, det_ids):
    return serialise_ev44("grace", time_start, [time_start], [0], tofs, det_ids)


def generate_f144(value, time_stamp):
    return serialise_f144("axis", value, time_stamp)


def extract_state_from_status_message(buffer):
    return json.loads(deserialise_x5f2(buffer).status_json)["state"]


def start_filewriter(
    producer,
    file_name,
    job_id,
    start_time_s,
    stop_time_s=None,
    metadata="",
    inst_control_topic=INST_CONTROL_TOPIC_1,
):
    print(f"\nRequesting start writing for file: {file_name}")
    buffer = serialise_pl72(
        job_id=job_id,
        filename=file_name,
        start_time=start_time_s * 1000,
        stop_time=stop_time_s * 1000 if stop_time_s else None,
        run_name="test_run",
        nexus_structure=json.dumps(NEXUS_STRUCTURE),
        service_id="",
        instrument_name="",
        broker="localhost:9092",
        control_topic=inst_control_topic,
        metadata=metadata,
    )
    producer.produce(POOL_TOPIC, buffer)
    producer.flush()


def stop_filewriter(producer, job_id, inst_control_topic=INST_CONTROL_TOPIC_1):
    buffer = serialise_6s4t(
        job_id=job_id,
        run_name="test_run",
        service_id="",
        stop_time=None,
        command_id=str(uuid.uuid4()),
    )
    producer.produce(inst_control_topic, buffer)
    producer.flush()


def create_consumer(topic):
    consumer_conf = {
        "bootstrap.servers": ",".join(get_brokers()),
        "group.id": uuid.uuid4(),
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(consumer_conf)
    topic_partitions = []

    metadata = consumer.list_topics(topic)
    partition_numbers = [p.id for p in metadata.topics[topic].partitions.values()]

    for pn in partition_numbers:
        partition = TopicPartition(topic, pn)
        # Make sure consumer is at end of the partition(s)
        partition.offset = OFFSET_END
        topic_partitions.append(partition)

    consumer.assign(topic_partitions)
    return consumer, topic_partitions
