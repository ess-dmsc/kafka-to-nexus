import os
from subprocess import PIPE, Popen
from time import sleep

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

BINARY_PATH_OPT = "--file-writer-binary"
KAFKA_BROKER_OPT = "--kafka-broker"
BROKER = "kafka:9093"
POOL_TOPIC = "test_filewriter_pool"
POOL_STATUS_TOPIC = "test_filewriter_status"
INST_CONTROL_TOPIC_1 = "test_filewriter_inst1"
INST_CONTROL_TOPIC_2 = "test_filewriter_inst2"
MOTION_TOPIC = "test_motion"
DETECTOR_TOPIC = "test_detector"
OUTPUT_DIR = "output-files"


def get_brokers():
    return [BROKER]


def pytest_addoption(parser):
    parser.addoption(
        BINARY_PATH_OPT,
        action="store",
        default=None,
        help="Path to file-writer binary (executable).",
    )
    parser.addoption(
        KAFKA_BROKER_OPT,
        action="store",
        default=None,
        help="Set Kafka address",
    )


def wait_until_kafka_ready():
    print("Waiting for Kafka broker to be ready for integration tests...")
    conf = {"bootstrap.servers": ",".join(get_brokers())}
    producer = Producer(conf)
    kafka_ready = False

    def delivery_callback(err, msg):
        nonlocal n_polls
        nonlocal kafka_ready
        if not err:
            print("Kafka is ready!")
            kafka_ready = True

    n_polls = 0
    while n_polls < 10 and not kafka_ready:
        producer.produce(
            "waitUntilUp", value="Test message", on_delivery=delivery_callback
        )
        producer.poll(10)
        n_polls += 1

    if not kafka_ready:
        raise Exception("Kafka broker was not ready after 100 seconds, aborting tests.")

    client = AdminClient(conf)
    topics_ready = False

    n_polls = 0
    while n_polls < 10 and not topics_ready:
        topics = set(client.list_topics().topics.keys())
        topics_needed = [
            POOL_TOPIC,
            POOL_STATUS_TOPIC,
            INST_CONTROL_TOPIC_1,
            INST_CONTROL_TOPIC_2,
            DETECTOR_TOPIC,
            MOTION_TOPIC,
        ]
        present = [t in topics for t in topics_needed]
        if all(present):
            topics_ready = True
            print("Topics are ready!", flush=True)
            break
        sleep(6)
        n_polls += 1

    if not topics_ready:
        raise Exception("Kafka topics were not ready after 60 seconds, aborting tests.")


@pytest.fixture(scope="session", autouse=True)
def set_broker(request):
    global BROKER
    if request.config.getoption(KAFKA_BROKER_OPT):
        # Set custom broker
        BROKER = request.config.getoption(KAFKA_BROKER_OPT)
    print(f"\nBROKER set to {BROKER}")
    return request


@pytest.fixture(scope="module")
def file_writer(request):
    wait_until_kafka_ready()
    print("Started preparing test environment...", flush=True)
    proc = Popen(
        [
            request.config.getoption(BINARY_PATH_OPT),
            "--brokers",
            f"{BROKER}",
            "-c",
            "config.ini",
        ],
        stdout=PIPE,
        stderr=PIPE,
    )

    # Give process time to start up
    sleep(10)

    print(f"File-writer is running on process id {proc.pid}")

    def fin():
        print(f"Killing file-writer {proc.pid}")
        proc.kill()

    # Using a finalizer rather than yield in the fixture means
    # that the process will be brought down even if tests fail.
    request.addfinalizer(fin)


@pytest.fixture(scope="module")
def job_pool(request):
    wait_until_kafka_ready()
    print("Started preparing test environment...", flush=True)
    proc1 = Popen(
        [
            request.config.getoption(BINARY_PATH_OPT),
            "--brokers",
            f"{BROKER}",
            "-c",
            "config.ini",
        ],
        stdout=PIPE,
        stderr=PIPE,
    )
    proc2 = Popen(
        [
            request.config.getoption(BINARY_PATH_OPT),
            "--brokers",
            f"{BROKER}",
            "-c",
            "config.ini",
        ],
        stdout=PIPE,
        stderr=PIPE,
    )

    # Give processes time to start up
    sleep(10)

    print(f"File-writer 1 is running on process id {proc1.pid}")
    print(f"File-writer 2 is running on process id {proc2.pid}")

    def fin():
        print(f"Killing file-writers {proc1.pid} {proc2.pid}")
        proc1.kill()
        proc2.kill()

    # Using a finalizer rather than yield in the fixture means
    # that the process will be brought down even if tests fail.
    request.addfinalizer(fin)
