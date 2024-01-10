import os
import os.path
import time
from subprocess import Popen
from datetime import datetime

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from file_writer_control import WorkerJobPool
from typing import Optional

from helpers.writer import stop_all_jobs
from helpers.nexushelpers import NEXUS_FILES_DIR


BINARY_PATH = "--writer-binary"
START_NO_FW = "--start-no-filewriter"
KAFKA_BROKER = "--kafka-broker"
DEFAULT_KAFKA_BROKER = "127.0.0.1:9093"
START_NR_OF_WRITERS = 2


def pytest_runtest_protocol(item, nextitem):
    print(f"\n\n{datetime.now()} - Starting test: {item.nodeid}")


def pytest_addoption(parser):
    parser.addoption(
        BINARY_PATH,
        action="store",
        default=None,
        help="Path to filewriter binary (executable).",
    )
    parser.addoption(
        KAFKA_BROKER,
        type=str,
        action="store",
        default="",
        help="Use custom Kafka broker.",
    )
    parser.addoption(
        START_NO_FW,
        type=bool,
        action="store",
        default=False,
        help="Use this flag prevent the starting of a filewriter instance. Used for debugging.",
    )


def pytest_collection_modifyitems(items, config):
    """
    If we are running against a local build then don't try to run the tests
    involving multiple instances of the file writer
    """
    if config.option.start_no_filewriter is True:
        avoid_fixture_name = "multiple_writers"
        selected_items = []
        deselected_items = []

        for item in items:
            if avoid_fixture_name in getattr(item, "fixturenames", ()):
                deselected_items.append(item)
            else:
                selected_items.append(item)
        config.hook.pytest_deselected(items=deselected_items)
        items[:] = selected_items
        print(
            "\nRunning system tests without starting (multiple) file-writers. De-selecting tests requiring multiple filewriters."
        )


def wait_until_kafka_ready(kafka_address):
    print("Waiting for Kafka broker to be ready for system tests...")
    conf = {"bootstrap.servers": kafka_address}
    producer = Producer(conf)

    kafka_ready = False

    def delivery_callback(err, msg):
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
    topic_ready = False

    n_polls = 0
    while n_polls < 10 and not topic_ready:
        all_topics = client.list_topics().topics.keys()
        if (
            "TEST_writer_jobs" in all_topics
            and "TEST_writer_commands" in all_topics
            and "TEST_writer_commands_alternative" in all_topics
        ):
            topic_ready = True
            print("Topic is ready!", flush=True)
            break
        time.sleep(6)
        n_polls += 1

    if not topic_ready:
        raise Exception("Kafka topic was not ready after 60 seconds, aborting tests.")


def run_writers(
    request,
    kafka_address,
    binary_path: Optional[str] = None,
):
    wait_until_kafka_ready(kafka_address)
    list_of_writers = []

    if binary_path is not None:
        file_writer_path = os.path.join(binary_path, "bin", "kafka-to-nexus")
        c_path = os.path.abspath(".")
        for i in range(START_NR_OF_WRITERS):
            log_file = open(f"logs/file-writer_{i}.txt", "w")
            proc = Popen(
                [
                    file_writer_path,
                    f"--config-file={c_path}/config-files/file_writer_config.ini",
                    f"--job-pool-uri={kafka_address}/TEST_writer_jobs",
                    f"--command-status-uri={kafka_address}/TEST_writer_commands",
                    f"--service-name=filewriter_{i}",
                ],
                stdout=log_file,
            )
            list_of_writers.append(proc)
        time.sleep(10)

    def fin():
        print("Stopping file-writers")
        for fw in list_of_writers:
            fw.terminate()
        for fw in list_of_writers:
            fw.wait()
        print("File-writers stopped")

    # Using a finalizer rather than yield in the fixture means
    # that the containers will be brought down even if tests fail
    request.addfinalizer(fin)


@pytest.fixture(scope="session", autouse=True)
def remove_logs_from_previous_run(request):
    print("Removing previous NeXus files", flush=True)
    output_dir_name = os.path.join(NEXUS_FILES_DIR, "output-files")
    output_dirlist = os.listdir(output_dir_name)
    for filename in output_dirlist:
        if filename.endswith(".nxs"):
            os.remove(os.path.join(output_dir_name, filename))
    print("Removing previous log files", flush=True)
    log_dir_name = os.path.join(os.getcwd(), "logs")
    dirlist = os.listdir(log_dir_name)
    for filename in dirlist:
        if filename.endswith(".log") or filename.endswith(".txt"):
            os.remove(os.path.join(log_dir_name, filename))


@pytest.fixture(scope="session", autouse=True)
def start_file_writer(request, kafka_address):
    """
    :type request: _pytest.python.FixtureRequest
    """
    if request.config.getoption(BINARY_PATH) is None and not request.config.getoption(
        START_NO_FW
    ):
        raise RuntimeError(
            f'You must set either a path to a file-writer executable ("{BINARY_PATH}") or the "{START_NO_FW}=true" flag.'
        )
    file_writer_path = None
    if not request.config.getoption(START_NO_FW):
        print("Starting the file-writer", flush=True)
        file_writer_path = request.config.getoption(BINARY_PATH)
    return run_writers(request, kafka_address, file_writer_path)


@pytest.fixture(scope="function", autouse=True)
def worker_pool(kafka_address, request):
    worker = WorkerJobPool(
        job_topic_url=f"{kafka_address}/TEST_writer_jobs",
        command_topic_url=f"{kafka_address}/TEST_writer_commands",
        max_message_size=1048576 * 500,
    )

    def stop_current_jobs():
        stop_all_jobs(worker)

    request.addfinalizer(stop_current_jobs)
    return worker


@pytest.fixture(scope="session", autouse=True)
def kafka_address(request):
    addr = request.config.getoption(KAFKA_BROKER)
    if addr == "":
        return DEFAULT_KAFKA_BROKER
    return addr


@pytest.fixture(scope="session", autouse=False)
def multiple_writers(request):
    pass
