import os.path
import signal
import sys
from subprocess import Popen, PIPE
from time import sleep

import pytest
# from compose.cli.main import TopLevelCommand, project_from_options
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

common_options = {
    "--no-deps": False,
    "--always-recreate-deps": False,
    "--scale": "",
    "--abort-on-container-exit": False,
    "SERVICE": "",
    "--remove-orphans": False,
    "--no-recreate": True,
    "--force-recreate": False,
    "--no-build": False,
    "--no-color": False,
    "--rmi": "none",
    "--volumes": True,  # Remove volumes when docker-compose down (don't persist kafka and zk data)
    "--follow": False,
    "--timestamps": False,
    "--tail": "all",
    "--detach": True,
    "--build": False,
    "--no-log-prefix": False,
}

BINARY_PATH = "--file-writer-binary"
BROKERS = ["localhost:9092"]
POOL_TOPIC = "local_filewriter_pool"
POOL_STATUS_TOPIC = "local_filewriter_status"
INST_CONTROL_TOPIC = "local_filewriter"


def pytest_addoption(parser):
    parser.addoption(
        BINARY_PATH,
        action="store",
        default=None,
        help="Path to file-maker binary (executable).",
    )


def wait_until_kafka_ready(docker_cmd, docker_options):
    print("Waiting for Kafka broker to be ready for integration tests...")
    conf = {"bootstrap.servers": ",".join(BROKERS)}
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
        docker_cmd.down(docker_options)  # Bring down containers cleanly
        raise Exception("Kafka broker was not ready after 100 seconds, aborting tests.")

    client = AdminClient(conf)
    topics_ready = False

    n_polls = 0
    while n_polls < 10 and not topics_ready:
        topics = set(client.list_topics().topics.keys())
        topics_needed = ["hist_commands"]
        present = [t in topics for t in topics_needed]
        if all(present):
            topics_ready = True
            print("Topics are ready!", flush=True)
            break
        sleep(6)
        n_polls += 1

    if not topics_ready:
        docker_cmd.down(docker_options)  # Bring down containers cleanly
        raise Exception("Kafka topics were not ready after 60 seconds, aborting tests.")


def run_file_maker(args, timeout=15):
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=True,
        )
        return result.stdout
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"Process timed out: {e}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Command failed with error: {e.stderr}")
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred: {e}")


@pytest.fixture(scope="session", autouse=True)
def start_kafka(request):
    print("Starting zookeeper and kafka", flush=True)
    # options = common_options
    # options["--project-name"] = "kafka"
    # options["--file"] = ["docker-compose.yml"]
    # project = project_from_options(os.path.dirname(__file__), options)
    # cmd = TopLevelCommand(project)
    #
    # cmd.up(options)
    # print("Started kafka containers", flush=True)
    # wait_until_kafka_ready(cmd, options)
    #
    # def fin():
    #     print("Stopping zookeeper and kafka", flush=True)
    #     options["--timeout"] = 30
    #     options["--project-name"] = "kafka"
    #     options["--file"] = ["docker-compose-kafka.yml"]
    #     cmd.down(options)
    #
    # request.addfinalizer(fin)


@pytest.fixture(scope="module")
def file_writer(request):
    print("Started preparing test environment...", flush=True)
    proc = Popen(
        [
            request.config.getoption(BINARY_PATH),
            "-c",
            "config.ini",
        ]
        , stdout=PIPE, stderr=PIPE
    )

    # Give process time to start up
    sleep(10)

    print(f"File-writer is running on process id {proc.pid}")

    def fin():
        proc.kill()

    # Using a finalizer rather than yield in the fixture means
    # that the process will be brought down even if tests fail.
    request.addfinalizer(fin)
