import os.path
import pytest
from compose.cli.main import TopLevelCommand, project_from_options
from confluent_kafka import Producer
import docker
from datetime import datetime

def unix_time_milliseconds(dt):
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0

def wait_until_kafka_ready(docker_cmd, docker_options):
    print('Waiting for Kafka broker to be ready for system tests...')
    conf = {'bootstrap.servers': 'localhost:9092',
            'api.version.request': True}
    producer = Producer(**conf)
    kafka_ready = False

    def delivery_callback(err, msg):
        nonlocal n_polls
        nonlocal kafka_ready
        if not err:
            print('Kafka is ready!')
            kafka_ready = True

    n_polls = 0
    while n_polls < 10 and not kafka_ready:
        producer.produce('waitUntilUp', value='Test message', on_delivery=delivery_callback)
        producer.poll(10)
        n_polls += 1

    if not kafka_ready:
        docker_cmd.down(docker_options)  # Bring down containers cleanly
        raise Exception('Kafka broker was not ready after 100 seconds, aborting tests.')


common_options = {"--no-deps": False,
                  "--always-recreate-deps": False,
                  "--scale": "",
                  "--abort-on-container-exit": False,
                  "SERVICE": "",
                  "--remove-orphans": False,
                  "--no-recreate": True,
                  "--force-recreate": False,
                  '--no-build': False,
                  '--no-color': False,
                  "--rmi": "none",
                  "--volumes": True,  # Remove volumes when docker-compose down (don't persist kafka and zk data)
                  "--follow": False,
                  "--timestamps": False,
                  "--tail": "all",
                  "--detach": True,
                  "--build": False
                  }


def build_filewriter_image():
    client = docker.from_env()
    print("Building Filewriter image", flush=True)
    build_args = {}
    if "http_proxy" in os.environ:
        build_args["http_proxy"] = os.environ["http_proxy"]
    if "https_proxy" in os.environ:
        build_args["https_proxy"] = os.environ["https_proxy"]
    image, logs = client.images.build(path="../", tag="kafka-to-nexus:latest", rm=False, buildargs=build_args)
    for item in logs:
        print(item, flush=True)


def run_containers(cmd, options):
    print("Running docker-compose up", flush=True)
    cmd.up(options)
    print("\nFinished docker-compose up\n", flush=True)
    wait_until_kafka_ready(cmd, options)


def build_and_run(options, request):
    build_filewriter_image()
    project = project_from_options(os.path.dirname(__file__), options)
    cmd = TopLevelCommand(project)
    start_time = str(int(unix_time_milliseconds(datetime.utcnow())))
    run_containers(cmd, options)

    def fin():
        cmd.logs(options)
        # Stop the containers then remove them and their volumes (--volumes option)
        print("containers stopping", flush=True)
        options["--timeout"] = 30
        cmd.down(options)
        print("containers stopped", flush=True)
        print("Removing file", flush=True)
        os.remove(os.path.join(os.path.join(os.getcwd(), "output-files"), "output_file.nxs"))
        print("Removed file", flush=True)

    # Using a finalizer rather than yield in the fixture means
    # that the containers will be brought down even if tests fail
    request.addfinalizer(fin)
    # Return the start time so the filewriter knows when to start consuming data
    # from to get all data which was published
    return start_time


@pytest.fixture(scope="module")
def docker_compose(request):
    """
    :type request: _pytest.python.FixtureRequest
    """
    print("Started preparing test environment...", flush=True)

    # Options must be given as long form
    options = common_options
    options["--file"] = ["docker-compose.yml"]
    return build_and_run(options, request)
