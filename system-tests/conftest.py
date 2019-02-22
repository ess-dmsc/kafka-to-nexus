import os.path
import pytest
from compose.cli.main import TopLevelCommand, project_from_options
from confluent_kafka import Producer
import docker
from datetime import datetime
from helpers.timehelpers import unix_time_milliseconds


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


@pytest.fixture(scope="session", autouse=True)
def build_filewriter_image():
    client = docker.from_env()
    print("Building Filewriter image", flush=True)
    build_args = {}
    if "http_proxy" in os.environ:
        build_args["http_proxy"] = os.environ["http_proxy"]
    if "https_proxy" in os.environ:
        build_args["https_proxy"] = os.environ["https_proxy"]
    if "local_conan_server" in os.environ:
        build_args["local_conan_server"] = os.environ["local_conan_server"]
    image, logs = client.images.build(path="../", tag="kafka-to-nexus:latest", rm=False, buildargs=build_args)
    for item in logs:
        print(item, flush=True)


def run_containers(cmd, options):
    print("Running docker-compose up", flush=True)
    cmd.up(options)
    print("\nFinished docker-compose up\n", flush=True)
    wait_until_kafka_ready(cmd, options)


def build_and_run(options, request):
    project = project_from_options(os.path.dirname(__file__), options)
    cmd = TopLevelCommand(project)
    start_time = str(int(unix_time_milliseconds(datetime.utcnow())))
    run_containers(cmd, options)

    def fin():
        # Stop the containers then remove them and their volumes (--volumes option)
        print("containers stopping", flush=True)
        try:
            # Used for when there are multiple filewriter instances
            # as the service is not called "filewriter"
            multiple_log_options = dict(options)
            multiple_log_options["SERVICE"] = ["filewriter1", "filewriter2"]
            cmd.logs(multiple_log_options)
        except:
            log_options = dict(options)
            log_options["SERVICE"] = ["filewriter"]
            cmd.logs(log_options)
        options["--timeout"] = 30
        cmd.down(options)
        print("containers stopped", flush=True)

    # Using a finalizer rather than yield in the fixture means
    # that the containers will be brought down even if tests fail
    request.addfinalizer(fin)
    # Return the start time so the filewriter knows when to start consuming data
    # from to get all data which was published
    return start_time


@pytest.fixture(scope="session", autouse=True)
def remove_logs_from_previous_run(request):
    print("Removing previous NeXus files", flush=True)
    output_dir_name = os.path.join(os.getcwd(), "output-files")
    output_dirlist = os.listdir(output_dir_name)
    for filename in output_dirlist:
        if filename.endswith(".nxs"):
            os.remove(os.path.join(output_dir_name, filename))
    print("Removed previous NeXus files", flush=True)
    print("Removing previous log files", flush=True)
    log_dir_name = os.path.join(os.getcwd(), "logs")
    dirlist = os.listdir(log_dir_name)
    for filename in dirlist:
        if filename.endswith(".log"):
            os.remove(os.path.join(log_dir_name, filename))
    print("Removed previous log files", flush=True)


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


@pytest.fixture(scope="module")
def docker_compose_multiple_instances(request):
    """
    :type request: _pytest.python.FixtureRequest
    """
    print("Started preparing test environment...", flush=True)

    # Options must be given as long form
    options = common_options
    options["--file"] = ["docker-compose-multiple-instances.yml"]
    return build_and_run(options, request)


@pytest.fixture(scope="module")
def docker_compose_stop_command_does_not_persist(request):
    """
    :type request: _pytest.python.FixtureRequest
    """
    print("Started preparing test environment...", flush=True)
    options = common_options
    options["--file"] = ["docker-compose-stop-command.yml"]
    return build_and_run(options, request)


@pytest.fixture(scope="module")
def docker_compose_static_data(request):
    """
    :type request: _pytest.python.FixtureRequest
    """
    print("Started preparing test environment...", flush=True)
    # Options must be given as long form
    options = common_options
    options["--file"] = ["docker-compose-static-data.yml"]
    return build_and_run(options, request)
