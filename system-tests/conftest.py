import os.path
import pytest
from compose.cli.main import TopLevelCommand, project_from_options
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
import docker
from datetime import datetime
from time import sleep
from subprocess import Popen
import signal
import warnings
from threading import Thread
from file_writer_control.WorkerCommandChannel import WorkerCommandChannel

LOCAL_BUILD = "--local-build"
WAIT_FOR_DEBUGGER_ATTACH = "--wait-to-attach-debugger"


def pytest_addoption(parser):
    parser.addoption(
        LOCAL_BUILD,
        action="store",
        default=None,
        help="Directory of local build to run tests against instead of rebuilding in a container",
    )
    parser.addoption(
        WAIT_FOR_DEBUGGER_ATTACH,
        type=bool,
        action="store",
        default=False,
        help=f"Use this flag when using the {LOCAL_BUILD} option to cause the system "
        f"tests to prompt you to attach a debugger to the file writer process",
    )


def pytest_collection_modifyitems(items, config):
    """
    If we are running against a local build then don't try to run the tests
    involving multiple instances of the file writer
    """
    if config.option.local_build is not None:
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
            f"\nRunning against local build. Only currently supported by a subset of the system tests."
        )


def wait_until_kafka_ready(docker_cmd, docker_options):
    print("Waiting for Kafka broker to be ready for system tests...")
    conf = {"bootstrap.servers": "localhost:9092", "api.version.request": False}
    producer = Producer(**conf)
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
    topic_ready = False

    n_polls = 0
    while n_polls < 10 and not topic_ready:
        topics = client.list_topics().topics.keys()
        if "TEST_writer_jobs" in topics and "TEST_writer_commands" in topics:
            topic_ready = True
            print("Topic is ready!", flush=True)
            break
        sleep(6)
        n_polls += 1

    if not topic_ready:
        docker_cmd.down(docker_options)  # Bring down containers cleanly
        raise Exception("Kafka topic was not ready after 60 seconds, aborting tests.")


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
}


@pytest.fixture(scope="session", autouse=True)
def init_images(request):
    fw_image_thread = Thread(target=build_filewriter_image, args=(request,))
    fw_image_thread.start()
    kafka_start_thread = Thread(target=start_kafka, args=(request,))
    kafka_start_thread.start()
    fw_image_thread.join()
    kafka_start_thread.join()


def build_filewriter_image(request):
    print("Build filewriter image")
    # Only build image if we are not running against a local build
    if request.config.getoption(LOCAL_BUILD) is None:
        client = docker.from_env()
        print("Building filewriter image", flush=True)
        from create_filewriter_image import create_filewriter_image

        create_filewriter_image()


def run_containers(cmd, options):
    print("Running docker-compose up", flush=True)
    cmd.up(options)
    print("\nFinished docker-compose up\n", flush=True)
    wait_until_kafka_ready(cmd, options)


def build_and_run(options, request, local_path=None, wait_for_debugger=False):
    if wait_for_debugger and local_path is None:
        warnings.warn(
            "Option specified to wait for debugger to attach, but this "
            "can only be used if a local build path is provided"
        )

    project = project_from_options(os.path.dirname(__file__), options)
    cmd = TopLevelCommand(project)
    run_containers(cmd, options)

    if local_path is not None:
        # Launch local build of file writer
        full_path_of_file_writer_exe = os.path.join(local_path, "bin", "kafka-to-nexus")
        log_file = open("logs/file-writer-logs.txt", "w")
        proc = Popen(
            [
                full_path_of_file_writer_exe,
                "-c",
                "./config-files/local_file_writer_config.ini",
            ],
            stdout=log_file,
        )
        if wait_for_debugger:
            proc.send_signal(
                signal.SIGSTOP
            )  # Pause the file writer until we've had chance to attach a debugger
            input(
                f"\n"
                f"Attach a debugger to process id {proc.pid} now if you wish, then press enter to continue: "
            )
            print(
                "You'll need to tell the debugger to continue after it has attached, "
                'for example type "continue" if using gdb.'
            )
            proc.send_signal(signal.SIGCONT)

    def fin():
        # Stop the containers then remove them and their volumes (--volumes option)
        print("containers stopping", flush=True)
        if local_path is None:
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
        else:
            proc.kill()
        options["--timeout"] = 30
        cmd.down(options)
        print("containers stopped", flush=True)

    # Using a finalizer rather than yield in the fixture means
    # that the containers will be brought down even if tests fail
    request.addfinalizer(fin)


def start_kafka(request):
    print("Starting zookeeper and kafka", flush=True)
    options = common_options
    options["--project-name"] = "kafka"
    options["--file"] = ["docker-compose-kafka.yml"]
    project = project_from_options(os.path.dirname(__file__), options)
    cmd = TopLevelCommand(project)

    cmd.up(options)
    print("Started kafka containers", flush=True)
    wait_until_kafka_ready(cmd, options)

    def fin():
        print("Stopping zookeeper and kafka", flush=True)
        options["--timeout"] = 30
        options["--project-name"] = "kafka"
        options["--file"] = ["docker-compose-kafka.yml"]
        cmd.down(options)

    request.addfinalizer(fin)


@pytest.fixture(scope="session", autouse=False)
def start_lr_images(request):
    print("Starting ioc and forwarder", flush=True)
    options = common_options
    options["--project-name"] = "forwarder"
    options["--file"] = ["docker-compose-lr.yml"]
    project = project_from_options(os.path.dirname(__file__), options)
    cmd = TopLevelCommand(project)

    cmd.up(options)
    print("Started ioc and forwarder containers", flush=True)

    def fin():
        print("Stopping ioc and forwarder", flush=True)
        options["--timeout"] = 30
        options["--project-name"] = "forwarder"
        options["--file"] = ["docker-compose-lr.yml"]
        cmd.down(options)

    request.addfinalizer(fin)


@pytest.fixture(scope="session", autouse=True)
def remove_logs_from_previous_run(request):
    print("Removing previous NeXus files", flush=True)
    output_dir_name = os.path.join(os.getcwd(), "output-files")
    output_dirlist = os.listdir(output_dir_name)
    for filename in output_dirlist:
        if filename.endswith(".nxs"):
            os.remove(os.path.join(output_dir_name, filename))
    print("Removing previous log files", flush=True)
    log_dir_name = os.path.join(os.getcwd(), "logs")
    dirlist = os.listdir(log_dir_name)
    for filename in dirlist:
        if filename.endswith(".log"):
            os.remove(os.path.join(log_dir_name, filename))


@pytest.fixture(scope="session", autouse=True)
def start_file_writer(request):
    """
    :type request: _pytest.python.FixtureRequest
    """
    print("Starting the file-writer", flush=True)

    # Options must be given as long form
    options = common_options
    if request.config.getoption(LOCAL_BUILD) is None:
        options["--file"] = ["docker-compose-writer.yml"]
    return build_and_run(
        options,
        request,
        request.config.getoption(LOCAL_BUILD),
        request.config.getoption(WAIT_FOR_DEBUGGER_ATTACH),
    )


@pytest.fixture(scope="session", autouse=True)
def writer_channel(request):
    """
    :type request: _pytest.python.FixtureRequest
    """
    kafka_host = "localhost:9092"
    return WorkerCommandChannel("{}/TEST_writer_commands".format(kafka_host))


@pytest.fixture(scope="session", autouse=False)
def multiple_writers(request):
    pass
