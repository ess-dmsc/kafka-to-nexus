from helpers.kafkahelpers import create_producer, send_writer_command
from time import sleep
from docker import DockerClient


def test_ignores_commands_with_incorrect_id(docker_compose_multiple_instances):
    producer = create_producer()
    sleep(10)
    # Command only filewriter2 to exit
    send_writer_command("commands/writer-exit-single.json", producer, "TEST_writerCommandMultiple")
    # Wait for filewriter2 to exit
    client = DockerClient()
    stopped = False
    for i in range(30):
        containers = client.containers.list(True)
        for container in containers:
            if "filewriter1" in container.name and "filewriter2" not in container.name:
                stopped = True
                break
        sleep(1)
    assert stopped
