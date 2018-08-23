from helpers.kafkahelpers import create_producer
from time import sleep
from datetime import datetime

CMDURI = "TEST_writerCommand"


def test_filewriter(docker_compose):
    assert CMDURI


def unix_time_milliseconds(dt):
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0


def send_writer_command(filepath, producer, topic="TEST_writerCommand"):
    with open(filepath, "r") as cmd_file:
        data = cmd_file.read().replace('\n', '')
        start_time = str(int(unix_time_milliseconds(datetime.utcnow())))
        data = data.replace('STARTTIME', start_time)
    producer.produce(topic, data)


def test_data_reaches_file(docker_compose):
    """
    This 'test' performs the job which NICOS will do in the production
    system at the ESS; sending the 'command' messages for the file writer.
    :param test_environment: This is the test fixture which launches the containers
    """
    producer = create_producer()
    sleep(5)

    # Start file writing
    send_writer_command("commands/example-json-command.json", producer)
    producer.flush()

    # Give it some time to accumulate data
    sleep(10)

    # Stop file writing
    send_writer_command("commands/stop-command.json", producer)
    sleep(5)
    send_writer_command("commands/writer-exit.json", producer)
    producer.flush()

    # Allow time for the file writing to complete
    sleep(5)