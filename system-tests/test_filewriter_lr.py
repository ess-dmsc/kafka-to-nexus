import pytest
import docker
from time import sleep
from helpers.kafkahelpers import (
    create_producer,
    consume_everything,
)
from helpers.nexushelpers import OpenNexusFile
from math import isclose
from streaming_data_types.status_x5f2 import deserialise_x5f2


def change_pv_value(pvname, value):
    """
    Epics call to change PV value.
    :param pvname:(string) PV name
    :param value: PV value to change to
    :return: none
    """
    container = False
    client = docker.from_env()
    for item in client.containers.list():
        if "_ioc_1" in item.name:
            container = item
            break
    if not container:
        raise Exception("IOC Container not found")
    exit_code, output = container.exec_run(
        "caput {} {}".format(pvname, value), privileged=True
    )
    assert exit_code == 0
    print("Updating PV value using caput: ")
    print(output.decode("utf-8"), flush=True)


# @pytest.mark.skip(reason="Long running test disabled by default")
# def test_long_run(docker_compose_long_running):
#     producer = create_producer()
#     sleep(20)
#     # Start file writing
#     job_id = publish_run_start_message(
#         producer,
#         "commands/nexus_structure_long_running.json",
#         nexus_filename="output_file_lr.nxs",
#         topic="TEST_writerCommandLR",
#         start_time=int(docker_compose_long_running),
#     )
#     sleep(10)
#     # Minimum length of the test is determined by (pv_updates * 3) + 10 seconds
#     pv_updates = 6000
#     # range is exclusive of the last number, so in order to get 1 to pv_updates we need to use pv_updates+1
#     for i in range(1, pv_updates + 1):
#         change_pv_value("SIMPLE:DOUBLE", i)
#         sleep(3)
#
#     publish_run_stop_message(producer, job_id=job_id, topic="TEST_writerCommandLR")
#     sleep(30)
#
#     filepath = "output-files/output_file_lr.nxs"
#     with OpenNexusFileWhenAvailable(filepath) as file:
#         counter = 1
#         # check values are contiguous
#         for value in file["entry/cont_data/value"]:
#             assert isclose(value, counter)
#             counter += 1
#
#     # check that the last value is the same as the number of updates
#     assert counter == pv_updates + 1
#
#     with open("logs/lr_status_messages.log", "w+") as file:
#         status_messages = consume_everything("TEST_writerStatus")
#         for msg in status_messages:
#             file.write(str(deserialise_x5f2(msg.value())) + "\n")
