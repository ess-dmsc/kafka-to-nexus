import pytest
import docker
from helpers.nexushelpers import OpenNexusFile
from math import isclose
from file_writer_control.WriteJob import WriteJob
from helpers.writer import (
    wait_start_job,
    wait_no_working_writers,
    wait_set_stop_now,
)
from datetime import datetime, timedelta
from time import sleep


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


@pytest.mark.skip(reason="Long running test disabled by default")
def test_long_run(writer_channel, kafka_address, start_lr_images):
    file_name = "output_file_lr.nxs"
    with open("commands/nexus_structure_long_running.json", "r") as f:
        structure = f.read()
    start_time = datetime.now()
    write_job = WriteJob(
        nexus_structure=structure,
        file_name=file_name,
        broker=kafka_address,
        start_time=start_time,
        stop_time=start_time + timedelta(days=365),
    )
    job_handler = wait_start_job(writer_channel, write_job, timeout=20)

    pv_updates = 6000
    # range is exclusive of the last number, so in order to get 1 to pv_updates we need to use pv_updates+1
    for i in range(1, pv_updates + 1):
        change_pv_value("SIMPLE:DOUBLE", i)
        sleep(3)

    wait_set_stop_now(job_handler, timeout=20)
    wait_no_working_writers(writer_channel, timeout=30)

    file_path = f"output-files/{file_name}"
    with OpenNexusFile(file_path) as file:
        counter = 1
        # check values are contiguous
        for value in file["entry/cont_data/value"]:
            assert isclose(value, counter)
            counter += 1

    # check that the last value is the same as the number of updates
    assert counter == pv_updates + 1
