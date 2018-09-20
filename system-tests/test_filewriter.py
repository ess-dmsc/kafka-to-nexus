from helpers.kafkahelpers import create_producer, send_writer_command
from time import sleep
import h5py
import numpy as np
import os


def test_data_reaches_file(docker_compose):
    producer = create_producer()
    sleep(15)
    # Start file writing
    send_writer_command("commands/example-json-command.json", producer, start_time=docker_compose)
    producer.flush()
    # Give it some time to accumulate data
    sleep(10)
    # Stop file writing
    send_writer_command("commands/stop-command.json", producer)
    sleep(20)
    send_writer_command("commands/writer-exit.json", producer)
    producer.flush()

    # Allow time for the file writing to complete
    for i in range(100):
        if os.path.isfile("output-files/output_file.nxs"):
            break
        sleep(1)

    file = h5py.File("output-files/output_file.nxs", mode='r')

    # Static checks
    assert not file.swmr_mode
    assert file["entry/start_time"].value == '2016-04-12T02:58:52'
    assert file["entry/end_time"].value == '2016-04-12T03:29:11'
    assert file["entry/duration"].value == 1817.0
    assert file["entry/features"][0] == 10138143369737381149
    assert file["entry/features"][1] == 17055242037422131565
    assert file["entry/title"][...] == 'Blend 1.9_SANS'
    assert file["entry/user_1/affiliation"].value == 'ISIS, STFC'
    assert np.allclose(file["entry/instrument/monitor1/transformations/location"].attrs["vector"], np.array([0.0, 0.0, -1.0]))
    assert file["entry/instrument/monitor1/transformations/location"].attrs["transformation_type"] == "translation"

    # Streamed checks
    # Ev42 event data (Detector_1)
    assert file["entry/instrument/detector_1/event_data/event_id"][0] == 3307068
    assert file["entry/instrument/detector_1/event_data/event_id"][1] == 1422258

    # f142 Sample env (Sample)
    assert np.isclose(98.25502863210343, file["entry/sample/sample_env_logs/auxanometer/value"][0])
    assert np.isclose(75.33558352055829, file["entry/sample/sample_env_logs/auxanometer/value"][1])
