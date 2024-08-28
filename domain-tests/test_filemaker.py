import h5py
import numpy as np


def test_f144_writes(write_file):
    with h5py.File(write_file, "r") as f:
        assert np.array_equal(
            f["/entry/instrument/chopper/rotation_speed/value"][:], [10, 15]
        )
        assert np.array_equal(
            f["/entry/instrument/chopper/rotation_speed/time"][:],
            [100000000, 110000000],
        )


def test_ep01_writes(write_file):
    with h5py.File(write_file, "r") as f:
        assert np.array_equal(
            f["/entry/instrument/chopper/rotation_speed/connection_status"][:], [2, 2]
        )
        assert np.array_equal(
            f["/entry/instrument/chopper/rotation_speed/connection_status_time"][:],
            [101, 111],
        )


def test_ev44_writes(write_file):
    with h5py.File(write_file, "r") as f:
        assert np.array_equal(
            f["/entry/instrument/event_detector/events/event_time_offset"][:],
            [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160],
        )
