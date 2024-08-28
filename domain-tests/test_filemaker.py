import h5py
import numpy as np


def test_f144_writes(write_file):
    with h5py.File(write_file, "r") as f:
        assert np.array_equal(
            f["/entry/instrument/chopper/rotation_speed/value"][:], [10, 15]
        )
        # TODO: timestamps

def test_ev44_writes(write_file):
    with h5py.File(write_file, "r") as f:
        assert np.array_equal(
            f["/entry/instrument/event_detector/events/event_time_offset"][:], [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160]
        )
