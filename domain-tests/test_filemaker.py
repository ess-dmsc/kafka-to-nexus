import h5py
import numpy as np


def test_f144_writes(write_file):
    with h5py.File(write_file, "r") as f:
        assert np.array_equal(
            f["/entry/instrument/mini_chopper/speed/value"][:], [10, 15]
        )
        # TODO: timestamps
