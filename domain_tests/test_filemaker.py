from subprocess import Popen
import h5py
import numpy as np
import pytest
import os


OUTPUT_FILE = "output.hdf"


@pytest.fixture(scope="session")
def setup(request):
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    args = [
        "../_ninja/bin/file-maker",
        "-f",
        "nexus_template.json",
        "-o",
        OUTPUT_FILE,
        "-d",
        "data_file.json",
    ]
    proc = Popen(args)
    outs, errs = proc.communicate(timeout=15)
    print(outs)


def test_f144_writes(setup):
    with h5py.File(OUTPUT_FILE, "r") as f:
        assert np.array_equal(
            f["/entry/instrument/mini_chopper/speed/value"][:], [10, 15]
        )
        # TODO: timestamps
