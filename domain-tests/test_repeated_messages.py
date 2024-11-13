import h5py
import numpy as np
import pytest
from conftest import write_file


# This fixture is used to create the file
@pytest.fixture(scope="module")
def local_file(request):
    return write_file(
        request,
        "output_files/repeated_messages.hdf",
        "nexus_templates/repeated_messages_template.json",
        "data_files/repeated_messages_data.json",
    )


def test_f144_skips_repeated_data(local_file):
    with h5py.File(local_file, "r") as f:
        assert np.array_equal(f["/entry/instrument/chopper/delay/value"][:], [10, 12, 15])
        assert np.array_equal(f["/entry/instrument/chopper/delay/time"][:], [10_100_000_000, 10_150_000_000, 10_200_000_000])
