import h5py
import numpy as np
import pytest
from conftest import write_file


# This fixture is used to create the file
@pytest.fixture(scope="module")
def local_file(request):
    return write_file(
        request,
        "output_files/static_information.hdf",
        "nexus_templates/static_information_template.json",
        "data_files/static_information_data.json",
    )


def test_f144_skips_repeated_data(local_file):
    with h5py.File(local_file, "r") as f:
        assert len(f["/entry/instrument/links/linked_value"]) == 4
        assert len(f["/entry/instrument/links/linked_time"]) == 4
        assert np.array_equal(
            f["/entry/instrument/chopper/delay/value"][:],
            f["/entry/instrument/links/linked_value"][:],
        )
        assert np.array_equal(
            f["/entry/instrument/chopper/delay/time"][:],
            f["/entry/instrument/links/linked_time"][:],
        )
