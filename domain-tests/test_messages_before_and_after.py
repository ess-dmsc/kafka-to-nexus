import h5py
import numpy as np
import pytest
from conftest import write_file


# This fixture is used to create the file
@pytest.fixture(scope="module")
def local_file(request):
    return write_file(
        request,
        "output_files/data_before_and_after.hdf",
        "nexus_templates/data_before_and_after_template.json",
        "data_files/messages_before_and_after_data.json",
    )


def test_last_data_before_start_is_written_but_earlier_values_ignored(local_file):
    with h5py.File(local_file, "r") as f:
        assert f["/entry/instrument/chopper/delay/value"][0] == 5
        assert f["/entry/instrument/chopper/delay/time"][0] == 9_999_000_000


def test_first_data_after_stop_written_but_later_values_ignored(local_file):
    with h5py.File(local_file, "r") as f:
        assert f["/entry/instrument/chopper/delay/value"][~0] == 17
        assert f["/entry/instrument/chopper/delay/time"][~0] == 16_000_000_000
