import h5py
import pytest
from conftest import write_file


# This fixture is used to create the file
@pytest.fixture(scope="module")
def local_file(request):
    return write_file(
        request,
        "output_files/buffered.hdf",
        "nexus_templates/buffered_template.json",
        "data_files/buffered_data.json",
    )


def test_f144_data_before_start_is_written_when_no_data_between_start_and_stop(local_file):
    with h5py.File(local_file, "r") as f:
        assert f["/entry/instrument/chopper/delay/value"][0] == 3
        assert f["/entry/instrument/chopper/delay/time"][0] == 9_000_000_000