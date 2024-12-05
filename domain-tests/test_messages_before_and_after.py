import h5py
import pytest
from conftest import write_file


# This fixture is used to create the file
@pytest.fixture(scope="module")
def local_file(request):
    return write_file(
        request,
        "output_files/messages_before_and_after.hdf",
        "nexus_templates/messages_before_and_after_template.json",
        "data_files/messages_before_and_after_data.json",
    )


def test_last_f144_data_before_start_is_written_but_earlier_values_ignored(local_file):
    with h5py.File(local_file, "r") as f:
        assert f["/entry/instrument/chopper/delay/value"][0] == 5
        assert f["/entry/instrument/chopper/delay/time"][0] == 9_999_000_000


def test_f144_data_after_stop_not_written(local_file):
    with h5py.File(local_file, "r") as f:
        assert f["/entry/instrument/chopper/delay/value"][~0] == 16
        assert f["/entry/instrument/chopper/delay/time"][~0] == 15_000_000_000


def test_ev44_data_before_start_is_not_written(local_file):
    with h5py.File(local_file, "r") as f:
        assert (
            f["/entry/instrument/event_detector/events/event_time_zero"][0]
            == 10_000_000_000
        )


def test_ev44_data_after_stop_not_written(local_file):
    with h5py.File(local_file, "r") as f:
        assert (
            f["/entry/instrument/event_detector/events/event_time_zero"][~0]
            == 15_000_000_000
        )
