import h5py
import numpy as np
import pytest
from conftest import write_file


# This fixture is used to create the file
@pytest.fixture(scope="module")
def local_file(request):
    return write_file(
        request,
        "output_files/writer_modules.hdf",
        "nexus_templates/writer_modules_template.json",
        "data_files/writer_modules_data.json",
    )


def test_f144_writes(local_file):
    with h5py.File(local_file, "r") as f:
        assert f["/entry/instrument/chopper/rotation_speed/value"][:].shape == (2,)
        assert f["/entry/instrument/chopper/rotation_speed/time"][:].shape == (2,)
        assert np.array_equal(
            f["/entry/instrument/chopper/rotation_speed/value"][:], [10, 15]
        )
        assert np.array_equal(
            f["/entry/instrument/chopper/rotation_speed/time"][:],
            [10100000000, 10110000000],
        )
        assert f["/entry/instrument/chopper/rotation_speed/minimum_value"][()] == 10
        assert f["/entry/instrument/chopper/rotation_speed/maximum_value"][()] == 15
        assert f["/entry/instrument/chopper/rotation_speed/average_value"][()] == 12.5


def test_f144_units_attributes_if_in_json(local_file):
    with h5py.File(local_file, "r") as f:
        assert "units" in f["/entry/instrument/chopper/rotation_speed/value"].attrs

        assert (
            f["/entry/instrument/chopper/rotation_speed/value"].attrs["units"] == "Hz"
        )
        assert f["/entry/instrument/chopper/rotation_speed/time"].attrs["units"] == "ns"
        assert (
            f["/entry/instrument/chopper/rotation_speed/minimum_value"].attrs["units"]
            == "Hz"
        )
        assert (
            f["/entry/instrument/chopper/rotation_speed/maximum_value"].attrs["units"]
            == "Hz"
        )
        assert (
            f["/entry/instrument/chopper/rotation_speed/average_value"].attrs["units"]
            == "Hz"
        )


def test_f144_units_attributes_if_not_in_json(local_file):
    with h5py.File(local_file, "r") as f:
        assert "units" in f["/entry/instrument/chopper/delay/value"].attrs

        assert f["/entry/instrument/chopper/delay/value"].attrs["units"] == ""
        assert f["/entry/instrument/chopper/delay/time"].attrs["units"] == "ns"
        assert f["/entry/instrument/chopper/delay/minimum_value"].attrs["units"] == ""
        assert f["/entry/instrument/chopper/delay/maximum_value"].attrs["units"] == ""
        assert f["/entry/instrument/chopper/delay/average_value"].attrs["units"] == ""


def test_ep01_writes(local_file):
    with h5py.File(local_file, "r") as f:
        assert np.array_equal(
            f["/entry/instrument/chopper/rotation_speed/connection_status"][:], [2, 2]
        )
        assert np.array_equal(
            f["/entry/instrument/chopper/rotation_speed/connection_status_time"][:],
            [10101000000, 10111000000],
        )


def test_al00_writes(local_file):
    with h5py.File(local_file, "r") as f:
        messages = f["/entry/instrument/chopper/rotation_speed/alarm_message"][:]
        assert messages[0].decode() == "Chopper speed is too low"
        assert messages[1].decode() == "Chopper speed is perfect"
        assert np.array_equal(
            f["/entry/instrument/chopper/rotation_speed/alarm_severity"][:], [1, 0]
        )
        assert np.array_equal(
            f["/entry/instrument/chopper/rotation_speed/alarm_time"][:],
            [10102000000, 10112000000],
        )


def test_ev44_writes(local_file):
    with h5py.File(local_file, "r") as f:
        assert np.array_equal(
            f["/entry/instrument/event_detector/events/event_time_offset"][:],
            [i * 10 for i in range(1, 17)],
        )
        assert np.array_equal(
            f["/entry/instrument/event_detector/events/event_time_zero"][:],
            [10_200_000_000, 10_210_000_000, 10_220_000_000, 10_230_000_000],
        )
        assert np.array_equal(
            f["/entry/instrument/event_detector/events/event_index"][:],
            [0, 4, 8, 12],
        )
        assert np.array_equal(
            f["/entry/instrument/event_detector/events/event_id"][:],
            [1, 2, 3, 4] * 4,
        )


def test_ad00_writes(local_file):
    with h5py.File(local_file, "r") as f:
        expected_data = np.array(
            [
                [[10, 11], [12, 13]],
                [[13, 12], [11, 10]],
            ]
        )
        assert np.array_equal(
            f["/entry/instrument/image_detector/data/value"][:],
            expected_data,
        )
        assert np.array_equal(
            f["/entry/instrument/image_detector/data/time"][:],
            [10_300_000_000, 10_310_000_000],
        )


def test_ad00_units_attributes_if_not_in_json(local_file):
    with h5py.File(local_file, "r") as f:
        assert "units" in f["/entry/instrument/image_detector/data/value"].attrs

        assert f["/entry/instrument/image_detector/data/value"].attrs["units"] == ""
        assert f["/entry/instrument/image_detector/data/time"].attrs["units"] == "ns"
