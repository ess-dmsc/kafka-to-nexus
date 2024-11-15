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


def test_can_create_links(local_file):
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


def test_static_data_is_written(local_file):
    with h5py.File(local_file, "r") as f:
        assert np.array_equal(
            f["/entry/instrument/example_detector/detector_number"][:], [1, 2, 3, 4, 5]
        )
        assert np.array_equal(
            f["/entry/instrument/example_detector/x_pixel_offset"][:],
            [10, 20, 30, 40, 50],
        )
        assert np.array_equal(
            f["/entry/instrument/example_detector/pixel_shape/cylinders"][:],
            np.array([[0, 1, 2]]),
        )
        assert np.array_equal(
            f["/entry/instrument/example_detector/pixel_shape/vertices"][:],
            np.array(
                [
                    [0, 0, 0],
                    [0, 0.05, 0],
                    [0.002, 0, 0],
                ],
                dtype=np.float32,
            ),
        )
