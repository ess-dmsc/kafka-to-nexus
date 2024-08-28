import pytest
import os
from subprocess import Popen


BINARY_PATH = "--file-maker-binary"
OUTPUT_FILE = "output.hdf"


def pytest_addoption(parser):
    parser.addoption(
        BINARY_PATH,
        action="store",
        default=None,
        help="Path to file-maker binary (executable).",
    )


@pytest.fixture(scope="session")
def write_file(request):
    if request.config.getoption(BINARY_PATH) is None:
        raise RuntimeError(
            f'You must supply a path to a file-maker executable ("{BINARY_PATH}").'
        )

    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    args = [
        request.config.getoption(BINARY_PATH),
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
    return OUTPUT_FILE
