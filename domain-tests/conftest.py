import pytest
import os
import subprocess


BINARY_PATH = "--file-maker-binary"
OUTPUT_FILE = "output.hdf"


def run_file_maker(args, timeout=15):
    try:
        result = subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=True,
        )
        return result.stdout
    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"Process timed out: {e}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Command failed with error: {e.stderr}")
    except Exception as e:
        raise RuntimeError(f"An unexpected error occurred: {e}")


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

    def finalize():
        if os.path.exists(OUTPUT_FILE):
            os.remove(OUTPUT_FILE)

    request.addfinalizer(finalize)

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
    file_maker_output = run_file_maker(args)
    return OUTPUT_FILE
