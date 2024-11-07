import pytest
import os
import subprocess


BINARY_PATH = "--file-maker-binary"
CLEANUP_OUTPUT = "--cleanup-output"


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
    parser.addoption(
        CLEANUP_OUTPUT,
        action="store_true",
        default=False,
        help="Remove the output file after the test run.",
    )


def write_file(request, output_file, template_file, data_file):
    if request.config.getoption(BINARY_PATH) is None:
        raise RuntimeError(
            f'You must supply a path to a file-maker executable ("{BINARY_PATH}").'
        )

    def finalize():
        if os.path.exists(output_file):
            os.remove(output_file)

    if request.config.getoption(CLEANUP_OUTPUT):
        request.addfinalizer(finalize)

    if os.path.exists(output_file):
        os.remove(output_file)
    args = [
        request.config.getoption(BINARY_PATH),
        "-f",
        template_file,
        "-o",
        output_file,
        "-d",
        data_file,
    ]
    run_file_maker(args)
    return output_file
