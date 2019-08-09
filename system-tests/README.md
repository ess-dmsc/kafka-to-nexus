## System tests

The system tests consist of a series of automated tests for this repository that test it in a way similar to how it would be used in production.

It uses Docker containers to create containerised instances of Kafka and other components.

### Usage

[optional] Set up a Python virtual environment and activate it (see [here](https://virtualenv.pypa.io/en/stable/))

* Install Docker

* Install the requirements using pip: `pip install -r system-tests/requirements.txt`

* Stop and remove any containers that may interfere with the system tests, e.g IOC or Kafka containers and containers from previous runs. To stop and remove all containers use `docker stop $(docker ps -a -q) && docker rm $(docker ps -a -q)`

* Run `pytest -s .` from the `system-tests/` directory

* Optionally use `--local-build <PATH_TO_BUILD_DIR>` to run against a local build of the file writer rather than rebuilding in a docker container.
Can also use `--wait-to-attach-debugger` to cause the system tests to display the process ID of the file writer and give opportunity for you to attach a debugger before continuing.

Note: these tests take several minutes to run.


### General Architecture
The system tests use pytest for the test runner, and use separate fixtures for different configurations of the file-writer. 

Firstly, the system tests attempt to build and tag the latest file-writer image. This can take a lot of time especially if something in conan has changed, as it has to reinstall all of the conan packages. The image build is layer-cached with three main layers - pulling the base image, dependency gathering (from apt, pip and conan) and building the file-writer. This strikes a balance between time taken to rebuild the image when something changes, and the disk space used by the cached layers.

The Kafka and Zookeeper containers are started with `docker-compose` and persist throughout all of the tests, and when finished will be stopped and removed.

Each fixture starts the file-writer with an `ini` config file (found in `/config-files`). In some cases the fixtures use a JSON command at startup and in others the command is sent over Kafka as part of the test. Some fixtures start the file-writer multiple times or use the NeXus-streamer image as a source of streamed data.

In some tests, command messages in `JSON` form are sent to Kafka to change the configuration of the file-writer during testing. 

Most tests check the NeXus file created by the file-writer contains the correct static and streamed data, however, some tests instead test that the status of the file writer matches expectation, by consuming status messages from Kafka.

Log files are placed in the `logs` folder in `system-tests` provided that the `ini` file is using the `--log-file` flag and the docker-compose file mounts the `logs` directory.

### Creating tests

To create a new fixture, a new function should be added in `conftest.py` as well as a docker compose file in `compose/` and a startup `ini` config file. The test itself should be created in a file with the prefix `test_`, for example `test_idle_pv_updates`, so that file can be picked up by pytest. 

The fixture name must be used as the first parameter to the test like so:
`def test_data_reaches_file(docker_compose):`
