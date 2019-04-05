## System tests

The system tests consist of a series of automated tests for this repository that test it in a way similar to how it would be used in production.

It uses Docker containers to create containerised instances of Kafka and other components.

### Usage

[optional] Set up a Python virtual environment and activate it (see [here](https://virtualenv.pypa.io/en/stable/))

* Install Docker

* Install the requirements using pip: `pip install -r system-tests/requirements.txt`

* Stop and remove any containers that may interfere with the system tests, e.g IOC or Kafka containers and containers from previous runs. To stop and remove all containers use `docker stop $(docker ps -a -q) && docker rm $(docker ps -a -q)`

* Run `pytest -s .` from the `system-tests/` directory

* Wait

Note: these tests take some time to run.


### General Architecture
The system tests use pytest for the test runner, and use separate fixtures for different configurations of the file-writer. 

Firstly, the system tests attempt to build and tag the latest file-writer image. This can take a lot of time especially if something in conan has changed, as it has to reinstall all of the conan packages.

The Kafka and Zookeeper containers are started with `docker-compose` and persist throughout all of the tests, and when finished will be stopped and removed.

Each fixture starts the file-writer with an `ini` config file (found in `/config-files`), and in some cases use a JSON command at startup so nothing has to be sent over Kafka. Some fixtures start the file-writer multiple times and others use the NeXus-streamer image. 

In some tests, command messages in `JSON` form are sent to Kafka to change the configuration of the file-writer during testing. 

Most tests check the NeXus file created by the file-writer contains the correct static and streamed data, however, some tests also consume everything from the status topic to assert against. 

Log files are placed in the `logs` folder in `system-tests` providing that the `ini` file is using the `--log-file` flag and the docker-compose file is mounting the `logs` directory.

### Creating tests

To create a new fixture, a new function should be added in `conftest.py` as well as a docker compose file in `compose/` and a startup `ini` config file. The test itself should be created in a file with the prefix `test_`, for example `test_idle_pv_updates`, so that file can be picked up by pytest. 

The fixture name can be used as the first parameter to the test like so: 
`def test_data_reaches_file(docker_compose):`
