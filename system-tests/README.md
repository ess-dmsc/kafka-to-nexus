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
