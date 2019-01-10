## System tests

The system tests consist of a series of automated tests for this repository that test it in a way similar to how it would be used in production.

It uses Docker containers to create containerised instances of Kafka and other components.

### Usage

[optional] Set up a Python virtual environment and activate it (see [here](https://virtualenv.pypa.io/en/stable/))

* Install Docker

* Install the requirements using pip: `pip install -r system-tests/requirements.txt`

* Run `pytest -s .` from the `system-tests/` directory

* Wait

Note: these tests take some time to run.
