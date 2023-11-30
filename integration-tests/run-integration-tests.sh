#!/bin/sh

docker exec filewriter bash -c 'cd integration-tests; scl enable rh-python38 -- ~/venv/bin/pytest --writer-binary=../kafka-to-nexus --junitxml=IntegrationTestsOutput.xml -k ep0'

result=$?

set -e

docker cp filewriter:/home/jenkins/integration-tests/IntegrationTestsOutput.xml .
docker cp filewriter:/home/jenkins/integration-tests/logs/. logs
docker cp filewriter:/home/jenkins/integration-tests/output-files/. output-files

exit $result
