#!/bin/sh

set -x

docker exec ${FILEWRITER_FILEWRITER_CONTAINER_NAME:-filewriter} bash -c "cd integration-tests; scl enable rh-python38 -- ~/venv/bin/pytest --writer-binary=../kafka-to-nexus --kafka-broker=${FILEWRITER_KAFKA_CONTAINER_NAME:-kafka}:9093 --junitxml=IntegrationTestsOutput.xml"

result=$?

set -e

docker cp ${FILEWRITER_FILEWRITER_CONTAINER_NAME:-filewriter}:/home/jenkins/integration-tests/IntegrationTestsOutput.xml .
docker cp ${FILEWRITER_FILEWRITER_CONTAINER_NAME:-filewriter}:/home/jenkins/integration-tests/logs/. logs
docker cp ${FILEWRITER_FILEWRITER_CONTAINER_NAME:-filewriter}:/home/jenkins/integration-tests/output-files/. output-files

exit $result
