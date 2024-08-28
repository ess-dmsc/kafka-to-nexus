#!/bin/sh

scl enable rh-python38 -- ~/venv/bin/pytest --writer-binary=../kafka-to-nexus --kafka-broker=${FILEWRITER_KAFKA_CONTAINER_NAME:-kafka}:9093 --junitxml=IntegrationTestsOutput.xml"
