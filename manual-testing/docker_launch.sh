#!/usr/bin/env bash

echo /kafka_to_nexus/kafka-to-nexus --command-uri ${COMMAND_URI:="//localhost:9092/TEST_writerCommand"} --status-uri ${STATUS_URI:="//localhost:9092/TEST_writerStatus"}
/kafka_to_nexus/kafka-to-nexus --command-uri ${COMMAND_URI:="//localhost:9092/TEST_writerCommand"} --status-uri ${STATUS_URI:="//localhost:9092/TEST_writerStatus"}
