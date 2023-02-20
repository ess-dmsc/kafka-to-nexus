#!/usr/bin/env bash

KAFKA_BROKER="kafka:9092"
KAFKA_CMD="kafkacat -b ${KAFKA_BROKER} -L"
echo "Waiting for Kafka to become available"

FOUND=false

for I in {1..10}
do
  if $KAFKA_CMD | grep -q "TEST_writer_commands_alternative" && $KAFKA_CMD | grep -q "TEST_writer_commands" && $KAFKA_CMD | grep -q "TEST_writer_jobs"; then
    FOUND=true
    break
  else
    sleep 5
  fi
done

if $FOUND; then
  echo "Kafka and topics found. Launching file-writer."
else
  echo "Unable to find kafka broker (with required topics) at: ${KAFKA_BROKER}. Exiting."
  exit
fi

export LD_LIBRARY_PATH=/home/jenkins/build/lib/


COMMAND=/home/jenkins/build/bin/kafka-to-nexus\ --config-file\ "${CONFIG_FILE}"\ --log-file\ "${LOG_FILE}"

echo $COMMAND
$COMMAND
