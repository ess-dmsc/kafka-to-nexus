#!/usr/bin/env bash

# Wait for Kafka broker to be up
kafkacat -b ${KAFKA_BROKER:="localhost"} -L
OUT=$?
i="0"
while [ $OUT -ne 0 -a  $i -ne 5  ]; do
   echo "Waiting for Kafka to be ready"
   sleep 10
   kafkacat -b ${KAFKA_BROKER:="localhost"} -L
   OUT=$?
   let i=$i+1
   echo $i
done
if [ $i -eq 5 ]
then
   echo "Kafka broker not accessible at file writer launch"
   exit 1
fi

COMMAND_URI="${COMMAND_URI:=//localhost:9092/TEST_writerCommand}"
STATUS_URI="${STATUS_URI:=//localhost:9092/TEST_writerStatus}"
GRAYLOG_ADDRESS="${GRAYLOG_ADDRESS:=localhost:12201}"
HDF_OUTPUT_PREFIX="${HDF_OUTPUT_PREFIX:=/output-files/}"

if [ -z "$CONFIG_FILE" ]
then
    COMMAND=/kafka_to_nexus/bin/kafka-to-nexus\ --command-uri\ "${COMMAND_URI}"\ \
      --status-uri\ "${STATUS_URI}"\ \
      --graylog-logger-address\ "${GRAYLOG_ADDRESS}"\ \
      --hdf-output-prefix\ "${HDF_OUTPUT_PREFIX}"\ \
      -v\ "${LOG_LEVEL:=3}"

    echo $COMMAND
    $COMMAND

else
    COMMAND=/kafka_to_nexus/bin/kafka-to-nexus\ --config-file\ "${CONFIG_FILE}"

    echo $COMMAND
    $COMMAND
fi
