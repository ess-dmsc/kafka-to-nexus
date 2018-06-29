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

echo /kafka_to_nexus/kafka-to-nexus --command-uri ${COMMAND_URI:="//localhost:9092/TEST_writerCommand"} \
  --status-uri ${STATUS_URI:="//localhost:9092/TEST_writerStatus"} \
  --graylog-logger-address ${GRAYLOG_ADDRESS:="localhost:12201"} \
  --hdf-output-prefix ${HDF_OUTPUT_PREFIX:="/output-files/"} \
  -v=${LOG_LEVEL:="3"}

/kafka_to_nexus/kafka-to-nexus --command-uri ${COMMAND_URI:="//localhost:9092/TEST_writerCommand"} \
  --status-uri ${STATUS_URI:="//localhost:9092/TEST_writerStatus"} \
  --graylog-logger-address ${GRAYLOG_ADDRESS:="localhost:12201"} \
  --hdf-output-prefix ${HDF_OUTPUT_PREFIX:="/output-files/"} \
  -v=${LOG_LEVEL:="3"}
