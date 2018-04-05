## Manual Testing

The repository contains a `docker-compose` script to use for manual testing of the file writer.
This can be launched by using `docker-compose up` from the root directory of the repository.

Once the containers have been launched kafkacat (https://github.com/edenhill/kafkacat) can be used to send json commands from file.
For example:
```
kafkacat -P -b localhost -t TEST_writerCommand -p 0 example-json-command.json
kafkacat -P -b localhost -t TEST_writerCommand -p 0 stop-command.json
kafkacat -P -b localhost -t TEST_writerCommand -p 0 writer-exit.json
```

The resulting file can be found in `manual-testing/output-files` as the docker-compose script mounts this location as the output directory for the file writer.

### Increasing allowed message size

For large commands (>1MB) it is necessary to increase the `message.max.bytes` and `replica.fetch.max.bytes` on the broker. When using the Kafka docker container this is achieved by specifying the follwing environment variables in the docker-compose script:
```
KAFKA_MESSAGE_MAX_BYTES: 20000000
KAFKA_REPLICA_FETCH_MAX_BYTES: 20000000
```
Alternatively, these configuration parameters can be set just for only specific topic by creating the topic with the bash script provided with Kafka, or changed in the Kafka-Manager interface. When using Kafkacat the corresponding producer configuration _must_ also be set using the `-X` flag, for example
```
kafkacat -P -X message.max.bytes=20000000 -b localhost -t TEST_writerCommand -p 0 example-json-command.json
```
allows max message size of 20MB.

### Other tools
The Kafka-Manager interface is available at http://localhost:9000
You'll need to select `Cluster`->`Add Cluster` and enter `zookeeper:2181` in the `Cluster Zookeeper Hosts` field, a name in the `Cluster Name` field, everything else can be left as default.

The Graylog interface is available at http://localhost:9001 and you can log in as user: admin, password: admin. You'll need to configure a GELF TCP input before messages are queryable.
