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

The resulting file can be copied out of the container. Get the container id from the output of `docker ps`. Then do
```
docker cp <CONTAINER_ID>:/kafka_to_nexus/output_file.nxs <DESTINATION_PATH>
```

### Other tools
The Kafka-Manager interface is available at http://localhost:9000
You'll need to select `Cluster`->`Add Cluster` and enter `zookeeper:2181` in the `Cluster Zookeeper Hosts` field, a name in the `Cluster Name` field, everything else can be left as default.

The Graylog interface is available at http://localhost:9001 and you can log in as user: admin, password: admin. You'll need to configure a GELF TCP input before messages are queryable.
