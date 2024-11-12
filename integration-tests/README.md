## Integration tests
Tests that check the basic functionality of the file-writer including Kafka integration.

### Running locally
- Create a Python environment with all the requirements installed.
- Run the following command:
```
pytest -s --file-writer-binary=../_ninja/bin/kafka-to-nexus 
```
### Running locally without Docker
Can be more convenient as Kafka remains running between tests runs, so other tools can be used to probe data.
Also, it is a bit quicker as it doesn't have to wait for Docker and Kafka to start.

- Create a Python environment with all the requirements installed.
- Run Kafka locally
- Create the topics listed in the topics.txt file
- Run the following command with appropriate Kafka address:
```
pytest -s --file-writer-binary=../_ninja/bin/kafka-to-nexus --use-local-kafka=localhost:9092
```