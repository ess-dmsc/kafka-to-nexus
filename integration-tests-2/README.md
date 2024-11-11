## Integration tests
Tests that check the basic functionality of the file-writer including Kafka integration.

### Running locally without Docker
- Create a Python environment with all the requirements installed.
- Run Kafka locally
- Create the topics listed in the docker-compose file
- Run the following command:
```
pytest -vvv --file-writer-binary=../_ninja/bin/kafka-to-nexus --use-local-kafka=True
```