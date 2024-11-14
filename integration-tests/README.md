## Integration tests
Tests that check the basic functionality of the file-writer including Kafka integration.

*Don't add new tests unless you are certain you need to!*
Prefer to test via Domain tests or unit tests.
Integration tests should only be used when testing something that needs a real Kafka instance, e.g. testing the job pool or status messages.

### Running locally
Might not work on MacOs due to Docker issues, to run locally with Docker see below.

- Create a Python environment with all the requirements installed.
- Start docker:
```
docker-compose up
```
- Create the required topics listed in the topics.txt file:
```
docker exec kafka bash -c "kafka-topics --bootstrap-server localhost:9092 --create --topic <topic_name>"
```
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
pytest -s --file-writer-binary=../_ninja/bin/kafka-to-nexus --kafka-broker=localhost:9092
```