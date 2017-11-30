## Manual Testing

The repository contains a `docker-compose` script to use for manual testing of the file writer.
This can be launched by using `docker-compose up` from the root directory of the repository.

Once the containers have been launched kafkacat (https://github.com/edenhill/kafkacat) can be used to send a json command to start writing a file.
For example:
```
kafkacat -P -b localhost -t example-json-command.json
```
The resulting file will be written at ___.

TODO document how to view container logs, get into the container etc

TODO add link to this documentation from the README.md
