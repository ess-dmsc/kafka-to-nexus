## Domain tests

Tests that run the whole `kafka-to-nexus` code except the parts that talk to Kafka.

Using the file-maker, the input "data" is read from JSON rather than from Kafka, so the tests are more reliable than the integration-tests.

To run:
```
$ cd domain-tests
$ pytest ---file-maker-binary=<path to file-maker binary>
```