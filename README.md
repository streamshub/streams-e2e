# STREAMS-E2E

Test suite for verify interoperability of streams components like kafka, flink, etc... managed by operators on kubernetes.

## Requirements
There is several requirements you have to have installed to properly build the project and run the tests:
- Java 17+
- Helm 3+

## Use latest released upstream operators install files
Run maven with profile `get-operator-files` to download all operators install files which will be used in test suite.

```bash
$ ./mvnw install -P get-operator-files
```
All operator install files are download into `operator-install-files` folder

## Use operators from operator catalog
TODO

## Use own install files
If you want to use own installation files you need to complete following steps

* Install upstream files to create proper structure
```bash
$ ./mvnw install -P get-operator-files
```

* Replace install files in `operator-install-files` folder

## Use own operator metadata bundle-container
TODO

## Run tests
Run all tests.
```bash
$ ./mvnw verify -P test
```

Run specific tag.
```bash
$ ./mvnw verify -P test -Dgroups=smoke
```

Run specific test class or test
```bash
$ ./mvnw verify -P test -Dit.tests=io.streams.e2e.dummy.DummyST
$ ./mvnw verify -P test -Dit.tests=io.streams.e2e.dummy.DummyST#dummyTest
```

## Test configuration
TODO
