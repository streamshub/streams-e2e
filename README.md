# STREAMS-E2E

Test suite for verify interoperability of streams components like kafka, flink, etc... managed by operators on kubernetes.

## Requirements
There is several requirements you have to have installed to properly build the project and run the tests:
- Java 17+
- Helm 3+
- OperatorSDK

## Test scenarios
Test scenarios are documented in test code by [test-metadata-generator](https://github.com/skodjob/test-metadata-generator) and generated docs are stored in [docs](docs) folder.

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
$ ./mvnw verify -P test -Dgroups=flink-sql-example
```

Run specific test class or test
```bash
$ ./mvnw verify -P test -Dit.tests=io.streams.e2e.flink.sql.SqlExampleST
$ ./mvnw verify -P test -Dit.tests=io.streams.e2e.flink.sql.SqlExampleST#testFlinkSqlExample
```

## Test configuration
### Using env vars
- To configure sql runner image set env var `SQL_RUNNER_IMAGE`
- To use custom flink operator bundle image use env var `FLINK_OPERATOR_BUNDLE_IMAGE`

### Using config file
- Modify variables in [config.yaml](config.yaml) file in root folder of repository

## Run Packit CI
If PR is opened, you can use packit for run you tests on top of kind cluster.
To run Packit CI, just make comment with following text...
```
# run sql example test
/packit test --labels flink-sql-example

# run all flink tests
/packit test --labels flink-all

# run smoke tests
/packit test --labels smoke
```
