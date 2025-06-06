# SqlJobRunnerST

**Description:** This test suite verifies that flink-sql-example works correctly

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy the Strimzi Kafka operator | Strimzi operator is deployed |
| 2. | Deploy the Flink Kubernetes operator | Flink operator is deployed |
| 3. | Deploy the Apicurio operator | Apicurio operator is deployed |
| 4. | Deploy the cert-manager operator | Cert-manager operator is deployed |

**Labels:**

* `flink-sql-runner` (description file doesn't exist)
* `flink` (description file doesn't exist)

<hr style="border:1px solid">

## testBadSqlStatement

**Description:** Test verifies that sql-runner.jar fail properly with not valid sql statement

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create namespace, serviceaccount and roles for Flink | Resources created |
| 2. | Deploy FlinkDeployment with not valid sql statement | FlinkDeployment is deployed |
| 3. | Verify that FlinkDeployment fails | FlinkDeployment failed |
| 4. | Verify error message | Error message contains 'SQL parse failed' |

**Labels:**

* `flink-sql-runner` (description file doesn't exist)
* `flink` (description file doesn't exist)


## testFRocksDbStateBackend

**Description:** Test verifies that user can use FRocksDB as state backend

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create namespace, serviceaccount and roles for Flink | Resources created |
| 2. | Deploy Apicurio registry | Apicurio registry is up and running |
| 3. | Deploy Kafka my-cluster with scram-sha auth | Kafka is up and running |
| 4. | Create KafkaUser with scram-sha secret | KafkaUser created |
| 5. | Deploy strimzi-kafka-clients producer with payment data generator | Client job is created and data are sent to flink.payment.data topic |
| 6. | Create PVC for FlinkDeployment for FRocksDB | PVC is created |
| 7. | Deploy FlinkDeployment with sql which gets data from flink.payment.data topic filter payment of type paypal and send data to flink.payment.paypal topic, for authentication is used secret created by KafkaUser and this secret is passed into by secret interpolation. Flink is configured to use FRocksDB as a state backend | FlinkDeployment is up and tasks are deployed and it sends filtered data into flink.payment.paypal topic, task manager deployed by FlinkDeployment uses FRockDB |
| 8. | Deploy strimzi-kafka-clients consumer as job and consume messages fromkafka topic flink.payment.paypal | Consumer is deployed and it consumes messages |
| 9. | Verify that messages are present | Messages are present |
| 10. | Verify that taskmanager logs contains 'State backend loader loads the state backend as EmbeddedRocksDBStateBackend' | Log message is present |

**Labels:**

* `flink-sql-runner` (description file doesn't exist)
* `flink` (description file doesn't exist)


## testS3StateBackend

**Description:** Test verifies that user can use S3 as state backend

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create namespace, serviceaccount and roles for Flink | Resources created |
| 2. | Deploy Apicurio registry | Apicurio registry is up and running |
| 3. | Deploy Kafka my-cluster with scram-sha auth | Kafka is up and running |
| 4. | Create KafkaUser with scram-sha secret | KafkaUser created |
| 5. | Deploy strimzi-kafka-clients producer with payment data generator | Client job is created and data are sent to flink.payment.data topic |
| 6. | Deploy Minio for S3 service | Minio is up and running |
| 7. | Deploy FlinkDeployment with sql which gets data from flink.payment.data topic filter payment of type paypal and send data to flink.payment.paypal topic, for authentication is used secret created by KafkaUser and this secret is passed into by secret interpolation. Flink is configured to use S3 as a state backend | FlinkDeployment is up and tasks are deployed and it sends filtered data into flink.payment.paypal topic, task manager deployed by FlinkDeployment uses S3 |
| 8. | Deploy strimzi-kafka-clients consumer as job and consume messages fromkafka topic flink.payment.paypal | Consumer is deployed and it consumes messages |
| 9. | Verify that messages are present | Messages are present |
| 10. | Verify that taskmanager logs contains 'State backend loader loads the state backend as HashMapStateBackend' | Log message is present |
| 11. | Verify that Minio contains some data from Flink | Flink bucket is not empty |

**Labels:**

* `flink-sql-runner` (description file doesn't exist)
* `flink` (description file doesn't exist)


## testSimpleFilter

**Description:** Test verifies sql-runner.jar works integrated with kafka, apicurio and uses scram-sha for kafka authentication

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create namespace, serviceaccount and roles for Flink | Resources created |
| 2. | Deploy Apicurio registry | Apicurio registry is up and running |
| 3. | Deploy Kafka my-cluster with scram-sha auth | Kafka is up and running |
| 4. | Create KafkaUser with scram-sha secret | KafkaUser created |
| 5. | Deploy strimzi-kafka-clients producer with payment data generator | Client job is created and data are sent to flink.payment.data topic |
| 6. | Deploy FlinkDeployment with sql which gets data from flink.payment.data topic filter payment of type paypal and send data to flink.payment.paypal topic, for authentication is used secret created by KafkaUser and this secret is passed into by secret interpolation | FlinkDeployment is up and tasks are deployed and it sends filtered data into flink.payment.paypal topic |
| 7. | Deploy strimzi-kafka-clients consumer as job and consume messages fromkafka topic flink.payment.paypal | Consumer is deployed and it consumes messages |
| 8. | Verify that messages are present | Messages are present |

**Labels:**

* `flink-sql-runner` (description file doesn't exist)
* `flink` (description file doesn't exist)


## testWrongConnectionInfo

**Description:** Test verifies sql-runner image with not valid kafka connection info

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create namespace, serviceaccount and roles for Flink | Resources created |
| 2. | Deploy FlinkDeployment with valid sql statement but not existing kafka connection | FlinkDeployment is deployed |
| 3. | Verify error message | Error message contains 'No resolvable bootstrap urls given in bootstrap.servers' |

**Labels:**

* `flink-sql-runner` (description file doesn't exist)
* `flink` (description file doesn't exist)

