# SqlJobRunnerST

**Description:** This test suite verifies that flink-sql-example works correctly

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy the strimzi kafka operator | Strimzi operator is deployed |
| 2. | Deploy the flink operator | Flink operator is deployed |
| 3. | Deploy the apicurio operator | Apicurio operator is deployed |
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
| 1. | Create namespace, serviceaccount and roles for flink | Resources created |
| 2. | Deploy FlinkDeployment with not valid sql statement | FlinkDeployment is deployed |
| 3. | Verify that FlinkDeployment fails | FlinkDeployment failed |
| 4. | Verify error message | Error message contains 'SQL parse failed' |

**Labels:**

* `flink-sql-example` (description file doesn't exist)
* `flink` (description file doesn't exist)


## testFlinkSqlRunnerSimpleFilter

**Description:** Test verifies sql-runner.jar works integrated with kafka, apicurio and uses scram-sha for kafka authentication

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create namespace, serviceaccount and roles for flink | Resources created |
| 2. | Deploy apicurio registry | Apicurio registry is up and running |
| 3. | Deploy kafka my-cluster with scram-sha auth | Kafka is up and running |
| 4. | Create KafkaUser with scram-sha secret | KafkaUser created |
| 5. | Deploy strimzi kafka clients producer with payment data generator | Client job is created and data are sent to flink.payment.data topic |
| 6. | Deploy FlinkDeployment with sql which gets data from flink.payment.data topic filter payment of type paypal and send data to flink.payment.paypal topic, for authentication is used secret created by KafkaUser and this secret is passed into by secret interpolation | FlinkDeployment is up and tasks are deployed and it sends filtered data into flink.payment.paypal topic |
| 7. | Deploy strimzi-kafka-client consumer as job and consume messages fromkafka topic flink.payment.paypal | Consumer is deployed and it consumes messages |
| 8. | Verify that messages are present | Messages are present |

**Labels:**

* `flink-sql-example` (description file doesn't exist)
* `flink` (description file doesn't exist)


## testWrongConnectionInfo

**Description:** Test verifies sql-runner image with not valid kafka connection info

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create namespace, serviceaccount and roles for flink | Resources created |
| 2. | Deploy FlinkDeployment with valid sql statement but not existing kafka connection | FlinkDeployment is deployed |
| 3. | Verify error message | Error message contains 'No resolvable bootstrap urls given in bootstrap.servers' |

**Labels:**

* `flink-sql-example` (description file doesn't exist)
* `flink` (description file doesn't exist)
