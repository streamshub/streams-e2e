# SqlExampleST

**Description:** This test suite verifies that flink-sql-example works correctly

**Before tests execution steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Deploy the Strimzi Kafka operator | Strimzi operator is deployed |
| 2. | Deploy the Flink Kubernetes operator | Flink operator is deployed |
| 3. | Deploy the Apicurio operator | Apicurio operator is deployed |
| 4. | Deploy the cert-manager operator | Cert-manager operator is deployed |

**Labels:**

* `flink-sql-example` (description file doesn't exist)
* `flink` (description file doesn't exist)

<hr style="border:1px solid">

## testFlinkSqlExample

**Description:** Test verifies that flink-sql-example recommended app https://github.com/streamshub/flink-sql-examples/tree/main/recommendation-app works

**Steps:**

| Step | Action | Result |
| - | - | - |
| 1. | Create namespace, serviceaccount and roles for Flink | Resources created |
| 2. | Deploy Apicurio registry | Apicurio registry is up and running |
| 3. | Deploy simple example Kafka my-cluster | Kafka is up and running |
| 4. | Deploy productInventory.csv as configmap | Configmap created |
| 5. | Deploy data-generator deployment | Deployment is up and running |
| 6. | Deploy FlinkDeployment from sql-example | FlinkDeployment is up and tasks are deployed and it sends filtered data into flink.recommended.products topic |
| 7. | Deploy strimzi-kafka-clients consumer as job and consume messages fromkafka topic flink.recommended.products | Consumer is deployed and it consumes messages |
| 8. | Verify that messages are present | Messages are present |

**Labels:**

* `flink-sql-example` (description file doesn't exist)
* `flink` (description file doesn't exist)

