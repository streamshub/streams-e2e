/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.e2e.flink.sql;

import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.TestFrameConstants;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.wait.Wait;
import io.streams.clients.kafka.StrimziKafkaClients;
import io.streams.clients.kafka.StrimziKafkaClientsBuilder;
import io.streams.e2e.Abstract;
import io.streams.operands.apicurio.templates.ApicurioRegistryTemplate;
import io.streams.operands.flink.resoruces.FlinkDeploymentType;
import io.streams.operands.flink.templates.FlinkDeploymentTemplate;
import io.streams.operands.flink.templates.FlinkRBAC;
import io.streams.operands.minio.SetupMinio;
import io.streams.operands.strimzi.resources.KafkaType;
import io.streams.operands.strimzi.templates.KafkaNodePoolTemplate;
import io.streams.operands.strimzi.templates.KafkaTemplate;
import io.streams.operands.strimzi.templates.KafkaUserTemplate;
import io.streams.operators.EOperator;
import io.streams.operators.OperatorInstaller;
import io.streams.sql.TestStatements;
import io.streams.utils.MinioUtils;
import io.streams.utils.StrimziClientUtils;
import io.streams.utils.TestUtils;
import io.streams.utils.kube.JobUtils;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthentication;
import org.apache.flink.v1beta1.FlinkDeployment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.streams.constants.TestTags.FLINK;
import static io.streams.constants.TestTags.FLINK_SQL_RUNNER;
import static io.streams.constants.TestTags.SMOKE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(FLINK)
@Tag(FLINK_SQL_RUNNER)
@SuiteDoc(
    description = @Desc("This test suite verifies that flink-sql-example works correctly"),
    beforeTestSteps = {
        @Step(value = "Deploy the Strimzi Kafka operator", expected = "Strimzi operator is deployed"),
        @Step(value = "Deploy the Flink Kubernetes operator", expected = "Flink operator is deployed"),
        @Step(value = "Deploy the Apicurio operator", expected = "Apicurio operator is deployed"),
        @Step(value = "Deploy the cert-manager operator", expected = "Cert-manager operator is deployed")
    },
    labels = {
        @Label(value = FLINK_SQL_RUNNER),
        @Label(value = FLINK),
    }
)
public class SqlJobRunnerST extends Abstract {
    final String kafkaClusterName = "my-cluster";

    @BeforeAll
    void prepareOperators() throws Exception {
        OperatorInstaller.installRequiredOperators(EOperator.FLINK, EOperator.APICURIO,
            EOperator.STRIMZI, EOperator.CERT_MANAGER);
    }

    @TestDoc(
        description = @Desc("Test verifies sql-runner.jar works integrated with kafka, " +
            "apicurio and uses scram-sha for kafka authentication"),
        steps = {
            @Step(value = "Create namespace, serviceaccount and roles for Flink", expected = "Resources created"),
            @Step(value = "Deploy Apicurio registry", expected = "Apicurio registry is up and running"),
            @Step(value = "Deploy Kafka my-cluster with scram-sha auth", expected = "Kafka is up and running"),
            @Step(value = "Create KafkaUser with scram-sha secret", expected = "KafkaUser created"),
            @Step(value = "Deploy strimzi-kafka-clients producer with payment data generator",
                expected = "Client job is created and data are sent to flink.payment.data topic"),
            @Step(value = "Deploy FlinkDeployment with sql which gets data from flink.payment.data topic filter " +
                "payment of type paypal and send data to flink.payment.paypal topic, for authentication is used " +
                "secret created by KafkaUser and this secret is passed into by secret interpolation",
                expected = "FlinkDeployment is up and tasks are deployed and it sends filtered " +
                    "data into flink.payment.paypal topic"),
            @Step(value = "Deploy strimzi-kafka-clients consumer as job and consume messages from" +
                "kafka topic flink.payment.paypal",
                expected = "Consumer is deployed and it consumes messages"),
            @Step(value = "Verify that messages are present", expected = "Messages are present"),
        },
        labels = {
            @Label(value = FLINK_SQL_RUNNER),
            @Label(value = FLINK),
        }
    )
    @Test
    @Tag(SMOKE)
    void testSimpleFilter() {
        String namespace = "flink-filter";
        String kafkaUser = "test-user";
        // Create namespace
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                new NamespaceBuilder().withNewMetadata()
                    .withName(namespace)
                    .endMetadata()
                    .build());

        // Add flink RBAC
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                FlinkRBAC.getFlinkRbacResources(namespace)
                    .toArray(new HasMetadata[0]));

        // Create kafka
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                KafkaNodePoolTemplate.defaultKafkaNodePoolJbod(namespace, "dual-role",
                        3, kafkaClusterName, List.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER))
                    .build());

        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                KafkaTemplate.defaultKafka(namespace, kafkaClusterName)
                    .editSpec()
                    .editKafka()
                    .withListeners(
                        new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withTls(false)
                            .withType(KafkaListenerType.INTERNAL)
                            .withPort((9092))
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build(),
                        new GenericKafkaListenerBuilder()
                            .withName("unsecure")
                            .withTls(false)
                            .withType(KafkaListenerType.INTERNAL)
                            .withPort((9094))
                            .build()
                    )
                    .endKafka()
                    .endSpec()
                    .build());

        // Create topic for ksql apicurio
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                ApicurioRegistryTemplate.apicurioKsqlTopic(namespace, kafkaClusterName, 3));

        // Create kafka scram sha user
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                KafkaUserTemplate.defaultKafkaUser(namespace, kafkaUser, kafkaClusterName)
                    .editSpec()
                    .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
                    .endSpec()
                    .build());

        String bootstrapServerAuth = KafkaType.kafkaClient()
            .inNamespace(namespace)
            .withName(kafkaClusterName)
            .get()
            .getStatus()
            .getListeners()
            .stream()
            .filter(l -> l.getName()
                .equals("plain"))
            .findFirst()
            .get()
            .getBootstrapServers();
        String bootstrapServerUnsecure = KafkaType.kafkaClient()
            .inNamespace(namespace)
            .withName(kafkaClusterName)
            .get()
            .getStatus()
            .getListeners()
            .stream()
            .filter(l -> l.getName()
                .equals("unsecure"))
            .findFirst()
            .get()
            .getBootstrapServers();

        // Add apicurio
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                ApicurioRegistryTemplate.defaultApicurioRegistry("apicurio-registry", namespace,
                        bootstrapServerUnsecure)
                    .build());

        // Get user secret jaas configuration
        final String saslJaasConfigEncrypted = KubeResourceManager.getKubeClient()
            .getClient()
            .secrets()
            .inNamespace(namespace)
            .withName(kafkaUser)
            .get()
            .getData()
            .get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = TestUtils.decodeFromBase64(saslJaasConfigEncrypted);

        // Run internal producer and produce data
        String producerName = "kafka-producer";
        StrimziKafkaClients kafkaProducerClient = new StrimziKafkaClientsBuilder()
            .withProducerName(producerName)
            .withNamespaceName(namespace)
            .withTopicName("flink.payment.data")
            .withBootstrapAddress(bootstrapServerAuth)
            .withMessageCount(10000)
            .withUsername(kafkaUser)
            .withDelayMs(10)
            .withMessageTemplate("payment_fiat")
            .withAdditionalConfig(
                StrimziClientUtils.getApicurioAdditionalProperties(AvroKafkaSerializer.class.getName(),
                    "http://apicurio-registry-service." + namespace + ".svc:8080/apis/registry/v2") + "\n"
                    + "sasl.mechanism=SCRAM-SHA-512\n"
                    + "security.protocol=SASL_PLAINTEXT\n"
                    + "sasl.jaas.config=" + saslJaasConfigDecrypted
            )
            .build();

        KubeResourceManager.getInstance()
            .createResourceWithWait(
                kafkaProducerClient.producerStrimzi()
            );

        String registryUrl = "http://apicurio-registry-service." + namespace + ".svc:8080/apis/ccompat/v6";

        // Deploy flink with test filter sql statement which filter to specific topic only payment type paypal
        FlinkDeployment flink = FlinkDeploymentTemplate.defaultFlinkDeployment(namespace,
                "flink-filter", List.of(TestStatements.getTestFlinkFilter(
                    bootstrapServerAuth, registryUrl, kafkaUser, namespace)))
            .build();
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(flink);

        JobUtils.waitForJobSuccess(namespace, kafkaProducerClient.getProducerName(),
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM);

        // Run consumer and check if data are filtered
        String consumerName = "kafka-consumer";
        StrimziKafkaClients kafkaConsumerClient = new StrimziKafkaClientsBuilder()
            .withConsumerName(consumerName)
            .withNamespaceName(namespace)
            .withTopicName("flink.payment.paypal")
            .withBootstrapAddress(bootstrapServerAuth)
            .withMessageCount(10)
            .withAdditionalConfig(
                "sasl.mechanism=SCRAM-SHA-512\n" +
                    "security.protocol=SASL_PLAINTEXT\n" +
                    "sasl.jaas.config=" + saslJaasConfigDecrypted
            )
            .withConsumerGroup("flink-filter-test-group")
            .build();

        KubeResourceManager.getInstance()
            .createResourceWithWait(
                kafkaConsumerClient.consumerStrimzi()
            );

        JobUtils.waitForJobSuccess(namespace, kafkaConsumerClient.getConsumerName(),
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM);
        String consumerPodName = KubeResourceManager.getKubeClient()
            .listPodsByPrefixInName(namespace, consumerName)
            .get(0)
            .getMetadata()
            .getName();
        String log = KubeResourceManager.getKubeClient()
            .getLogsFromPod(namespace, consumerPodName);
        assertTrue(log.contains("\"type\":\"paypal\""));
        assertFalse(log.contains("\"type\":\"creditCard\""));
    }

    @TestDoc(
        description = @Desc("Test verifies that sql-runner.jar fail properly with not valid sql statement"),
        steps = {
            @Step(value = "Create namespace, serviceaccount and roles for Flink", expected = "Resources created"),
            @Step(value = "Deploy FlinkDeployment with not valid sql statement",
                expected = "FlinkDeployment is deployed"),
            @Step(value = "Verify that FlinkDeployment fails", expected = "FlinkDeployment failed"),
            @Step(value = "Verify error message", expected = "Error message contains 'SQL parse failed'"),
        },
        labels = {
            @Label(value = FLINK_SQL_RUNNER),
            @Label(value = FLINK),
        }
    )
    @Test
    @Tag(SMOKE)
    void testBadSqlStatement() {
        String namespace = "flink-bad-sql";
        String flinkDeploymentName = namespace;

        // Create namespace
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                new NamespaceBuilder().withNewMetadata()
                    .withName(namespace)
                    .endMetadata()
                    .build());

        // Add flink RBAC
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                FlinkRBAC.getFlinkRbacResources(namespace)
                    .toArray(new HasMetadata[0]));

        // Deploy flink with not valid sql
        FlinkDeployment flink = FlinkDeploymentTemplate.defaultFlinkDeployment(namespace,
                flinkDeploymentName, List.of("blah blah"))
            .build();
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithoutWait(flink);

        // Check if no task is deployed and error is proper in flink deployment
        Wait.until("Flink deployment fail", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM, () -> {
                String error = new FlinkDeploymentType().getClient()
                    .inNamespace(namespace)
                    .withName(flinkDeploymentName)
                    .get()
                    .getStatus()
                    .getError();
                return error.contains("DeploymentFailedException") || error.contains("ReconciliationException");
            });

        String podName = KubeResourceManager.getKubeClient()
            .listPodsByPrefixInName(namespace, flinkDeploymentName)
            .get(0)
            .getMetadata()
            .getName();

        Wait.until("Flink deployment contains error message", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM, () ->
                KubeResourceManager.getKubeClient()
                    .getLogsFromPod(namespace, podName)
                    .contains("SQL parse failed"));
    }

    @TestDoc(
        description = @Desc("Test verifies sql-runner image with not valid kafka connection info"),
        steps = {
            @Step(value = "Create namespace, serviceaccount and roles for Flink", expected = "Resources created"),
            @Step(value = "Deploy FlinkDeployment with valid sql statement but not existing kafka connection",
                expected = "FlinkDeployment is deployed"),
            @Step(value = "Verify error message",
                expected = "Error message contains 'No resolvable bootstrap urls given in bootstrap.servers'"),
        },
        labels = {
            @Label(value = FLINK_SQL_RUNNER),
            @Label(value = FLINK),
        }
    )
    @Test
    @Tag(SMOKE)
    void testWrongConnectionInfo() {
        String namespace = "flink-wrong-connection";
        String flinkDeploymentName = namespace;

        // Create namespace
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                new NamespaceBuilder().withNewMetadata()
                    .withName(namespace)
                    .endMetadata()
                    .build());

        // Add flink RBAC
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                FlinkRBAC.getFlinkRbacResources(namespace)
                    .toArray(new HasMetadata[0]));

        // Deploy flink with not valid sql
        FlinkDeployment flink = FlinkDeploymentTemplate.defaultFlinkDeployment(namespace,
                flinkDeploymentName, List.of(TestStatements.getWrongConnectionSql()))
            .build();
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithoutWait(flink);

        // Check if no task is deployed and error is proper in flink deployment
        Wait.until("Flink deployment starts", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM, () ->
                !KubeResourceManager.getKubeClient()
                    .listPodsByPrefixInName(namespace, flinkDeploymentName)
                    .isEmpty());

        String podName = KubeResourceManager.getKubeClient()
            .listPodsByPrefixInName(namespace, flinkDeploymentName)
            .get(0)
            .getMetadata()
            .getName();

        Wait.until("Flink deployment contains error message", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM, () ->
                KubeResourceManager.getKubeClient()
                    .getLogsFromPod(namespace, podName)
                    .contains("No resolvable bootstrap urls given in bootstrap.servers"));
    }

    @TestDoc(
        description = @Desc("Test verifies that user can use FRocksDB as state backend"),
        steps = {
            @Step(value = "Create namespace, serviceaccount and roles for Flink", expected = "Resources created"),
            @Step(value = "Deploy Apicurio registry", expected = "Apicurio registry is up and running"),
            @Step(value = "Deploy Kafka my-cluster with scram-sha auth", expected = "Kafka is up and running"),
            @Step(value = "Create KafkaUser with scram-sha secret", expected = "KafkaUser created"),
            @Step(value = "Deploy strimzi-kafka-clients producer with payment data generator",
                expected = "Client job is created and data are sent to flink.payment.data topic"),
            @Step(value = "Create PVC for FlinkDeployment for FRocksDB", expected = "PVC is created"),
            @Step(value = "Deploy FlinkDeployment with sql which gets data from flink.payment.data topic filter " +
                "payment of type paypal and send data to flink.payment.paypal topic, for authentication is used " +
                "secret created by KafkaUser and this secret is passed into by secret interpolation. Flink is " +
                "configured to use FRocksDB as a state backend",
                expected = "FlinkDeployment is up and tasks are deployed and it sends filtered " +
                    "data into flink.payment.paypal topic, task manager deployed by FlinkDeployment uses " +
                    "FRockDB"),
            @Step(value = "Deploy strimzi-kafka-clients consumer as job and consume messages from" +
                "kafka topic flink.payment.paypal",
                expected = "Consumer is deployed and it consumes messages"),
            @Step(value = "Verify that messages are present", expected = "Messages are present"),
            @Step(value = "Verify that taskmanager logs contains 'State backend loader loads the state " +
                "backend as EmbeddedRocksDBStateBackend'", expected = "Log message is present"),
        },
        labels = {
            @Label(value = FLINK_SQL_RUNNER),
            @Label(value = FLINK),
        }
    )
    @Test
    void testFRocksDbStateBackend() {
        String namespace = "flink-state-backend";
        String flinkDeploymentName = "flink-state-backend";
        String kafkaUser = "test-user";
        // Create namespace
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                new NamespaceBuilder().withNewMetadata()
                    .withName(namespace)
                    .endMetadata()
                    .build());

        // Add flink RBAC
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                FlinkRBAC.getFlinkRbacResources(namespace)
                    .toArray(new HasMetadata[0]));

        // Create kafka
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                KafkaNodePoolTemplate.defaultKafkaNodePoolJbod(namespace, "dual-role",
                        3, kafkaClusterName, List.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER))
                    .build());

        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                KafkaTemplate.defaultKafka(namespace, kafkaClusterName)
                    .editSpec()
                    .editKafka()
                    .withListeners(
                        new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withTls(false)
                            .withType(KafkaListenerType.INTERNAL)
                            .withPort((9092))
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build(),
                        new GenericKafkaListenerBuilder()
                            .withName("unsecure")
                            .withTls(false)
                            .withType(KafkaListenerType.INTERNAL)
                            .withPort((9094))
                            .build()
                    )
                    .endKafka()
                    .endSpec()
                    .build());

        // Create topic for ksql apicurio
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                ApicurioRegistryTemplate.apicurioKsqlTopic(namespace, kafkaClusterName, 3));

        // Create kafka scram sha user
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                KafkaUserTemplate.defaultKafkaUser(namespace, kafkaUser, kafkaClusterName)
                    .editSpec()
                    .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
                    .endSpec()
                    .build());

        String bootstrapServerAuth = KafkaType.kafkaClient()
            .inNamespace(namespace)
            .withName(kafkaClusterName)
            .get()
            .getStatus()
            .getListeners()
            .stream()
            .filter(l -> l.getName()
                .equals("plain"))
            .findFirst()
            .get()
            .getBootstrapServers();
        String bootstrapServerUnsecure = KafkaType.kafkaClient()
            .inNamespace(namespace)
            .withName(kafkaClusterName)
            .get()
            .getStatus()
            .getListeners()
            .stream()
            .filter(l -> l.getName()
                .equals("unsecure"))
            .findFirst()
            .get()
            .getBootstrapServers();

        // Add apicurio
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                ApicurioRegistryTemplate.defaultApicurioRegistry("apicurio-registry", namespace,
                        bootstrapServerUnsecure)
                    .build());

        // Get user secret jaas configuration
        final String saslJaasConfigEncrypted = KubeResourceManager.getKubeClient()
            .getClient()
            .secrets()
            .inNamespace(namespace)
            .withName(kafkaUser)
            .get()
            .getData()
            .get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = TestUtils.decodeFromBase64(saslJaasConfigEncrypted);

        // Run internal producer and produce data
        String producerName = "kafka-producer";
        StrimziKafkaClients kafkaProducerClient = new StrimziKafkaClientsBuilder()
            .withProducerName(producerName)
            .withNamespaceName(namespace)
            .withTopicName("flink.payment.data")
            .withBootstrapAddress(bootstrapServerAuth)
            .withMessageCount(10000)
            .withUsername(kafkaUser)
            .withDelayMs(10)
            .withMessageTemplate("payment_fiat")
            .withAdditionalConfig(
                StrimziClientUtils.getApicurioAdditionalProperties(AvroKafkaSerializer.class.getName(),
                    "http://apicurio-registry-service." + namespace + ".svc:8080/apis/registry/v2") + "\n"
                    + "sasl.mechanism=SCRAM-SHA-512\n"
                    + "security.protocol=SASL_PLAINTEXT\n"
                    + "sasl.jaas.config=" + saslJaasConfigDecrypted
            )
            .build();

        KubeResourceManager.getInstance()
            .createResourceWithWait(
                kafkaProducerClient.producerStrimzi()
            );

        String registryUrl = "http://apicurio-registry-service." + namespace + ".svc:8080/apis/ccompat/v6";

        // Create PVC for flink
        PersistentVolumeClaim flinkPVC = FlinkDeploymentTemplate
            .getFlinkPVC(namespace, "flink-state-backend")
            .build();
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(flinkPVC);

        // Deploy flink with test filter sql statement which filter to specific topic only payment type paypal
        // Modify flink default deployment with state backend and pvc configuration
        FlinkDeployment flink = FlinkDeploymentTemplate.defaultFlinkDeployment(namespace,
                flinkDeploymentName, List.of(TestStatements.getTestFlinkFilter(
                    bootstrapServerAuth, registryUrl, kafkaUser, namespace)))
            .editSpec()
            .addToFlinkConfiguration(
                Map.of(
                    "execution.checkpointing.interval", "60000",
                    "execution.checkpointing.snapshot-compression", "true",
                    "kubernetes.operator.job.restart.failed", "true",
                    "state.backend.rocksdb.compression.per.level_FLINK_JIRA", "SNAPPY_COMPRESSION",
                    "state.backend", "rocksdb",
                    "state.checkpoints.dir", "file:///flink-state-store/checkpoints",
                    "state.savepoints.dir", "file:///flink-state-store/savepoints"
                )
            )
            .editPodTemplate()
            .editOrNewSpec()
            .editFirstContainer()
            .addNewVolumeMount()
            .withMountPath("/flink-state-store")
            .withName("flink-state-store")
            .endFlinkdeploymentspecVolumeMount()
            .endFlinkdeploymentspecContainer()
            .addNewVolume()
            .withName("flink-state-store")
            .withNewPersistentVolumeClaim()
            .withClaimName(flinkPVC.getMetadata()
                .getName())
            .endFlinkdeploymentspecPersistentVolumeClaim()
            .endFlinkdeploymentspecVolume()
            .endFlinkdeploymentspecSpec()
            .endPodTemplate()
            .endSpec()
            .build();
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(flink);

        JobUtils.waitForJobSuccess(namespace, kafkaProducerClient.getProducerName(),
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM);

        //Check task manager log for presence rocksbd configuration
        Wait.until("Task manager contains info about rocksdb", TestFrameConstants.GLOBAL_POLL_INTERVAL_LONG,
            TestFrameConstants.GLOBAL_TIMEOUT, () -> {
                List<Pod> taskManagerPods = KubeResourceManager.getKubeClient()
                    .listPodsByPrefixInName(namespace, flinkDeploymentName + "-taskmanager");
                for (Pod p : taskManagerPods) {
                    return KubeResourceManager.getKubeClient()
                        .getLogsFromPod(namespace, p.getMetadata()
                            .getName())
                        .contains("State backend loader loads the state backend as EmbeddedRocksDBStateBackend");
                }
                return false;
            });

        // Run consumer and check if data are filtered
        String consumerName = "kafka-consumer";
        StrimziKafkaClients kafkaConsumerClient = new StrimziKafkaClientsBuilder()
            .withConsumerName(consumerName)
            .withNamespaceName(namespace)
            .withTopicName("flink.payment.paypal")
            .withBootstrapAddress(bootstrapServerAuth)
            .withMessageCount(10)
            .withAdditionalConfig(
                "sasl.mechanism=SCRAM-SHA-512\n" +
                    "security.protocol=SASL_PLAINTEXT\n" +
                    "sasl.jaas.config=" + saslJaasConfigDecrypted
            )
            .withConsumerGroup("flink-filter-test-group")
            .build();

        KubeResourceManager.getInstance()
            .createResourceWithWait(
                kafkaConsumerClient.consumerStrimzi()
            );

        JobUtils.waitForJobSuccess(namespace, kafkaConsumerClient.getConsumerName(),
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM);
        String consumerPodName = KubeResourceManager.getKubeClient()
            .listPodsByPrefixInName(namespace, consumerName)
            .get(0)
            .getMetadata()
            .getName();
        String log = KubeResourceManager.getKubeClient()
            .getLogsFromPod(namespace, consumerPodName);
        assertTrue(log.contains("\"type\":\"paypal\""));
        assertFalse(log.contains("\"type\":\"creditCard\""));
    }

    @TestDoc(
        description = @Desc("Test verifies that user can use S3 as state backend"),
        steps = {
            @Step(value = "Create namespace, serviceaccount and roles for Flink", expected = "Resources created"),
            @Step(value = "Deploy Apicurio registry", expected = "Apicurio registry is up and running"),
            @Step(value = "Deploy Kafka my-cluster with scram-sha auth", expected = "Kafka is up and running"),
            @Step(value = "Create KafkaUser with scram-sha secret", expected = "KafkaUser created"),
            @Step(value = "Deploy strimzi-kafka-clients producer with payment data generator",
                expected = "Client job is created and data are sent to flink.payment.data topic"),
            @Step(value = "Deploy Minio for S3 service", expected = "Minio is up and running"),
            @Step(value = "Deploy FlinkDeployment with sql which gets data from flink.payment.data topic filter " +
                "payment of type paypal and send data to flink.payment.paypal topic, for authentication is used " +
                "secret created by KafkaUser and this secret is passed into by secret interpolation. Flink is " +
                "configured to use S3 as a state backend",
                expected = "FlinkDeployment is up and tasks are deployed and it sends filtered " +
                    "data into flink.payment.paypal topic, task manager deployed by FlinkDeployment uses " +
                    "S3"),
            @Step(value = "Deploy strimzi-kafka-clients consumer as job and consume messages from" +
                "kafka topic flink.payment.paypal",
                expected = "Consumer is deployed and it consumes messages"),
            @Step(value = "Verify that messages are present", expected = "Messages are present"),
            @Step(value = "Verify that taskmanager logs contains 'State backend loader loads the state " +
                "backend as HashMapStateBackend'", expected = "Log message is present"),
            @Step(value = "Verify that Minio contains some data from Flink", expected = "Flink bucket is not empty")
        },
        labels = {
            @Label(value = FLINK_SQL_RUNNER),
            @Label(value = FLINK),
        }
    )
    @Test
    void testS3StateBackend() {
        String namespace = "flink-s3-state-backend";
        String flinkDeploymentName = "flink-state-backend";
        String kafkaUser = "test-user";
        String bucketName = "flink-bucket";

        // Create namespace
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                new NamespaceBuilder().withNewMetadata()
                    .withName(namespace)
                    .endMetadata()
                    .build());

        // Add flink RBAC
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                FlinkRBAC.getFlinkRbacResources(namespace)
                    .toArray(new HasMetadata[0]));

        // Create kafka
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                KafkaNodePoolTemplate.defaultKafkaNodePoolJbod(namespace, "dual-role",
                        3, kafkaClusterName, List.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER))
                    .build());

        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                KafkaTemplate.defaultKafka(namespace, kafkaClusterName)
                    .editSpec()
                    .editKafka()
                    .withListeners(
                        new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withTls(false)
                            .withType(KafkaListenerType.INTERNAL)
                            .withPort((9092))
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build(),
                        new GenericKafkaListenerBuilder()
                            .withName("unsecure")
                            .withTls(false)
                            .withType(KafkaListenerType.INTERNAL)
                            .withPort((9094))
                            .build()
                    )
                    .endKafka()
                    .endSpec()
                    .build());

        // Create topic for ksql apicurio
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                ApicurioRegistryTemplate.apicurioKsqlTopic(namespace, kafkaClusterName, 3));

        // Create kafka scram sha user
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                KafkaUserTemplate.defaultKafkaUser(namespace, kafkaUser, kafkaClusterName)
                    .editSpec()
                    .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
                    .endSpec()
                    .build());

        String bootstrapServerAuth = KafkaType.kafkaClient()
            .inNamespace(namespace)
            .withName(kafkaClusterName)
            .get()
            .getStatus()
            .getListeners()
            .stream()
            .filter(l -> l.getName()
                .equals("plain"))
            .findFirst()
            .get()
            .getBootstrapServers();
        String bootstrapServerUnsecure = KafkaType.kafkaClient()
            .inNamespace(namespace)
            .withName(kafkaClusterName)
            .get()
            .getStatus()
            .getListeners()
            .stream()
            .filter(l -> l.getName()
                .equals("unsecure"))
            .findFirst()
            .get()
            .getBootstrapServers();

        // Add apicurio
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(
                ApicurioRegistryTemplate.defaultApicurioRegistry("apicurio-registry", namespace,
                        bootstrapServerUnsecure)
                    .build());

        // Get user secret jaas configuration
        final String saslJaasConfigEncrypted = KubeResourceManager.getKubeClient()
            .getClient()
            .secrets()
            .inNamespace(namespace)
            .withName(kafkaUser)
            .get()
            .getData()
            .get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = TestUtils.decodeFromBase64(saslJaasConfigEncrypted);

        // Run internal producer and produce data
        String producerName = "kafka-producer";
        StrimziKafkaClients kafkaProducerClient = new StrimziKafkaClientsBuilder()
            .withProducerName(producerName)
            .withNamespaceName(namespace)
            .withTopicName("flink.payment.data")
            .withBootstrapAddress(bootstrapServerAuth)
            .withMessageCount(30000)
            .withUsername(kafkaUser)
            .withDelayMs(1)
            .withMessageTemplate("payment_fiat")
            .withAdditionalConfig(
                StrimziClientUtils.getApicurioAdditionalProperties(AvroKafkaSerializer.class.getName(),
                    "http://apicurio-registry-service." + namespace + ".svc:8080/apis/registry/v2") + "\n"
                    + "sasl.mechanism=SCRAM-SHA-512\n"
                    + "security.protocol=SASL_PLAINTEXT\n"
                    + "sasl.jaas.config=" + saslJaasConfigDecrypted
            )
            .build();

        KubeResourceManager.getInstance()
            .createResourceWithWait(
                kafkaProducerClient.producerStrimzi()
            );

        String registryUrl = "http://apicurio-registry-service." + namespace + ".svc:8080/apis/ccompat/v6";

        // Add Minio
        SetupMinio.deployMinio(namespace);
        SetupMinio.createBucket(namespace, bucketName);

        // Deploy flink with test filter sql statement which filter to specific topic only payment type paypal
        // Modify flink default deployment with state backend and pvc configuration
        HashMap<String, String> flinkConfig = new HashMap();
        flinkConfig.put("execution.checkpointing.interval", "10000");
        flinkConfig.put("execution.checkpointing.snapshot-compression", "true");
        flinkConfig.put("kubernetes.operator.job.restart.failed", "true");
//        flinkConfig.put("state.backend.rocksdb.compression.per.level_FLINK_JIRA", "SNAPPY_COMPRESSION");
        // rocksdb can be used as a state backend but the location is referenced in s3 instead on local pvc
//        flinkConfig.put("state.backend.type", "rocksdb");
        flinkConfig.put("state.backend", "filesystem");
        flinkConfig.put("state.checkpoints.dir", "s3://" + bucketName + "/" + SetupMinio.MINIO + ":" + SetupMinio.MINIO_PORT);
        flinkConfig.put("state.savepoints.dir", "s3://" + bucketName + "/" + SetupMinio.MINIO + ":" + SetupMinio.MINIO_PORT);
        // Currently Minio is deployed only in HTTP mode so we need to specify http in the url
        flinkConfig.put("s3.endpoint", "http://" + SetupMinio.MINIO + ":" + SetupMinio.MINIO_PORT);
        // This should be set to make sure Flink will properly work with Minio
        flinkConfig.put("s3.path.style.access", "true");
        flinkConfig.put("s3.access-key", SetupMinio.ADMIN_CREDS);
        flinkConfig.put("s3.secret-key", SetupMinio.ADMIN_CREDS);

        FlinkDeployment flink = FlinkDeploymentTemplate.defaultFlinkDeployment(namespace,
                flinkDeploymentName, List.of(TestStatements.getTestFlinkFilter(
                    bootstrapServerAuth, registryUrl, kafkaUser, namespace)))
            .editSpec()
            .addToFlinkConfiguration(
                flinkConfig
            )
            .editPodTemplate()
            .editOrNewSpec()
            .editFirstContainer()
            .endFlinkdeploymentspecContainer()
            .endFlinkdeploymentspecSpec()
            .endPodTemplate()
            .endSpec()
            .build();
        KubeResourceManager.getInstance()
            .createOrUpdateResourceWithWait(flink);

        JobUtils.waitForJobSuccess(namespace, kafkaProducerClient.getProducerName(),
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM);

        //Check task manager log for presence rocksbd configuration
        Wait.until("Task manager contains info about state.backend", TestFrameConstants.GLOBAL_POLL_INTERVAL_LONG,
            TestFrameConstants.GLOBAL_TIMEOUT, () -> {
                List<Pod> taskManagerPods = KubeResourceManager.getKubeClient()
                    .listPodsByPrefixInName(namespace, flinkDeploymentName + "-taskmanager");
                for (Pod p : taskManagerPods) {
                    return KubeResourceManager.getKubeClient()
                        .getLogsFromPod(namespace, p.getMetadata()
                            .getName())
//                        .contains("State backend loader loads the state backend as EmbeddedRocksDBStateBackend");
                        .contains("State backend loader loads the state backend as HashMapStateBackend");
                }
                return false;
            });

        // TODO remove
        String consumerName1 = "pepa-consumer";
        StrimziKafkaClients kafkaConsumerClient1 = new StrimziKafkaClientsBuilder()
            .withConsumerName(consumerName1)
            .withNamespaceName(namespace)
            .withTopicName("flink.payment.data")
            .withBootstrapAddress(bootstrapServerAuth)
            .withMessageCount(100)
            .withAdditionalConfig(
                "sasl.mechanism=SCRAM-SHA-512\n" +
                    "security.protocol=SASL_PLAINTEXT\n" +
                    "sasl.jaas.config=" + saslJaasConfigDecrypted
            )
            .withConsumerGroup("flink-filter-test-group")
            .build();

        // Run consumer and check if data are filtered
        String consumerName = "kafka-consumer";
        StrimziKafkaClients kafkaConsumerClient = new StrimziKafkaClientsBuilder()
            .withConsumerName(consumerName)
            .withNamespaceName(namespace)
            .withTopicName("flink.payment.paypal")
            .withBootstrapAddress(bootstrapServerAuth)
            .withMessageCount(100)
            .withAdditionalConfig(
                "sasl.mechanism=SCRAM-SHA-512\n" +
                    "security.protocol=SASL_PLAINTEXT\n" +
                    "sasl.jaas.config=" + saslJaasConfigDecrypted
            )
            .withConsumerGroup("flink-filter-test-group")
            .build();

        KubeResourceManager.getInstance()
            .createResourceWithWait(
                kafkaConsumerClient.consumerStrimzi()
            );

        JobUtils.waitForJobSuccess(namespace, kafkaConsumerClient.getConsumerName(),
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM);
        String consumerPodName = KubeResourceManager.getKubeClient()
            .listPodsByPrefixInName(namespace, consumerName)
            .get(0)
            .getMetadata()
            .getName();
        String log = KubeResourceManager.getKubeClient()
            .getLogsFromPod(namespace, consumerPodName);
        assertTrue(log.contains("\"type\":\"paypal\""));
        assertFalse(log.contains("\"type\":\"creditCard\""));

        MinioUtils.waitForDataInMinio(namespace, bucketName);
    }
}
