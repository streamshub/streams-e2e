/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.e2e.flink.sql;

import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
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
import io.streams.operands.strimzi.resources.KafkaType;
import io.streams.operands.strimzi.templates.KafkaNodePoolTemplate;
import io.streams.operands.strimzi.templates.KafkaTemplate;
import io.streams.operands.strimzi.templates.KafkaUserTemplate;
import io.streams.operators.manifests.ApicurioRegistryManifestInstaller;
import io.streams.operators.manifests.CertManagerManifestInstaller;
import io.streams.operators.manifests.FlinkManifestInstaller;
import io.streams.operators.manifests.StrimziManifestInstaller;
import io.streams.sql.TestStatements;
import io.streams.utils.StrimziClientUtils;
import io.streams.utils.TestUtils;
import io.streams.utils.kube.JobUtils;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthentication;
import org.apache.flink.v1beta1.FlinkDeployment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.streams.constants.TestTags.FLINK;
import static io.streams.constants.TestTags.FLINK_SQL_RUNNER;
import static io.streams.constants.TestTags.SMOKE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(FLINK)
@Tag(FLINK_SQL_RUNNER)
public class SqlJobRunnerST extends Abstract {

    @BeforeAll
    void prepareOperators() throws IOException {
        CompletableFuture.allOf(
            CertManagerManifestInstaller.install()).join();

        CompletableFuture.allOf(
            StrimziManifestInstaller.install(),
            ApicurioRegistryManifestInstaller.install(),
            FlinkManifestInstaller.install()).join();
    }

    @Test
    @Tag(SMOKE)
    void testFlinkSqlRunnerSimpleFilter() {
        String namespace = "flink-filter";
        String kafkaUser = "test-user";
        // Create namespace
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            new NamespaceBuilder().withNewMetadata().withName(namespace).endMetadata().build());

        // Add apicurio
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            ApicurioRegistryTemplate.defaultApicurioRegistry("apicurio-registry", namespace).build());

        // Add flink RBAC
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            FlinkRBAC.getFlinkRbacResources(namespace).toArray(new HasMetadata[0]));

        // Create kafka
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            KafkaNodePoolTemplate.defaultKafkaNodePoolJbod(namespace, "dual-role",
                3, "my-cluster", List.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)).build());

        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            KafkaTemplate.defaultKafka(namespace, "my-cluster")
                .editSpec()
                .editKafka()
                .editFirstListener()
                .withAuth(new KafkaListenerAuthenticationScramSha512())
                .endListener()
                .endKafka()
                .endSpec()
                .build());

        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            KafkaUserTemplate.defaultKafkaUser(namespace, "test-user", "my-cluster")
                .editSpec()
                .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
                .endSpec()
                .build());

        final String saslJaasConfigEncrypted = KubeResourceManager.getKubeClient().getClient().secrets()
            .inNamespace(namespace).withName(kafkaUser).get().getData().get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = TestUtils.decodeFromBase64(saslJaasConfigEncrypted);

        // Run internal producer and produce data
        String bootstrapServer = KafkaType.kafkaClient().inNamespace(namespace).withName("my-cluster").get()
            .getStatus().getListeners().get(0).getBootstrapServers();

        String producerName = "kafka-producer";
        StrimziKafkaClients kafkaProducerClient = new StrimziKafkaClientsBuilder()
            .withProducerName(producerName)
            .withNamespaceName(namespace)
            .withTopicName("flink.payment.data")
            .withBootstrapAddress(bootstrapServer)
            .withMessageCount(10000)
            .withUsername("test-user")
            .withDelayMs(10)
            .withMessageTemplate("payment_fiat")
            .withAdditionalConfig(
                StrimziClientUtils.getApicurioAdditionalProperties(AvroKafkaSerializer.class.getName(),
                    "http://apicurio-registry-service.flink-filter.svc:8080/apis/registry/v2") + "\n"
                    + "sasl.mechanism=SCRAM-SHA-512\n"
                    + "security.protocol=SASL_PLAINTEXT\n"
                    + "sasl.jaas.config=" + saslJaasConfigDecrypted
            )
            .build();

        KubeResourceManager.getInstance().createResourceWithWait(
            kafkaProducerClient.producerStrimzi()
        );

        String registryUrl = "http://apicurio-registry-service.flink-filter.svc:8080/apis/ccompat/v6";

        FlinkDeployment flink = FlinkDeploymentTemplate.defaultFlinkDeployment(namespace,
                "flink-filter", List.of(TestStatements.getTestFlinkFilter(
                    bootstrapServer, registryUrl, kafkaUser, namespace)))
            .build();
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(flink);

        JobUtils.waitForJobSuccess(namespace, kafkaProducerClient.getProducerName(),
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM);

        // Run consumer and check if data are filtered
        String consumerName = "kafka-consumer";
        StrimziKafkaClients kafkaConsumerClient = new StrimziKafkaClientsBuilder()
            .withConsumerName(consumerName)
            .withNamespaceName(namespace)
            .withTopicName("flink.payment.paypal")
            .withBootstrapAddress(bootstrapServer)
            .withMessageCount(10)
            .withAdditionalConfig(
                "sasl.mechanism=SCRAM-SHA-512\n" +
                    "security.protocol=SASL_PLAINTEXT\n" +
                    "sasl.jaas.config=" + saslJaasConfigDecrypted
            )
            .withConsumerGroup("flink-filter-test-group").build();

        KubeResourceManager.getInstance().createResourceWithWait(
            kafkaConsumerClient.consumerStrimzi()
        );

        JobUtils.waitForJobSuccess(namespace, kafkaConsumerClient.getConsumerName(),
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM);
        String consumerPodName = KubeResourceManager.getKubeClient().listPodsByPrefixInName(namespace, consumerName)
            .get(0).getMetadata().getName();
        String log = KubeResourceManager.getKubeClient().getLogsFromPod(namespace, consumerPodName);
        assertTrue(log.contains("\"type\":\"paypal\""));
        assertFalse(log.contains("\"type\":\"creditCard\""));
    }

    @Test
    @Tag(SMOKE)
    void testBadSqlStatement() {
        String namespace = "flink-bad-sql";
        String flinkDeploymentName = namespace;

        // Create namespace
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            new NamespaceBuilder().withNewMetadata().withName(namespace).endMetadata().build());

        // Add flink RBAC
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            FlinkRBAC.getFlinkRbacResources(namespace).toArray(new HasMetadata[0]));

        // Deploy flink with not valid sql
        FlinkDeployment flink = FlinkDeploymentTemplate.defaultFlinkDeployment(namespace,
                flinkDeploymentName, List.of("blah blah"))
            .build();
        KubeResourceManager.getInstance().createOrUpdateResourceWithoutWait(flink);

        // Check if no task is deployed and error is proper in flink deployment
        Wait.until("Flink deployment fail", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM, () ->
                new FlinkDeploymentType().getClient().inNamespace(namespace).withName(flinkDeploymentName)
                    .get().getStatus().getError().contains("DeploymentFailedException"));

        String podName = KubeResourceManager.getKubeClient().listPodsByPrefixInName(namespace, flinkDeploymentName)
            .get(0).getMetadata().getName();

        Wait.until("Flink deployment contains error message", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM, () ->
                KubeResourceManager.getKubeClient()
                    .getLogsFromPod(namespace, podName).contains("SQL parse failed"));
    }
}
