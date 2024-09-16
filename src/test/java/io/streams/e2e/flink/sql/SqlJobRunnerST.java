/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.e2e.flink.sql;

import io.apicurio.registry.rest.v2.beans.IfExists;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.apicurio.registry.serde.config.IdOption;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.skodjob.testframe.TestFrameConstants;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.streams.clients.kafka.StrimziKafkaClients;
import io.streams.clients.kafka.StrimziKafkaClientsBuilder;
import io.streams.e2e.Abstract;
import io.streams.operands.apicurio.templates.ApicurioRegistryTemplate;
import io.streams.operands.flink.templates.FlinkDeploymentTemplate;
import io.streams.operands.flink.templates.FlinkRBAC;
import io.streams.operands.strimzi.resources.KafkaType;
import io.streams.operands.strimzi.templates.KafkaNodePoolTemplate;
import io.streams.operands.strimzi.templates.KafkaTemplate;
import io.streams.operators.manifests.ApicurioRegistryManifestInstaller;
import io.streams.operators.manifests.CertManagerManifestInstaller;
import io.streams.operators.manifests.FlinkManifestInstaller;
import io.streams.operators.manifests.StrimziManifestInstaller;
import io.streams.sql.TestStatements;
import io.streams.utils.kube.JobUtils;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import org.apache.flink.v1beta1.FlinkDeployment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.streams.constants.TestTags.FLINK;
import static io.streams.constants.TestTags.SQL_RUNNER;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(FLINK)
@Tag(SQL_RUNNER)
public class SqlJobRunnerST extends Abstract {

    String namespace = "flink-filter";

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
    void testFlinkSqlRunnerSimpleFilter() {
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
                .withCruiseControl(null)
                .withKafkaExporter(null)
                .endSpec()
                .build());

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
            .withDelayMs(10)
            .withMessageTemplate("payment_fiat")
            .withAdditionalConfig(
                new StringBuilder()
                    .append("value.serializer").append("=").append(AvroKafkaSerializer.class.getName())
                    .append(System.lineSeparator())
                    .append(SerdeConfig.REGISTRY_URL).append("=")
                    .append("http://apicurio-registry-service.flink-filter.svc:8080/apis/registry/v2")
                    .append(System.lineSeparator())
                    .append(SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER).append("=").append(Boolean.TRUE)
                    .append(System.lineSeparator())
                    .append(SerdeConfig.USE_ID).append("=").append(IdOption.contentId)
                    .append(System.lineSeparator())
                    .append(SerdeConfig.ENABLE_HEADERS).append("=").append(Boolean.FALSE)
                    .append(System.lineSeparator())
                    .append(SerdeConfig.AUTO_REGISTER_ARTIFACT).append("=").append(Boolean.TRUE)
                    .append(System.lineSeparator())
                    .append(SerdeConfig.AUTO_REGISTER_ARTIFACT_IF_EXISTS).append("=").append(IfExists.RETURN.name())
                    .toString()
            )
            .build();

        KubeResourceManager.getInstance().createResourceWithWait(
            kafkaProducerClient.producerStrimzi()
        );

        String registryUrl = "http://apicurio-registry-service.flink-filter.svc:8080/apis/ccompat/v6";

        FlinkDeployment flink = FlinkDeploymentTemplate.defaultFlinkDeployment(namespace,
                "flink-filter", List.of(TestStatements.getTestFlinkFilter(bootstrapServer, registryUrl)))
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
}
