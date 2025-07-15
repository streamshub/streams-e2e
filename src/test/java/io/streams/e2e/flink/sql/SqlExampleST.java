/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.e2e.flink.sql;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.qameta.allure.Allure;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.TestFrameConstants;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.utils.JobUtils;
import io.streams.clients.kafka.StrimziKafkaClients;
import io.streams.clients.kafka.StrimziKafkaClientsBuilder;
import io.streams.constants.TestConstants;
import io.streams.e2e.Abstract;
import io.streams.operands.apicurio.templates.ApicurioRegistryTemplate;
import io.streams.operands.flink.templates.FlinkDeploymentTemplate;
import io.streams.operands.flink.templates.FlinkRBAC;
import io.streams.operands.strimzi.resources.KafkaType;
import io.streams.operands.strimzi.templates.KafkaNodePoolTemplate;
import io.streams.operands.strimzi.templates.KafkaTemplate;
import io.streams.operators.InstallableOperator;
import io.streams.operators.OperatorInstaller;
import io.streams.sql.TestStatements;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import org.apache.flink.v1beta1.FlinkDeployment;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.streams.constants.TestTags.FLINK;
import static io.streams.constants.TestTags.FLINK_SQL_EXAMPLE;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(FLINK)
@Tag(FLINK_SQL_EXAMPLE)
@SuiteDoc(
    description = @Desc("This test suite verifies that flink-sql-example works correctly"),
    beforeTestSteps = {
        @Step(value = "Deploy the Strimzi Kafka operator", expected = "Strimzi operator is deployed"),
        @Step(value = "Deploy the Flink Kubernetes operator", expected = "Flink operator is deployed"),
        @Step(value = "Deploy the Apicurio operator", expected = "Apicurio operator is deployed"),
        @Step(value = "Deploy the cert-manager operator", expected = "Cert-manager operator is deployed")
    },
    labels = {
        @Label(value = FLINK_SQL_EXAMPLE),
        @Label(value = FLINK),
    }
)
public class SqlExampleST extends Abstract {

    String namespace = "flink";
    Path exampleFiles = TestConstants.YAML_MANIFEST_PATH.resolve("examples").resolve("sql-example");

    @BeforeAll
    void prepareOperators() throws Exception {
        Allure.step("Install required operators", () -> OperatorInstaller.installRequiredOperators(
            InstallableOperator.FLINK,
            InstallableOperator.APICURIO,
            InstallableOperator.STRIMZI,
            InstallableOperator.CERT_MANAGER));
    }

    @TestDoc(
        description = @Desc("Test verifies that flink-sql-example recommended app " +
            "https://github.com/streamshub/flink-sql-examples/tree/main/recommendation-app works"),
        steps = {
            @Step(value = "Create namespace, serviceaccount and roles for Flink", expected = "Resources created"),
            @Step(value = "Deploy Apicurio registry", expected = "Apicurio registry is up and running"),
            @Step(value = "Deploy simple example Kafka my-cluster", expected = "Kafka is up and running"),
            @Step(value = "Deploy productInventory.csv as configmap", expected = "Configmap created"),
            @Step(value = "Deploy data-generator deployment", expected = "Deployment is up and running"),
            @Step(value = "Deploy FlinkDeployment from sql-example",
                expected = "FlinkDeployment is up and tasks are deployed and it sends filtered " +
                    "data into flink.recommended.products topic"),
            @Step(value = "Deploy strimzi-kafka-clients consumer as job and consume messages from" +
                "kafka topic flink.recommended.products",
                expected = "Consumer is deployed and it consumes messages"),
            @Step(value = "Verify that messages are present", expected = "Messages are present"),
        },
        labels = {
            @Label(value = FLINK_SQL_EXAMPLE),
            @Label(value = FLINK),
        }
    )
    @Test
    void testRecommendationApp() throws IOException {
        Allure.step("Prepare " + namespace + " namespace", () -> {
            // Create namespace
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                new NamespaceBuilder().withNewMetadata().withName(namespace).endMetadata().build());

            // Add flink RBAC
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                FlinkRBAC.getFlinkRbacResources(namespace).toArray(new HasMetadata[0]));
        });

        Allure.step("Deploy kafka", () -> {
            // Create kafka
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                KafkaNodePoolTemplate.defaultKafkaNodePoolJbod(namespace, "dual-role",
                    1, "my-cluster", List.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)).build());

            KubeResourceManager.get().createOrUpdateResourceWithWait(
                KafkaTemplate.defaultKafka(namespace, "my-cluster")
                    .editSpec()
                    .withCruiseControl(null)
                    .withKafkaExporter(null)
                    .editKafka()
                    .withConfig(Map.of(
                        "offsets.topic.replication.factor", 1,
                        "transaction.state.log.replication.factor", 1,
                        "transaction.state.log.min.isr", 1,
                        "default.replication.factor", 1,
                        "min.insync.replicas", 1
                    ))
                    .endKafka()
                    .endSpec()
                    .build());
        });

        String bootstrapServer = KafkaType.kafkaClient().inNamespace(namespace).withName("my-cluster").get()
            .getStatus().getListeners().stream().filter(l -> l.getName().equals("plain"))
            .findFirst().get().getBootstrapServers();

        Allure.step("Deploy apicurio registry", () -> {
            // Create topic for ksql apicurio
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                ApicurioRegistryTemplate.apicurioKsqlTopic(namespace, "my-cluster", 1));

            // Add apicurio
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                ApicurioRegistryTemplate.defaultApicurioRegistry("apicurio-registry", namespace,
                    bootstrapServer).build());
        });

        Allure.step("Deploy recommendation application", () -> {
            // Create configMap
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                new ConfigMapBuilder()
                    .withNewMetadata()
                    .withName("product-inventory")
                    .withNamespace(namespace)
                    .endMetadata()
                    .withData(
                        Collections.singletonMap("productInventory.csv",
                            Files.readString(exampleFiles.resolve("productInventory.csv"))))
                    .build());

            // Create data-app
            List<HasMetadata> dataApp = KubeResourceManager.get()
                .readResourcesFromFile(exampleFiles.resolve("data-generator.yaml"));
            dataApp.forEach(r -> r.getMetadata().setNamespace(namespace));
            dataApp.stream().filter(r -> r.getKind().equals("Deployment")).forEach(r -> {
                Deployment d = (Deployment) r;
                d.getSpec().getTemplate().getSpec().getContainers().get(0).setImagePullPolicy(TestConstants.ALWAYS_IMAGE_PULL_POLICY);
            });
            KubeResourceManager.get().createOrUpdateResourceWithWait(dataApp.toArray(new HasMetadata[0]));
        });

        // Deploy flink
        String registryUrl = "http://apicurio-registry-service.flink.svc:8080/apis/ccompat/v6";

        Allure.step("Deploy flink application", () -> {
            FlinkDeployment flinkApp = FlinkDeploymentTemplate.flinkExampleDeployment(namespace,
                "recommendation-app", List.of(TestStatements.getTestSqlExample(bootstrapServer, registryUrl))).build();
            KubeResourceManager.get().createOrUpdateResourceWithWait(flinkApp);
        });

        Allure.step("Deploy consumer and verify filtered messages", () -> {
            // Run internal consumer and check if topic contains messages
            String consumerName = "kafka-consumer";
            StrimziKafkaClients strimziKafkaClients = new StrimziKafkaClientsBuilder()
                .withConsumerName(consumerName)
                .withNamespaceName(namespace)
                .withTopicName("flink.recommended.products")
                .withBootstrapAddress(bootstrapServer)
                .withMessageCount(10)
                .withConsumerGroup("my-group").build();

            KubeResourceManager.get().createResourceWithWait(
                strimziKafkaClients.consumerStrimzi()
            );
            JobUtils.waitForJobSuccess(namespace, strimziKafkaClients.getConsumerName(),
                TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM);
            String consumerPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(namespace, consumerName)
                .get(0).getMetadata().getName();

            String log = KubeResourceManager.get().kubeClient().getLogsFromPod(namespace, consumerPodName);
            assertTrue(log.contains("user-"));
        });
    }
}
