package io.streams.e2e.flink.sql;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.skodjob.testframe.executor.ExecResult;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.streams.constants.TestConstants;
import io.streams.e2e.Abstract;
import io.streams.operands.apicurio.templates.ApicurioRegistryTemplate;
import io.streams.operands.flink.resoruces.FlinkDeploymentType;
import io.streams.operands.flink.templates.FlinkRBAC;
import io.streams.operands.strimzi.templates.KafkaNodePoolTemplate;
import io.streams.operands.strimzi.templates.KafkaTemplate;
import io.streams.operators.manifests.ApicurioRegistryManifestInstaller;
import io.streams.operators.manifests.CertManagerManifestInstaller;
import io.streams.operators.manifests.FlinkManifestInstaller;
import io.streams.operators.manifests.StrimziManifestInstaller;
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
import java.util.concurrent.CompletableFuture;

import static io.streams.constants.TestTags.SQL_EXAMPLE;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(SQL_EXAMPLE)
public class SqlExampleST extends Abstract {

    String namespace = "flink";
    Path exampleFiles = TestConstants.YAML_MANIFEST_PATH.resolve("examples").resolve("sql-example");

    @BeforeAll
    void prepareOperators() throws IOException {
        CompletableFuture.allOf(
            CertManagerManifestInstaller.install()).join();

        CompletableFuture.allOf(
            StrimziManifestInstaller.install(),
            FlinkManifestInstaller.install(),
            ApicurioRegistryManifestInstaller.install()).join();
    }

    @Test
    void testFlinkSqlExample() throws IOException {
        // Create namespace
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            new NamespaceBuilder().withNewMetadata().withName(namespace).endMetadata().build());

        // Add flink RBAC
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            FlinkRBAC.getFlinkRbacResources(namespace).toArray(new HasMetadata[0]));

        // Add apicurio
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            ApicurioRegistryTemplate.defaultApicurioRegistry("apicurio-registry", namespace).build());

        // Create kafka
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            KafkaNodePoolTemplate.defaultKafkaNodePoolJbod(namespace, "dual-role",
                1, "my-cluster", List.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)).build());

        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
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

        // Create configMap
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
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
        List<HasMetadata> dataApp = KubeResourceManager.getInstance()
            .readResourcesFromFile(exampleFiles.resolve("data-generator.yaml"));
        dataApp.forEach(r -> r.getMetadata().setNamespace(namespace));
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(dataApp.toArray(new HasMetadata[0]));

        // Deploy flink
        FlinkDeployment flinkApp = new FlinkDeploymentType().getClient()
            .load(exampleFiles.resolve("flink-deployment.yaml").toFile()).item();
        flinkApp.getMetadata().setNamespace(namespace);
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(flinkApp);

        // Run internal consumer and check if topic contains messages
        // TODO: Use strimzi test clients in future
        ExecResult res = KubeResourceManager.getKubeCmdClient().inNamespace(namespace)
            .execInPod("my-cluster-dual-role-0", "/bin/bash",
                "./bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092",
                "--topic", "flink.recommended.products", "--from-beginning", "--group", "test-1", "--max-messages", "10");

        assertTrue(res.out().contains("user-"));
    }
}
