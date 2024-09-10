/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.e2e.flink.sql;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.streams.e2e.Abstract;
import io.streams.operands.flink.templates.FlinkRBAC;
import io.streams.operands.strimzi.templates.KafkaNodePoolTemplate;
import io.streams.operands.strimzi.templates.KafkaTemplate;
import io.streams.operators.manifests.CertManagerManifestInstaller;
import io.streams.operators.manifests.FlinkManifestInstaller;
import io.streams.operators.manifests.StrimziManifestInstaller;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.streams.constants.TestTags.FLINK;
import static io.streams.constants.TestTags.SQL_RUNNER;

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
            FlinkManifestInstaller.install()).join();
    }

    @Test
    void testFlinkSqlRunnerSimpleFilter() {
        // Create namespace
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            new NamespaceBuilder().withNewMetadata().withName(namespace).endMetadata().build());

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

        // TODO: complete the test
    }
}
