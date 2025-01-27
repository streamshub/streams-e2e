/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operators.manifests;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.skodjob.testframe.TestFrameConstants;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.wait.Wait;
import io.streams.constants.TestConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Installer of Debezium operator using yaml manifests files
 */
public class DebeziumManifestInstaller {

    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumManifestInstaller.class);
    private static Path filesDir = TestConstants.YAML_MANIFEST_PATH.resolve("debezium");

    /**
     * Deployment name for Debezium operator
     */
    public static final String DEPLOYMENT_NAME = "debezium-operator";

    /**
     * Operator namespace
     */
    public static final String OPERATOR_NS = DEPLOYMENT_NAME;

    /**
     * Installs operator from yaml manifests files
     *
     * @return async waiter for deployment complete
     * @throws IOException io exception
     */
    public static CompletableFuture<Void> install() throws IOException {
        LOGGER.info("Installing Debezium into namespace: {}", OPERATOR_NS);

        Namespace namespace = new NamespaceBuilder().withNewMetadata().withName(OPERATOR_NS).endMetadata().build();
        KubeResourceManager.get().createOrUpdateResourceWithWait(namespace);

        // modify namespaces, convert rolebinding to clusterrolebindings, update deployment if needed

        List<HasMetadata> debeziumResources = new LinkedList<>();
        Files.list(filesDir).sorted().forEach(file -> {
            try {
                debeziumResources.addAll(KubeResourceManager.get().readResourcesFromFile(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        debeziumResources.forEach(res -> {
            if (res instanceof Namespaced) {
                res.getMetadata().setNamespace(OPERATOR_NS);
            }
            if (res instanceof ClusterRoleBinding crb) {
                crb.getSubjects().forEach(sbj -> sbj.setNamespace(OPERATOR_NS));
                crb.getMetadata().setName(crb.getMetadata().getName() + "." + OPERATOR_NS);
            }
            KubeResourceManager.get().createOrUpdateResourceWithoutWait(res);
        });
        LOGGER.info("Debezium operator installed to namespace: {}", OPERATOR_NS);
        return Wait.untilAsync("Debezium operator readiness", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT, DebeziumManifestInstaller::isReady);
    }

    private static boolean isReady() {
        if (KubeResourceManager.get().kubeClient().getClient().apps()
            .deployments().inNamespace(OPERATOR_NS).withName(DEPLOYMENT_NAME).isReady()) {
            LOGGER.info("Debezium Operator {}/{} is ready", OPERATOR_NS, DEPLOYMENT_NAME);
            return true;
        } else {
            return false;
        }
    }
}
