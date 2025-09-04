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
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
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
 * Installer of Keyclaok operator using yaml manifests files
 */
public class KeycloakManifestInstaller {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeycloakManifestInstaller.class);
    private static Path filesDir = TestConstants.YAML_MANIFEST_PATH.resolve("keycloak");

    /**
     * Deployment name for Keycloak Registry operator
     */
    public static final String DEPLOYMENT_NAME = "keycloak-operator";

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
        LOGGER.info("Installing Keycloak into namespace: {}", OPERATOR_NS);

        Namespace namespace = new NamespaceBuilder().withNewMetadata().withName(OPERATOR_NS).endMetadata().build();
        KubeResourceManager.get().createOrUpdateResourceWithWait(namespace);

        List<HasMetadata> keycloakResources = new LinkedList<>();
        Files.list(filesDir).sorted().forEach(file -> {
            try {
                keycloakResources.addAll(KubeResourceManager.get().readResourcesFromFile(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        keycloakResources.forEach(res -> {
            if (res instanceof Namespaced) {
                res.getMetadata().setNamespace(OPERATOR_NS);
            }
            if (res instanceof ClusterRoleBinding crb) {
                crb.getSubjects().forEach(sbj -> sbj.setNamespace(OPERATOR_NS));
                crb.getMetadata().setName(crb.getMetadata().getName() + "." + OPERATOR_NS);
            } else if (res instanceof RoleBinding rb) {
                rb.getSubjects().forEach(sbj -> sbj.setNamespace(OPERATOR_NS));
            }
            KubeResourceManager.get().createOrUpdateResourceWithoutWait(res);
        });
        LOGGER.info("Keycloak operator installed to namespace: {}", OPERATOR_NS);
        return Wait.untilAsync("Keycloak operator readiness", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT, KeycloakManifestInstaller::isReady);
    }

    private static boolean isReady() {
        if (KubeResourceManager.get().kubeClient().getClient().apps()
            .deployments().inNamespace(OPERATOR_NS).withName(DEPLOYMENT_NAME).isReady()) {
            LOGGER.info("Keycloak operator {}/{} is ready", OPERATOR_NS, DEPLOYMENT_NAME);
            return true;
        } else {
            return false;
        }
    }
}
