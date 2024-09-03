/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operators.manifests;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Installer of Cert-manager using yaml manifests files
 */
public class CertManagerManifestInstaller {

    private static final Logger LOGGER = LoggerFactory.getLogger(CertManagerManifestInstaller.class);
    private static Path filesDir = TestConstants.YAML_MANIFEST_PATH.resolve("cert-manager");

    /**
     * Deployment name for Cert-manager
     */
    public static final String DEPLOYMENT_NAME = "cert-manager";
    public static final String WEBHOOK_DEPLOYMENT_NAME = "cert-manager-webhook";
    public static final String CA_INJECTION_DEPLOYMENT_NAME = "cert-manager-cainjector";

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
        LOGGER.info("Installing Cert-manager into namespace: {}", OPERATOR_NS);

        Namespace namespace = new NamespaceBuilder().withNewMetadata().withName(OPERATOR_NS).endMetadata().build();
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(namespace);

        List<HasMetadata> certManagerResources = new LinkedList<>();
        Files.list(filesDir).sorted().forEach(file -> {
            try {
                certManagerResources.addAll(KubeResourceManager.getInstance().readResourcesFromFile(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        certManagerResources.forEach(res -> {
            if (!Objects.equals(res.getMetadata().getNamespace(), "kube-system")) {
                res.getMetadata().setNamespace(OPERATOR_NS);
            }

            if (res instanceof ClusterRoleBinding crb) {
                crb.getSubjects().forEach(sbj -> sbj.setNamespace(OPERATOR_NS));
                crb.getMetadata().setName(crb.getMetadata().getName() + "." + OPERATOR_NS);
            } else if (res instanceof RoleBinding rb) {
                rb.getSubjects().forEach(sbj -> sbj.setNamespace(OPERATOR_NS));
            }
            KubeResourceManager.getInstance().createOrUpdateResourceWithoutWait(res);
        });
        LOGGER.info("Cert-manager installed to namespace: {}", OPERATOR_NS);
        return Wait.untilAsync("Cert-manager readiness", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT, CertManagerManifestInstaller::isReady);
    }

    private static boolean isReady() {
        if (KubeResourceManager.getKubeClient().getClient().apps()
            .deployments().inNamespace(OPERATOR_NS).withName(DEPLOYMENT_NAME).isReady() &&
            KubeResourceManager.getKubeClient().getClient().apps()
                .deployments().inNamespace(OPERATOR_NS).withName(WEBHOOK_DEPLOYMENT_NAME).isReady() &&
            KubeResourceManager.getKubeClient().getClient().apps()
                .deployments().inNamespace(OPERATOR_NS).withName(CA_INJECTION_DEPLOYMENT_NAME).isReady()) {
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(60));
            LOGGER.info("Cert-manager {}/{} is ready", OPERATOR_NS, DEPLOYMENT_NAME);
            return true;
        } else {
            return false;
        }
    }
}
