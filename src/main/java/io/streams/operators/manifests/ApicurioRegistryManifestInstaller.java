/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operators.manifests;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.api.model.apps.Deployment;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Installer of Apicurio Registry operator using yaml manifests files
 */
public class ApicurioRegistryManifestInstaller {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApicurioRegistryManifestInstaller.class);
    private static Path filesDir = TestConstants.YAML_MANIFEST_PATH.resolve("apicurio-registry");

    /**
     * Deployment name for Apicurio Registry operator
     */
    public static final String DEPLOYMENT_NAME = "apicurio-registry-operator";

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
        LOGGER.info("Installing Apicurio Registry into namespace: {}", OPERATOR_NS);

        Namespace namespace = new NamespaceBuilder().withNewMetadata().withName(OPERATOR_NS).endMetadata().build();
        KubeResourceManager.get().createOrUpdateResourceWithWait(namespace);

        List<HasMetadata> apicurioRegistryResources = new LinkedList<>();
        Files.list(filesDir).sorted().forEach(file -> {
            try {
                apicurioRegistryResources.addAll(KubeResourceManager.get().readResourcesFromFile(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        apicurioRegistryResources.forEach(res -> {
            if (res instanceof Namespaced) {
                res.getMetadata().setNamespace(OPERATOR_NS);
            }
            if (res instanceof ClusterRoleBinding crb) {
                crb.getSubjects().forEach(sbj -> sbj.setNamespace(OPERATOR_NS));
                crb.getMetadata().setName(crb.getMetadata().getName() + "." + OPERATOR_NS);
            } else if (res instanceof RoleBinding rb) {
                rb.getSubjects().forEach(sbj -> sbj.setNamespace(OPERATOR_NS));
            } else if (res instanceof Deployment dep && DEPLOYMENT_NAME.equals(res.getMetadata().getName())) {
                modifyDeployment(dep);
            }
            KubeResourceManager.get().createOrUpdateResourceWithoutWait(res);
        });
        LOGGER.info("Apicurio Registry operator installed to namespace: {}", OPERATOR_NS);
        return Wait.untilAsync("Apicurio Registry operator readiness", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT, ApicurioRegistryManifestInstaller::isReady);
    }

    private static void modifyDeployment(Deployment deployment) {
        Container container = deployment.getSpec().getTemplate().getSpec().getContainers().get(0);
        List<EnvVar> env = new ArrayList<>(container.getEnv() == null ? Collections.emptyList() : container.getEnv());
        env.removeIf(envVar -> envVar.getName().equals("WATCH_NAMESPACE"));
        env.add(new EnvVarBuilder().withName("WATCH_NAMESPACE").withValue("").build());
        container.setEnv(env);
    }

    private static boolean isReady() {
        if (KubeResourceManager.get().kubeClient().getClient().apps()
            .deployments().inNamespace(OPERATOR_NS).withName(DEPLOYMENT_NAME).isReady()) {
            LOGGER.info("Apicurio Registry operator {}/{} is ready", OPERATOR_NS, DEPLOYMENT_NAME);
            return true;
        } else {
            return false;
        }
    }
}
