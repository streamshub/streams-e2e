/*
 * Copyright Skodjob authors.
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
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Installer of strimzi operator using yaml manifests files
 */
public class StrimziManifestInstaller {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziManifestInstaller.class);
    private static Path filesDir = TestConstants.YAML_MANIFEST_PATH.resolve("strimzi-kafka-operator").resolve(
        "install").resolve("cluster-operator");

    /**
     * Deployment name for strimzi operator
     */
    public static final String DEPLOYMENT_NAME = "strimzi-cluster-operator";

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
        LOGGER.info("Installing Strimzi into namespace: {}", OPERATOR_NS);

        Namespace namespace = new NamespaceBuilder().withNewMetadata().withName(OPERATOR_NS).endMetadata().build();
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(namespace);

        // modify namespaces, convert rolebinding to clusterrolebindings, update deployment if needed
        String crbID = UUID.randomUUID().toString().substring(0, 5);

        List<HasMetadata> strimziResoruces = new LinkedList<>();
        Files.list(filesDir).sorted().forEach(file -> {
            try {
                strimziResoruces.addAll(KubeResourceManager.getInstance().readResourcesFromFile(file));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        strimziResoruces.forEach(res -> {
            if (res instanceof Namespaced) {
                res.getMetadata().setNamespace(OPERATOR_NS);
            }
            if (res instanceof ClusterRoleBinding) {
                ClusterRoleBinding crb = (ClusterRoleBinding) res;
                crb.getSubjects().forEach(sbj -> sbj.setNamespace(OPERATOR_NS));
                crb.getMetadata().setName(crb.getMetadata().getName() + "." + OPERATOR_NS);
            } else if (res instanceof RoleBinding) {
                RoleBinding rb = (RoleBinding) res;
                rb.getSubjects().forEach(sbj -> sbj.setNamespace(OPERATOR_NS));

                ClusterRoleBinding crb = new ClusterRoleBindingBuilder()
                    .withNewMetadata()
                    .withName(rb.getMetadata().getName() + "-all-ns-" + crbID)
                    .withAnnotations(rb.getMetadata().getAnnotations())
                    .withLabels(rb.getMetadata().getLabels())
                    .endMetadata()
                    .withRoleRef(rb.getRoleRef())
                    .withSubjects(rb.getSubjects())
                    .build();

                KubeResourceManager.getInstance().createOrUpdateResourceWithoutWait(crb);
            } else if (res instanceof Deployment && DEPLOYMENT_NAME.equals(res.getMetadata().getName())) {
                modifyDeployment((Deployment) res);
            }
            KubeResourceManager.getInstance().createOrUpdateResourceWithoutWait(res);
        });
        LOGGER.info("Strimzi operator installed to namespace: {}", OPERATOR_NS);
        return Wait.untilAsync("Strimzi operator readiness", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT, StrimziManifestInstaller::isReady);
    }

    private static void modifyDeployment(Deployment deployment) {
        Container container = deployment.getSpec().getTemplate().getSpec().getContainers().get(0);
        List<EnvVar> env = new ArrayList<>(container.getEnv() == null ? Collections.emptyList() : container.getEnv());
        env.removeIf(envVar -> envVar.getName().equals("STRIMZI_NAMESPACE"));
        env.add(new EnvVarBuilder().withName("STRIMZI_NAMESPACE").withValue("*").build());
        container.setEnv(env);
    }

    private static boolean isReady() {
        if (KubeResourceManager.getKubeClient().getClient().apps()
            .deployments().inNamespace(OPERATOR_NS).withName(DEPLOYMENT_NAME).isReady()) {
            LOGGER.info("Strimzi {}/{} is ready", OPERATOR_NS, DEPLOYMENT_NAME);
            return true;
        } else {
            return false;
        }
    }
}
