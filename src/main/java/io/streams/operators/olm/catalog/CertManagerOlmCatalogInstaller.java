/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operators.olm.catalog;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1.OperatorGroupBuilder;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.Subscription;
import io.fabric8.openshift.api.model.operatorhub.v1alpha1.SubscriptionBuilder;
import io.skodjob.testframe.TestFrameConstants;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.resources.ResourceItem;
import io.skodjob.testframe.utils.PodUtils;
import io.skodjob.testframe.wait.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Installer cert-manager operator using olm from catalog
 */
public class CertManagerOlmCatalogInstaller {
    private static final Logger LOGGER = LoggerFactory.getLogger(CertManagerOlmCatalogInstaller.class);

    private static final String SUBSCRIPTION_NAME = "cert-manager";
    private static final String CERT_MANAGER_NS = "cert-manager";

    /**
     * Install cert-manager operator from catalog presented on cluster using OLM
     *
     * @param operatorName      name of operator
     * @param operatorNamespace where operator will be present
     * @param startingCsv       version of operator
     * @param channel           chanel
     * @param source            source name of catalog
     * @param catalogNs         source catalog namespace
     * @return wait future
     */
    @SuppressFBWarnings("BC_UNCONFIRMED_CAST_OF_RETURN_VALUE")
    public static CompletableFuture<Void> install(String operatorName, String operatorNamespace,
                                                  String startingCsv, String channel, String source, String catalogNs) {
        // Create ns for the operator
        Namespace ns = new NamespaceBuilder()
            .withNewMetadata()
            .withName(operatorNamespace)
            .endMetadata()
            .build();
        KubeResourceManager.get().createOrUpdateResourceWithWait(ns);
        // Create ns for the cert-manager instance
        Namespace ns2 = new NamespaceBuilder()
            .withNewMetadata()
            .withName("cert-manager")
            .endMetadata()
            .build();
        KubeResourceManager.get().createOrUpdateResourceWithWait(ns2);
        //Create operator group for the operator
        if (KubeResourceManager.get().kubeClient().getOpenShiftClient().operatorHub().operatorGroups()
            .inNamespace(operatorNamespace).list().getItems().isEmpty()) {
            OperatorGroupBuilder operatorGroup = new OperatorGroupBuilder()
                .editOrNewMetadata()
                .withName("streams-e2e-operator-group")
                .withNamespace(operatorNamespace)
                .endMetadata()
                .withNewSpec()
                .addToTargetNamespaces(operatorNamespace)
                .endSpec();
            KubeResourceManager.get().createResourceWithoutWait(operatorGroup.build());
        } else {
            LOGGER.info("OperatorGroup is already exists.");
        }

        Subscription subscription = new SubscriptionBuilder()
            .editOrNewMetadata()
            .withName(SUBSCRIPTION_NAME)
            .withNamespace(operatorNamespace)
            .endMetadata()
            .editOrNewSpec()
            .withName(operatorName)
            .withChannel(channel)
            .withStartingCSV(startingCsv)
            .withSource(source)
            .withSourceNamespace(catalogNs)
            .withInstallPlanApproval("Automatic")
            .editOrNewConfig()
            .endConfig()
            .endSpec()
            .build();

        KubeResourceManager.get().pushToStack(new ResourceItem<>(() -> cleanClusterRoleBindings(), null));
        KubeResourceManager.get().pushToStack(new ResourceItem<>(() -> cleanClusterRole(), null));
        KubeResourceManager.get().pushToStack(new ResourceItem<>(() -> cleanValidationWebhook(), null));
        KubeResourceManager.get().createOrUpdateResourceWithoutWait(subscription);
        return Wait.untilAsync(operatorName + " is ready", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT, () -> isOperatorReady(CERT_MANAGER_NS));
    }

    @SuppressFBWarnings("REC_CATCH_EXCEPTION")
    private static boolean isOperatorReady(String ns) {
        try {
            PodUtils.waitForPodsReadyWithRestart(ns, new LabelSelectorBuilder()
                .withMatchLabels(Map.of("app.kubernetes.io/instance", "cert-manager")).build(), 3, true);
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(120));
            LOGGER.info("Cert-manager operator in namespace {} is ready", ns);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    private static void cleanClusterRoleBindings() {
        KubeResourceManager.get().kubeClient().getClient().rbac().clusterRoleBindings()
            .withLabel("app.kubernetes.io/component", "cert-manager").list().getItems().forEach(crb -> {
                KubeResourceManager.get().deleteResource(crb);
            });
    }

    private static void cleanClusterRole() {
        KubeResourceManager.get().kubeClient().getClient().rbac().clusterRoles()
            .withLabel("app.kubernetes.io/part-of", "cert-manager-operator").list().getItems().forEach(cr -> {
                KubeResourceManager.get().deleteResource(cr);
            });
    }

    private static void cleanValidationWebhook() {
        KubeResourceManager.get().kubeCmdClient().inNamespace(CERT_MANAGER_NS)
            .exec(false, false, "delete", "validatingwebhookconfiguration", "cert-manager-webhook");
        KubeResourceManager.get().kubeCmdClient().inNamespace(CERT_MANAGER_NS)
            .exec(false, false, "delete", "mutatingwebhookconfiguration", "cert-manager-webhook");
    }
}
