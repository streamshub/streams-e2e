/*
 * Copyright Skodjob authors.
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
import io.skodjob.testframe.utils.PodUtils;
import io.skodjob.testframe.wait.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Installer strimzi operator using olm from catalog
 */
public class StrimziOlmCatalogInstaller {
    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziOlmCatalogInstaller.class);

    private static final String SUBSCRIPTION_NAME = "strimzi";

    /**
     * Install strimzi operator from catalog presented on cluster using OLM
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
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(ns);
        //Create operator group for the operator
        if (KubeResourceManager.getKubeClient().getOpenShiftClient().operatorHub().operatorGroups()
            .inNamespace(operatorNamespace).list().getItems().isEmpty()) {
            OperatorGroupBuilder operatorGroup = new OperatorGroupBuilder()
                .editOrNewMetadata()
                .withName("streams-e2e-operator-group")
                .withNamespace(operatorNamespace)
                .endMetadata();
            KubeResourceManager.getInstance().createResourceWithoutWait(operatorGroup.build());
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

        KubeResourceManager.getInstance().createOrUpdateResourceWithoutWait(subscription);
        return Wait.untilAsync(operatorName + " is ready", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT, () -> isOperatorReady(operatorNamespace));
    }

    @SuppressFBWarnings("REC_CATCH_EXCEPTION")
    private static boolean isOperatorReady(String ns) {
        try {
            PodUtils.waitForPodsReadyWithRestart(ns, new LabelSelectorBuilder()
                .withMatchLabels(Map.of("strimzi.io/kind", "cluster-operator")).build(), 1, true);
            LOGGER.info("Strimzi operator in namespace {} is ready", ns);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}
