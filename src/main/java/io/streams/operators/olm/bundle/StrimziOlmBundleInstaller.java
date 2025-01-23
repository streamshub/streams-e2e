/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operators.olm.bundle;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.skodjob.testframe.TestFrameConstants;
import io.skodjob.testframe.olm.OperatorSdkRun;
import io.skodjob.testframe.olm.OperatorSdkRunBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.utils.PodUtils;
import io.skodjob.testframe.wait.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Installer strimzi operator using olm from bundle-image
 */
public class StrimziOlmBundleInstaller {
    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziOlmBundleInstaller.class);

    /**
     * Install strimzi operator from bundle-image using OLM
     *
     * @param operatorName      name of operator
     * @param operatorNamespace namespace where to install
     * @param bundleImageRef    bundle image
     * @return wait future
     */
    public static CompletableFuture<Void> install(String operatorName, String operatorNamespace, String bundleImageRef) {
        // Create ns for the operator
        Namespace ns = new NamespaceBuilder()
            .withNewMetadata()
            .withName(operatorNamespace)
            .endMetadata()
            .build();
        KubeResourceManager.get().createOrUpdateResourceWithWait(ns);

        OperatorSdkRun osr = new OperatorSdkRunBuilder()
            .withBundleImage(bundleImageRef)
            .withInstallMode("AllNamespaces")
            .withNamespace(operatorNamespace)
            .build();

        CompletableFuture<Void> osrRun = CompletableFuture.runAsync(osr::run);

        return Wait.untilAsync(operatorName + " is ready", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT, () -> {
                CompletableFuture.allOf(osrRun).join();
                return isOperatorReady(operatorNamespace);
            });
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
