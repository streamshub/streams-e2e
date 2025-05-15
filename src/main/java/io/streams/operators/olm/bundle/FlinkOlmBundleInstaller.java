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
import io.skodjob.testframe.utils.TestFrameUtils;
import io.skodjob.testframe.wait.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Installer flink operator using olm from bundle-image
 */
public class FlinkOlmBundleInstaller {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkOlmBundleInstaller.class);

    /**
     * Install flink operator from bundle-image using OLM
     *
     * @param operatorName      name of operator
     * @param operatorNamespace namespace where to install
     * @param bundleImageRef    bundle image
     * @return wait future
     */
    public static CompletableFuture<Void> install(String operatorName, String operatorNamespace, String bundleImageRef) {
        OperatorSdkRun osr = new OperatorSdkRunBuilder()
            .withBundleImage(bundleImageRef)
            .withInstallMode("AllNamespaces")
            .withNamespace(operatorNamespace)
            .build();

        return Wait.untilAsync(operatorName + " is ready", TestFrameConstants.GLOBAL_POLL_INTERVAL_1_SEC,
            TestFrameConstants.GLOBAL_TIMEOUT, () -> {
                TestFrameUtils.runUntilPass(3, () -> {
                    Namespace ns = new NamespaceBuilder()
                        .withNewMetadata()
                        .withName(operatorNamespace)
                        .endMetadata()
                        .build();
                    KubeResourceManager.get().createOrUpdateResourceWithWait(ns);
                    try {
                        return osr.run();
                    } catch (Exception ex) {
                        KubeResourceManager.get().deleteResourceAsyncWait(ns);
                        throw ex;
                    }
                });
                return isOperatorReady(operatorNamespace);
            });
    }

    @SuppressFBWarnings("REC_CATCH_EXCEPTION")
    private static boolean isOperatorReady(String ns) {
        try {
            PodUtils.waitForPodsReadyWithRestart(ns, new LabelSelectorBuilder()
                    .withMatchLabels(Map.of("app.kubernetes.io/name", "flink-kubernetes-operator")).build(),
                1, true);
            LOGGER.info("Flink operator in namespace {} is ready", ns);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}
