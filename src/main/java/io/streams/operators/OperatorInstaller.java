/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operators;

import io.streams.Environment;
import io.streams.operators.manifests.ApicurioRegistryManifestInstaller;
import io.streams.operators.manifests.CertManagerManifestInstaller;
import io.streams.operators.manifests.DebeziumManifestInstaller;
import io.streams.operators.manifests.FlinkManifestInstaller;
import io.streams.operators.manifests.StrimziManifestInstaller;
import io.streams.operators.olm.bundle.FlinkOlmBundleInstaller;
import io.streams.operators.olm.bundle.StrimziOlmBundleInstaller;
import io.streams.operators.olm.catalog.ApicurioOlmCatalogInstaller;
import io.streams.operators.olm.catalog.CertManagerOlmCatalogInstaller;
import io.streams.operators.olm.catalog.StrimziOlmCatalogInstaller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Test suite helper provides abstraction for overall operators install
 */
public class OperatorInstaller {

    private OperatorInstaller() {
        // Empty private constructor
    }

    /**
     * Installs required operators for test suite
     *
     * @param operators list of operators
     */
    public static void installRequiredOperators(EOperator... operators) throws Exception {
        List<CompletableFuture<?>> operatorWaiting = new ArrayList<>();
        List<CompletableFuture<?>> additionalOperatorWaiting = new ArrayList<>();

        // Install every operator except flink if present
        for (EOperator operator : operators) {
            switch (operator) {
                case STRIMZI -> operatorWaiting.add(installStrimziOperator());
                case APICURIO -> operatorWaiting.add(installApicurioOperator());
                case CERT_MANAGER -> operatorWaiting.add(installCertManagerOperator());
                case DEBEZIUM -> operatorWaiting.add(installDebeziumOperator());
                case FLINK -> {
                    // Skip flink install due to requirement to running cert-manager
                }
                default -> throw new Exception("Not implemented");
            }
        }
        CompletableFuture.allOf(operatorWaiting.toArray(new CompletableFuture[0])).join();

        // if flink is present, install it
        if (Arrays.asList(operators).contains(EOperator.FLINK)) {
            additionalOperatorWaiting.add(installFlinkOperator());
            CompletableFuture.allOf(additionalOperatorWaiting.toArray(new CompletableFuture[0])).join();
        }
    }

    private static CompletableFuture<?> installStrimziOperator() throws IOException {
        if (Environment.INSTALL_STRIMZI_FROM_RH_CATALOG) {
            return StrimziOlmCatalogInstaller.install("amq-streams",
                StrimziManifestInstaller.OPERATOR_NS, null, "stable",
                "redhat-operators", "openshift-marketplace");
        } else if (Environment.STRIMZI_OPERATOR_BUNDLE_IMAGE.isEmpty()) {
            return StrimziManifestInstaller.install();
        } else {
            return StrimziOlmBundleInstaller.install("strimzi-cluster-operator",
                StrimziManifestInstaller.OPERATOR_NS, Environment.STRIMZI_OPERATOR_BUNDLE_IMAGE);
        }
    }

    private static CompletableFuture<?> installCertManagerOperator() throws IOException {
        if (Environment.INSTALL_STRIMZI_FROM_RH_CATALOG) {
            return CertManagerOlmCatalogInstaller.install("openshift-cert-manager-operator",
                "cert-manager-operator", null, "stable-v1",
                "redhat-operators", "openshift-marketplace");
        } else {
            return CertManagerManifestInstaller.install();
        }
    }

    private static CompletableFuture<?> installFlinkOperator() throws IOException {
        if (Environment.FLINK_OPERATOR_BUNDLE_IMAGE.isEmpty()) {
            return FlinkManifestInstaller.install();
        } else {
            return FlinkOlmBundleInstaller.install("flink-kubernetes-operator",
                FlinkManifestInstaller.OPERATOR_NS, Environment.FLINK_OPERATOR_BUNDLE_IMAGE);
        }
    }

    private static CompletableFuture<?> installApicurioOperator() throws IOException {
        if (Environment.INSTALL_STRIMZI_FROM_RH_CATALOG) {
            return ApicurioOlmCatalogInstaller.install("service-registry-operator",
                ApicurioRegistryManifestInstaller.OPERATOR_NS, null, "2.x",
                "redhat-operators", "openshift-marketplace");
        } else {
            return ApicurioRegistryManifestInstaller.install();
        }
    }

    private static CompletableFuture<?> installDebeziumOperator() throws IOException {
        return DebeziumManifestInstaller.install();
    }
}
