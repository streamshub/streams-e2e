package io.streams.e2e.dummy;

import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.streams.constants.TestTags;
import io.streams.e2e.Abstract;
import io.streams.operators.manifests.CertManagerManifestInstaller;
import io.streams.operators.manifests.ApicurioRegistryManifestInstaller;
import io.streams.operators.manifests.DebeziumManifestInstaller;
import io.streams.operators.manifests.FlinkManifestInstaller;
import io.streams.operators.manifests.StrimziManifestInstaller;
import io.streams.operators.olm.catalog.StrimziOlmCatalogInstaller;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class aimed for test that test suite support methods works as expected
 */
@Tag(TestTags.DUMMY)
@EnabledIf(value = "enabled")
public class DummyST extends Abstract {

    @Test
    void kubeResourceManagerImplTest() {
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            new NamespaceBuilder().withNewMetadata().withName("test").endMetadata().build()
        );
        assertTrue(KubeResourceManager.getKubeClient().namespaceExists("test"));
        assertEquals("true", KubeResourceManager.getKubeClient().getClient().namespaces()
            .withName("test").get().getMetadata().getLabels().get("streams-e2e"));
    }

    @Test
    void installStrimziFromManifestsTest() throws IOException {
        CompletableFuture.allOf(
            StrimziManifestInstaller.install()).join();
        assertTrue(KubeResourceManager.getKubeClient().getClient().apps()
            .deployments().inNamespace(StrimziManifestInstaller.OPERATOR_NS)
            .withName(StrimziManifestInstaller.DEPLOYMENT_NAME).isReady());
    }

    @Test
    void installStrimziByOlmCatalogTest() {
        CompletableFuture.allOf(
            StrimziOlmCatalogInstaller.install("strimzi-kafka-operator", "strimzi-olm",
                "strimzi-cluster-operator.v0.42.0", "stable", "operatorhubio-catalog", "olm")
        ).join();
        assertTrue(KubeResourceManager.getKubeClient().getClient().apps()
            .deployments().inNamespace("strimzi-olm")
            .withName("strimzi-cluster-operator-v0.42.0").isReady());
    }

    @Test
    void installDebeziumFromManifestsTest() throws IOException {
        CompletableFuture.allOf(
            DebeziumManifestInstaller.install()).join();
        assertTrue(KubeResourceManager.getKubeClient().getClient().apps()
            .deployments().inNamespace(DebeziumManifestInstaller.OPERATOR_NS)
            .withName(DebeziumManifestInstaller.DEPLOYMENT_NAME).isReady());
    }

    @Test
    void installFlinkAndCertManagerFromManifestsTest() throws IOException {
        CompletableFuture.allOf(
            CertManagerManifestInstaller.install(),
            FlinkManifestInstaller.install()).join();

        assertTrue(KubeResourceManager.getKubeClient().getClient().apps()
            .deployments().inNamespace(CertManagerManifestInstaller.OPERATOR_NS)
            .withName(CertManagerManifestInstaller.DEPLOYMENT_NAME).isReady());

        assertTrue(KubeResourceManager.getKubeClient().getClient().apps()
            .deployments().inNamespace(FlinkManifestInstaller.OPERATOR_NS)
            .withName(FlinkManifestInstaller.DEPLOYMENT_NAME).isReady());
    }

    @Test
    void installApicurioRegistryFromManifestTest() throws IOException {
        ApicurioRegistryManifestInstaller.install().join();

        assertTrue(KubeResourceManager.getKubeClient().getClient().apps()
            .deployments().inNamespace(ApicurioRegistryManifestInstaller.OPERATOR_NS)
            .withName(ApicurioRegistryManifestInstaller.DEPLOYMENT_NAME).isReady());
    }

    static boolean enabled() {
        return System.getProperty("groups") != null && System.getProperty("groups").toLowerCase().contains("dummy");
    }
}
