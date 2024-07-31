package io.streams.e2e.dummy;

import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.streams.constants.TestTags;
import io.streams.e2e.Abstract;
import io.streams.operators.manifests.StrimziManifestInstaller;
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

    static boolean enabled() {
        return System.getProperty("groups") != null && System.getProperty("groups").toLowerCase().contains("dummy");
    }
}
