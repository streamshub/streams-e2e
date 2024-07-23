package io.streams.e2e.dummy;

import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.streams.constants.TestTags;
import io.streams.e2e.Abstract;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag(TestTags.SMOKE)
public class DummyST extends Abstract {

    @Test
    void dummyTest() {
        KubeResourceManager.getInstance().createOrUpdateResourceWithWait(
            new NamespaceBuilder().withNewMetadata().withName("test").endMetadata().build()
        );
        assertTrue(KubeResourceManager.getKubeClient().namespaceExists("test"));
        assertEquals("true", KubeResourceManager.getKubeClient().getClient().namespaces()
            .withName("test").get().getMetadata().getLabels().get("streams-e2e"));
    }
}
