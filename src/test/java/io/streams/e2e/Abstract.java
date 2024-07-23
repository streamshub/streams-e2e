package io.streams.e2e;

import io.skodjob.testframe.annotations.ResourceManager;
import io.skodjob.testframe.annotations.TestVisualSeparator;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.resources.NamespaceType;
import io.skodjob.testframe.resources.OperatorGroupType;
import io.skodjob.testframe.resources.SubscriptionType;
import io.skodjob.testframe.utils.KubeUtils;
import io.streams.constants.TestConstants;
import io.streams.listeners.TestExceptionCallbackListener;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ResourceManager
@TestVisualSeparator
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestExceptionCallbackListener.class)
public class Abstract {
    static {
        KubeResourceManager.getInstance().setResourceTypes(
            new NamespaceType(),
            new SubscriptionType(),
            new OperatorGroupType()
        );
        KubeResourceManager.getInstance().addCreateCallback(r -> {
            if (r.getKind().equals("Namespace")) {
                KubeUtils.labelNamespace(r.getMetadata().getName(), TestConstants.LOG_COLLECT_LABEL, "true");
            }
        });
    }
}
