package io.streams.e2e;

import io.skodjob.testframe.annotations.ResourceManager;
import io.skodjob.testframe.annotations.TestVisualSeparator;
import io.skodjob.testframe.resources.DeploymentType;
import io.skodjob.testframe.resources.InstallPlanType;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.resources.NamespaceType;
import io.skodjob.testframe.resources.OperatorGroupType;
import io.skodjob.testframe.resources.ServiceType;
import io.skodjob.testframe.resources.SubscriptionType;
import io.skodjob.testframe.utils.KubeUtils;
import io.streams.constants.TestConstants;
import io.streams.listeners.TestExceptionCallbackListener;
import io.streams.operands.strimzi.resources.KafkaBridgeType;
import io.streams.operands.strimzi.resources.KafkaConnectType;
import io.streams.operands.strimzi.resources.KafkaConnectorType;
import io.streams.operands.strimzi.resources.KafkaMirrorMaker2Type;
import io.streams.operands.strimzi.resources.KafkaNodePoolType;
import io.streams.operands.strimzi.resources.KafkaRebalanceType;
import io.streams.operands.strimzi.resources.KafkaTopicType;
import io.streams.operands.strimzi.resources.KafkaType;
import io.streams.operands.strimzi.resources.KafkaUserType;
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
            new OperatorGroupType(),
            new DeploymentType(),
            new InstallPlanType(),
            new ServiceType(),
            new KafkaType(),
            new KafkaNodePoolType(),
            new KafkaTopicType(),
            new KafkaUserType(),
            new KafkaConnectType(),
            new KafkaConnectorType(),
            new KafkaMirrorMaker2Type(),
            new KafkaRebalanceType(),
            new KafkaBridgeType()
        );
        KubeResourceManager.getInstance().addCreateCallback(r -> {
            if (r.getKind().equals("Namespace")) {
                KubeUtils.labelNamespace(r.getMetadata().getName(), TestConstants.LOG_COLLECT_LABEL, "true");
            }
        });
    }
}
