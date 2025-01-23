/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
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
import io.streams.Environment;
import io.streams.constants.TestConstants;
import io.streams.listeners.TestExceptionCallbackListener;
import io.streams.operands.apicurio.resources.ApicurioRegistryType;
import io.streams.operands.flink.resoruces.FlinkDeploymentType;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@ResourceManager
@TestVisualSeparator
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(TestExceptionCallbackListener.class)
public class Abstract {
    static final Logger LOGGER = LoggerFactory.getLogger(Abstract.class);

    static {
        // Init abstraction of resource types
        KubeResourceManager.get().setResourceTypes(
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
            new KafkaBridgeType(),
            new FlinkDeploymentType(),
            new ApicurioRegistryType()
        );

        // Set collect label for every namespace created during test run
        KubeResourceManager.get().addCreateCallback(r -> {
            if (r.getKind().equals("Namespace")) {
                KubeUtils.labelNamespace(r.getMetadata().getName(), TestConstants.LOG_COLLECT_LABEL, "true");
            }
        });

        // Enable storing created resources in yaml format.
        KubeResourceManager.get().setStoreYamlPath(Environment.LOG_DIR.toString());

        // Store config file for current test run
        try {
            Environment.saveConfig();
        } catch (IOException e) {
            LOGGER.warn("Saving of config file failed");
        }
    }
}
