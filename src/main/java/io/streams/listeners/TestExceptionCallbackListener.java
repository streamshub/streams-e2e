/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.listeners;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.skodjob.testframe.LogCollector;
import io.skodjob.testframe.LogCollectorBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.streams.Environment;
import io.streams.constants.KubeResourceConstants;
import io.streams.constants.TestConstants;
import io.streams.utils.TestUtils;
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.connect.KafkaConnect;
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.user.KafkaUser;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.LifecycleMethodExecutionExceptionHandler;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * jUnit5 specific class which listening on test exception callbacks
 */
public class TestExceptionCallbackListener implements TestExecutionExceptionHandler, LifecycleMethodExecutionExceptionHandler {
    static final Logger LOGGER = LoggerFactory.getLogger(TestExceptionCallbackListener.class);

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        LOGGER.error("Test failed at {} : {}", "Test execution", throwable.getMessage(), throwable);
        saveKubernetesState(context, throwable);
    }

    @Override
    public void handleBeforeAllMethodExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        LOGGER.error("Test failed at {} : {}", "Test before all", throwable.getMessage(), throwable);
        saveKubernetesState(context, throwable);
    }

    @Override
    public void handleBeforeEachMethodExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        LOGGER.error("Test failed at {} : {}", "Test before each", throwable.getMessage(), throwable);
        saveKubernetesState(context, throwable);
    }

    @Override
    public void handleAfterEachMethodExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        LOGGER.error("Test failed at {} : {}", "Test after each", throwable.getMessage(), throwable);
        saveKubernetesState(context, throwable);
    }

    @Override
    public void handleAfterAllMethodExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        LOGGER.error("Test failed at {} : {}", "Test after all", throwable.getMessage(), throwable);
        saveKubernetesState(context, throwable);
    }

    @SuppressFBWarnings({
        "BC_UNCONFIRMED_CAST_OF_RETURN_VALUE",
        "THROWS_METHOD_THROWS_CLAUSE_THROWABLE"
    })
    private void saveKubernetesState(ExtensionContext context, Throwable throwable) throws Throwable {
        LogCollector logCollector = new LogCollectorBuilder()
            .withNamespacedResources(
                KubeResourceConstants.DEPLOYMENT,
                KubeResourceConstants.SUBSCRIPTION,
                KubeResourceConstants.OPERATOR_GROUP,
                KubeResourceConstants.SECRET,
                KubeResourceConstants.CONFIGMAPS,
                KubeResourceConstants.FLINK_DEPLOYMENT,
                KubeResourceConstants.APICURIO_REGISTRY,
                KubeResourceConstants.JOB,
                KubeResourceConstants.ROLE,
                KubeResourceConstants.ROLE_BINDING,
                KubeResourceConstants.SERVICE_ACCOUNT,
                KubeResourceConstants.PVC,
                KubeResourceConstants.STATEFUL_SET,
                KubeResourceConstants.REPLICA_SET,
                KubeResourceConstants.SERVICE,
                KubeResourceConstants.ROUTE,
                KubeResourceConstants.INGRESS,
                KubeResourceConstants.NETWORK_POLICY,
                Kafka.RESOURCE_SINGULAR,
                KafkaNodePool.RESOURCE_SINGULAR,
                KafkaConnect.RESOURCE_SINGULAR,
                KafkaConnector.RESOURCE_SINGULAR,
                KafkaBridge.RESOURCE_SINGULAR,
                KafkaMirrorMaker2.RESOURCE_SINGULAR,
                KafkaRebalance.RESOURCE_SINGULAR,
                KafkaTopic.RESOURCE_SINGULAR,
                KafkaUser.RESOURCE_SINGULAR)
            .withClusterWideResources(
                KubeResourceConstants.NODE,
                KubeResourceConstants.PV)
            .withKubeClient(KubeResourceManager.get().kubeClient())
            .withKubeCmdClient(KubeResourceManager.get().kubeCmdClient())
            .withRootFolderPath(TestUtils.getLogPath(
                Environment.LOG_DIR.resolve("failedTest").toString(), context).toString())
            .build();
        try {
            logCollector.collectFromNamespacesWithLabels(new LabelSelectorBuilder()
                .withMatchLabels(Collections.singletonMap(TestConstants.LOG_COLLECT_LABEL, "true"))
                .build());
        } catch (Exception ignored) {
            LOGGER.warn("Failed to collect");
        }
        logCollector.collectClusterWideResources();
        throw throwable;
    }
}
