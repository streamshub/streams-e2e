/*
 * Copyright Skodjob authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.strimzi.templates;

import io.strimzi.api.ResourceLabels;
import io.strimzi.api.kafka.model.connector.KafkaConnectorBuilder;

public class KafkaConnectorTemplate {

    public static KafkaConnectorBuilder defaultKafkaConnector(String namespace, String name, String kafkaConnectClusterName,
                                                     String topicName) {
        return new KafkaConnectorBuilder()
            .withNewMetadata()
            .withNamespace(namespace)
            .withName(name)
            .addToLabels(ResourceLabels.STRIMZI_CLUSTER_LABEL, kafkaConnectClusterName)
            .endMetadata()
            .withNewSpec()
            .withNewAutoRestart()
            .endAutoRestart()
            .withClassName("org.apache.kafka.connect.file.FileStreamSourceConnector")
            .withTasksMax(2)
            .addToConfig("file", "/opt/kafka/LICENSE")
            .addToConfig("topic", topicName)
            .endSpec();
    }
}
