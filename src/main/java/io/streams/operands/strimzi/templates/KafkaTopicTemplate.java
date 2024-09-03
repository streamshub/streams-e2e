/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.strimzi.templates;

import io.strimzi.api.ResourceLabels;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;

public class KafkaTopicTemplate {

    public static KafkaTopicBuilder defaultKafkaTopic(String namespace, String name, String kafkaClusterName) {
        return new KafkaTopicBuilder()
            .withNewMetadata()
            .withNamespace(namespace)
            .withName(name)
            .addToLabels(ResourceLabels.STRIMZI_CLUSTER_LABEL, kafkaClusterName)
            .endMetadata()
            .withNewSpec()
            .withPartitions(1)
            .withReplicas(1)
            .endSpec();
    }
}
