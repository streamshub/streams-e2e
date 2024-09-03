/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.strimzi.templates;

import io.strimzi.api.ResourceLabels;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;

public class KafkaRebalanceTemplate {

    public static KafkaRebalanceBuilder defaultKafkaRebalance(String namespace, String name, String kafkaClusterName) {
        return new KafkaRebalanceBuilder()
            .withNewMetadata()
            .withNamespace(namespace)
            .withName(name)
            .addToLabels(ResourceLabels.STRIMZI_CLUSTER_LABEL, kafkaClusterName)
            .endMetadata()
            .withNewSpec()
            .endSpec();
    }
}
