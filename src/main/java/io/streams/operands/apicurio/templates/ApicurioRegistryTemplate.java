/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.apicurio.templates;

import io.apicurio.registry.operator.api.v1.model.ApicurioRegistryBuilder;
import io.strimzi.api.ResourceLabels;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;

import java.util.Map;

/**
 * Resources for apicurio registry
 */
public class ApicurioRegistryTemplate {

    public static ApicurioRegistryBuilder defaultApicurioRegistry(String name, String namespace, String kafkaBootstrap) {
        return new ApicurioRegistryBuilder()
            .withNewMetadata()
            .withName(name)
            .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
            .withNewConfiguration()
            .withPersistence("kafkasql")
            .withNewKafkasql()
            .withBootstrapServers(kafkaBootstrap)
            .endKafkasql()
            .endConfiguration()
            .endSpec();
    }

    public static KafkaTopic apicurioKsqlTopic(String namespace, String kafkaClusterName, int replicas) {
        return new KafkaTopicBuilder()
            .withNewMetadata()
            .withNamespace(namespace)
            .withName("kafkasql-journal")
            .addToLabels(ResourceLabels.STRIMZI_CLUSTER_LABEL, kafkaClusterName)
            .endMetadata()
            .withNewSpec()
            .withPartitions(replicas)
            .withReplicas(replicas)
            .withConfig(Map.of(
                "cleanup.policy", "compact"
            ))
            .endSpec()
            .build();
    }
}
