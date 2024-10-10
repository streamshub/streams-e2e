/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.strimzi.templates;

import io.strimzi.api.ResourceAnnotations;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;

public class KafkaTemplate {

    public static KafkaBuilder defaultKafka(String namespace, String name) {
        return new KafkaBuilder()
            .withNewMetadata()
            .withNamespace(namespace)
            .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
            .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_KRAFT, "enabled")
            .withName(name)
            .endMetadata()
            .withNewSpec()
            .withNewKafka()
            .addToListeners(
                new GenericKafkaListenerBuilder()
                    .withName("plain")
                    .withTls(false)
                    .withType(KafkaListenerType.INTERNAL)
                    .withPort((9092))
                    .build())
            .addToListeners(
                new GenericKafkaListenerBuilder()
                    .withName("tls")
                    .withTls(true)
                    .withType(KafkaListenerType.INTERNAL)
                    .withPort((9093))
                    .build())
            .endKafka()
            .withNewEntityOperator()
            .editOrNewTemplate()
            .editOrNewTopicOperatorContainer()
            .addNewEnv()
            .withName("STRIMZI_USE_FINALIZERS")
            .withValue("false")
            .endEnv()
            .endTopicOperatorContainer()
            .endTemplate()
            .withNewUserOperator()
            .endUserOperator()
            .withNewTopicOperator()
            .endTopicOperator()
            .endEntityOperator()
            .withNewKafkaExporter()
            .endKafkaExporter()
            .withNewCruiseControl()
            .endCruiseControl()
            .endSpec();
    }
}
