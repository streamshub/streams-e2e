/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.strimzi.templates;

import io.strimzi.api.ResourceAnnotations;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.connect.KafkaConnectBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;

public class KafkaConnectTemplate {

    public static KafkaConnectBuilder defaultKafkaConnect(String namespace, String name, String kafkaClusterName,
                                                          String bootstrapServer) {
        return new KafkaConnectBuilder()
            .withNewMetadata()
            .withNamespace(namespace)
            .withName(name)
            .endMetadata()
            .withNewSpec()
            .withReplicas(1)
            .withBootstrapServers(bootstrapServer)
            // Tls configuration
            .withNewTls()
            .addToTrustedCertificates(
                new CertSecretSourceBuilder()
                    .withSecretName(KafkaResources.clusterCaCertificateSecretName(kafkaClusterName))
                    .withPattern("*.crt")
                    .build())
            .endTls()
            .endSpec();
    }

    public static KafkaConnectBuilder defaultKafkaConnectWithConnector(String namespace, String name, String kafkaClusterName,
                                                                       String bootstrapServer) {
        return defaultKafkaConnect(namespace, name, kafkaClusterName, bootstrapServer)
            .editMetadata()
            .addToAnnotations(ResourceAnnotations.STRIMZI_DOMAIN + "use-connector-resources", "true")
            .endMetadata();
    }
}
