/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.strimzi.templates;

import io.strimzi.api.kafka.model.bridge.KafkaBridgeBuilder;

public class KafkaBridgeTemplate {

    public static KafkaBridgeBuilder defaultKafkaBridge(String namespace, String name, String bootstrapServer) {
        return new KafkaBridgeBuilder()
            .withNewMetadata()
            .withNamespace(namespace)
            .withName(name)
            .endMetadata()
            .withNewSpec()
            .withReplicas(1)
            .withBootstrapServers(bootstrapServer)
            .withNewHttp(8080)
            .endSpec();
    }
}
