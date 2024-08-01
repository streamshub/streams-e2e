/*
 * Copyright Skodjob authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.strimzi.templates;

import io.strimzi.api.ResourceLabels;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;

import java.util.List;

public class KafkaNodePoolTemplate {

    public static KafkaNodePoolBuilder defaultKafkaNodePool(String namespace, String name,
                                                            String kafkaClusterName, List<ProcessRoles> roles) {
        return new KafkaNodePoolBuilder()
            .withNewMetadata()
            .withNamespace(namespace)
            .addToLabels(ResourceLabels.STRIMZI_CLUSTER_LABEL, kafkaClusterName)
            .withName(name)
            .endMetadata()
            .withNewSpec()
            .withReplicas(3)
            .addAllToRoles(roles)
            .withNewPersistentClaimStorage()
            .withSize("1Gi")
            .withDeleteClaim(true)
            .endPersistentClaimStorage()
            .endSpec();
    }
}
