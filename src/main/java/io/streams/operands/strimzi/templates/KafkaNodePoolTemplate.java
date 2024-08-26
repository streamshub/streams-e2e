/*
 * Copyright Skodjob authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.strimzi.templates;

import io.strimzi.api.ResourceLabels;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.KRaftMetadataStorage;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;

import java.util.List;

public class KafkaNodePoolTemplate {

    public static KafkaNodePoolBuilder defaultKafkaNodePoolPvc(String namespace, String name, int replicas,
                                                               String kafkaClusterName, List<ProcessRoles> roles) {
        return new KafkaNodePoolBuilder()
            .withNewMetadata()
            .withNamespace(namespace)
            .addToLabels(ResourceLabels.STRIMZI_CLUSTER_LABEL, kafkaClusterName)
            .withName(name)
            .endMetadata()
            .withNewSpec()
            .withReplicas(replicas)
            .addAllToRoles(roles)
            .withNewPersistentClaimStorage()
            .withSize("1Gi")
            .withDeleteClaim(true)
            .endPersistentClaimStorage()
            .endSpec();
    }

    public static KafkaNodePoolBuilder defaultKafkaNodePoolJbod(String namespace, String name, int replicas,
                                                                String kafkaClusterName, List<ProcessRoles> roles) {
        return new KafkaNodePoolBuilder()
            .withNewMetadata()
            .withNamespace(namespace)
            .addToLabels(ResourceLabels.STRIMZI_CLUSTER_LABEL, kafkaClusterName)
            .withName(name)
            .endMetadata()
            .withNewSpec()
            .withReplicas(replicas)
            .addAllToRoles(roles)
            .withStorage(
                new JbodStorageBuilder().addToVolumes(
                        new PersistentClaimStorageBuilder().withId(0).withSize("1Gi").withDeleteClaim(true)
                            .withKraftMetadata(KRaftMetadataStorage.SHARED).build())
                    .build())
            .endSpec();
    }
}
