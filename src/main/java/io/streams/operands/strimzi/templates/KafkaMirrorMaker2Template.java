/*
 * Copyright Skodjob authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.strimzi.templates;

import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2Builder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2ClusterSpecBuilder;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2MirrorSpecBuilder;

public class KafkaMirrorMaker2Template {

    public static KafkaMirrorMaker2Builder defaultKafkaMirrorMaker2(String namespace, String name,
                                                                    String sourceClusterName, String sourceClusterBootstrap,
                                                                    String targetClusterName, String targetClusterBootstrap) {
        return new KafkaMirrorMaker2Builder()
            .withNewMetadata()
            .withNamespace(namespace)
            .withName(name)
            .endMetadata()
            .withNewSpec()
            .withReplicas(1)
            .withConnectCluster(targetClusterName)
            .withClusters(new KafkaMirrorMaker2ClusterSpecBuilder()
                    .withBootstrapServers(targetClusterBootstrap)
                    .withAlias(targetClusterName)
                    .withNewTls()
                    .addToTrustedCertificates(
                        new CertSecretSourceBuilder()
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(targetClusterName))
                            .withPattern("*.crt")
                            .build())
                    .endTls()
                    .build(),
                new KafkaMirrorMaker2ClusterSpecBuilder()
                    .withBootstrapServers(sourceClusterBootstrap)
                    .withAlias(sourceClusterName)
                    .withNewTls()
                    .addToTrustedCertificates(
                        new CertSecretSourceBuilder()
                            .withSecretName(KafkaResources.clusterCaCertificateSecretName(sourceClusterName))
                            .withPattern("*.crt")
                            .build())
                    .endTls()
                    .build())
            .withMirrors(new KafkaMirrorMaker2MirrorSpecBuilder()
                .withTargetCluster(targetClusterName)
                .withSourceCluster(sourceClusterName)
                .withTopicsPattern(".*")
                .withGroupsPattern(".*")
                .build())
            .endSpec();
    }
}
