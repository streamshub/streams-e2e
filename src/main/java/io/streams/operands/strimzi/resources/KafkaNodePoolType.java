/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.strimzi.resources;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.skodjob.testframe.interfaces.ResourceType;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolList;

import java.util.function.Consumer;

public class KafkaNodePoolType implements ResourceType<KafkaNodePool> {

    public KafkaNodePoolType() {
    }

    @Override
    public String getKind() {
        return KafkaNodePool.RESOURCE_KIND;
    }

    @Override
    public MixedOperation<KafkaNodePool, KafkaNodePoolList, Resource<KafkaNodePool>> getClient() {
        return kafkaNodePoolClient();
    }

    @Override
    public void create(KafkaNodePool resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(KafkaNodePool resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).delete();
    }

    @Override
    public void update(KafkaNodePool resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public void replace(KafkaNodePool resource, Consumer<KafkaNodePool> editor) {
        KafkaNodePool toBeUpdated = getClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName()).get();
        editor.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public boolean isReady(KafkaNodePool resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName())
            .get() != null;
    }

    @Override
    public boolean isDeleted(KafkaNodePool resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get() == null;
    }

    public MixedOperation<KafkaNodePool, KafkaNodePoolList, Resource<KafkaNodePool>> kafkaNodePoolClient() {
        return Crds.kafkaNodePoolOperation(KubeResourceManager.get().kubeClient().getClient());
    }
}
