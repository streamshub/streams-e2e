/*
 * Copyright Skodjob authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.strimzi.resources;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.skodjob.testframe.interfaces.NamespacedResourceType;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolList;

import java.util.function.Consumer;

public class KafkaNodePoolType implements NamespacedResourceType<KafkaNodePool> {

    public KafkaNodePoolType() {}

    @Override
    public String getKind() {
        return KafkaNodePool.RESOURCE_KIND;
    }

    @Override
    public MixedOperation<KafkaNodePool, KafkaNodePoolList, Resource<KafkaNodePool>> getClient() {
        return kafkaNodePoolClient();
    }

    @Override
    public void createInNamespace(String namespace, KafkaNodePool kafkaNodePool) {
        getClient().inNamespace(namespace).resource(kafkaNodePool).create();
    }

    @Override
    public void updateInNamespace(String namespace, KafkaNodePool kafkaNodePool) {
        getClient().inNamespace(namespace).resource(kafkaNodePool).update();
    }

    @Override
    public void deleteFromNamespace(String namespace, String name) {
        getClient().inNamespace(namespace).withName(name).delete();
    }

    @Override
    public void replaceInNamespace(String namespace, String name, Consumer<KafkaNodePool> consumer) {
        KafkaNodePool toBeUpdated = getClient().inNamespace(namespace).withName(name).get();
        consumer.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public void create (KafkaNodePool resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(String name) {
        getClient().withName(name).delete();
    }

    @Override
    public void update(KafkaNodePool resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public void replace(String s, Consumer<KafkaNodePool> editor) {
        KafkaNodePool toBeUpdated = getClient().withName(s).get();
        editor.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public boolean waitForReadiness(KafkaNodePool resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName())
            .get() != null;
    }

    @Override
    public boolean waitForDeletion(KafkaNodePool resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get() == null;
    }

    public MixedOperation<KafkaNodePool, KafkaNodePoolList, Resource<KafkaNodePool>> kafkaNodePoolClient() {
        return Crds.kafkaNodePoolOperation(KubeResourceManager.getKubeClient().getClient());
    }
}
