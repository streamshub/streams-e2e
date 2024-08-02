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
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class KafkaRebalanceType implements NamespacedResourceType<KafkaRebalance> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRebalanceType.class);


    public KafkaRebalanceType() {
    }

    @Override
    public String getKind() {
        return KafkaRebalance.RESOURCE_KIND;
    }

    @Override
    public MixedOperation<KafkaRebalance, KafkaRebalanceList, Resource<KafkaRebalance>> getClient() {
        return kafkaRebalanceClient();
    }

    @Override
    public void createInNamespace(String namespace, KafkaRebalance resource) {
        getClient().inNamespace(namespace).resource(resource).create();
    }

    @Override
    public void updateInNamespace(String namespace, KafkaRebalance resource) {
        getClient().inNamespace(namespace).resource(resource).update();
    }

    @Override
    public void deleteFromNamespace(String namespace, String name) {
        getClient().inNamespace(namespace).withName(name).delete();
    }

    @Override
    public void replaceInNamespace(String namespace, String name, Consumer<KafkaRebalance> consumer) {
        KafkaRebalance toBeUpdated = getClient().inNamespace(namespace).withName(name).get();
        consumer.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public void create(KafkaRebalance resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(String name) {
        getClient().withName(name).delete();
    }

    @Override
    public void update(KafkaRebalance resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public void replace(String name, Consumer<KafkaRebalance> editor) {
        KafkaRebalance toBeUpdated = getClient().withName(name).get();
        editor.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public boolean waitForReadiness(KafkaRebalance resource) {
        KafkaRebalance kafkaRebalance = kafkaRebalanceClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName())
            .get();

        boolean isReady = kafkaRebalance.getStatus().getConditions().stream()
            .anyMatch(condition -> condition.getType().equals("Ready") && condition.getStatus().equals("True"));

        if (isReady) {
            LOGGER.info("KafkaRebalance {}/{} is Ready", resource.getMetadata().getNamespace(), resource.getMetadata().getName());
            return true;
        } else {
            LOGGER.debug("KafkaRebalance {}/{} is not ready yet. Waiting...", resource.getMetadata().getNamespace(),
                resource.getMetadata().getName());
            return false;
        }
    }

    @Override
    public boolean waitForDeletion(KafkaRebalance resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get() == null;
    }

    public static MixedOperation<KafkaRebalance, KafkaRebalanceList, Resource<KafkaRebalance>> kafkaRebalanceClient() {
        return Crds.kafkaRebalanceOperation(KubeResourceManager.getKubeClient().getClient());
    }
}
