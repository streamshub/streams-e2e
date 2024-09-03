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
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class KafkaRebalanceType implements ResourceType<KafkaRebalance> {

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
    public void create(KafkaRebalance resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(KafkaRebalance resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).delete();
    }

    @Override
    public void update(KafkaRebalance resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public void replace(KafkaRebalance resource, Consumer<KafkaRebalance> editor) {
        KafkaRebalance toBeUpdated = getClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName()).get();
        editor.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public boolean isReady(KafkaRebalance resource) {
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
    public boolean isDeleted(KafkaRebalance resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get() == null;
    }

    public static MixedOperation<KafkaRebalance, KafkaRebalanceList, Resource<KafkaRebalance>> kafkaRebalanceClient() {
        return Crds.kafkaRebalanceOperation(KubeResourceManager.getKubeClient().getClient());
    }
}
