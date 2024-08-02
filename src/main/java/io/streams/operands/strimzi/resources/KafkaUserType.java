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
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class KafkaUserType implements NamespacedResourceType<KafkaUser> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUserType.class);


    public KafkaUserType() {
    }

    @Override
    public String getKind() {
        return KafkaUser.RESOURCE_KIND;
    }

    @Override
    public MixedOperation<KafkaUser, KafkaUserList, Resource<KafkaUser>> getClient() {
        return kafkaUserClient();
    }

    @Override
    public void createInNamespace(String namespace, KafkaUser resource) {
        getClient().inNamespace(namespace).resource(resource).create();
    }

    @Override
    public void updateInNamespace(String namespace, KafkaUser resource) {
        getClient().inNamespace(namespace).resource(resource).update();
    }

    @Override
    public void deleteFromNamespace(String namespace, String name) {
        getClient().inNamespace(namespace).withName(name).delete();
    }

    @Override
    public void replaceInNamespace(String namespace, String name, Consumer<KafkaUser> consumer) {
        KafkaUser toBeUpdated = getClient().inNamespace(namespace).withName(name).get();
        consumer.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public void create(KafkaUser resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(String name) {
        getClient().withName(name).delete();
    }

    @Override
    public void update(KafkaUser resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public void replace(String name, Consumer<KafkaUser> editor) {
        KafkaUser toBeUpdated = getClient().withName(name).get();
        editor.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public boolean waitForReadiness(KafkaUser resource) {
        KafkaUser kafkaUser = kafkaUserClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName())
            .get();

        boolean isReady = kafkaUser.getStatus().getConditions().stream()
            .anyMatch(condition -> condition.getType().equals("Ready") && condition.getStatus().equals("True"));

        if (isReady) {
            LOGGER.info("KafkaUser {}/{} is Ready", resource.getMetadata().getNamespace(), resource.getMetadata().getName());
            return true;
        } else {
            LOGGER.debug("KafkaUser {}/{} is not ready yet. Waiting...", resource.getMetadata().getNamespace(),
                resource.getMetadata().getName());
            return false;
        }
    }

    @Override
    public boolean waitForDeletion(KafkaUser resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get() == null;
    }

    public static MixedOperation<KafkaUser, KafkaUserList, Resource<KafkaUser>> kafkaUserClient() {
        return Crds.kafkaUserOperation(KubeResourceManager.getKubeClient().getClient());
    }
}
