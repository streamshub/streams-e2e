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
import io.strimzi.api.kafka.model.bridge.KafkaBridge;
import io.strimzi.api.kafka.model.bridge.KafkaBridgeList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class KafkaBridgeType implements NamespacedResourceType<KafkaBridge> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaBridgeType.class);


    public KafkaBridgeType() {
    }

    @Override
    public String getKind() {
        return KafkaBridge.RESOURCE_KIND;
    }

    @Override
    public MixedOperation<KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>> getClient() {
        return kafkaBridgeClient();
    }

    @Override
    public void createInNamespace(String namespace, KafkaBridge resource) {
        getClient().inNamespace(namespace).resource(resource).create();
    }

    @Override
    public void updateInNamespace(String namespace, KafkaBridge resource) {
        getClient().inNamespace(namespace).resource(resource).update();
    }

    @Override
    public void deleteFromNamespace(String namespace, String name) {
        getClient().inNamespace(namespace).withName(name).delete();
    }

    @Override
    public void replaceInNamespace(String namespace, String name, Consumer<KafkaBridge> consumer) {
        KafkaBridge toBeUpdated = getClient().inNamespace(namespace).withName(name).get();
        consumer.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public void create(KafkaBridge resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(String name) {
        getClient().withName(name).delete();
    }

    @Override
    public void update(KafkaBridge resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public void replace(String name, Consumer<KafkaBridge> editor) {
        KafkaBridge toBeUpdated = getClient().withName(name).get();
        editor.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public boolean waitForReadiness(KafkaBridge resource) {
        KafkaBridge kafkaBridge = kafkaBridgeClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName())
            .get();

        boolean isReady = kafkaBridge.getStatus().getConditions().stream()
            .anyMatch(condition -> condition.getType().equals("Ready") && condition.getStatus().equals("True"));

        if (isReady) {
            LOGGER.info("KafkaBridge {}/{} is Ready", resource.getMetadata().getNamespace(), resource.getMetadata().getName());
            return true;
        } else {
            LOGGER.debug("KafkaBridge {}/{} is not ready yet. Waiting...", resource.getMetadata().getNamespace(),
                resource.getMetadata().getName());
            return false;
        }
    }

    @Override
    public boolean waitForDeletion(KafkaBridge resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get() == null;
    }

    public static MixedOperation<KafkaBridge, KafkaBridgeList, Resource<KafkaBridge>> kafkaBridgeClient() {
        return Crds.kafkaBridgeOperation(KubeResourceManager.getKubeClient().getClient());
    }
}
