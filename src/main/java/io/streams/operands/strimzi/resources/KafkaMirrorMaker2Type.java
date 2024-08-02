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
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.mirrormaker2.KafkaMirrorMaker2List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class KafkaMirrorMaker2Type implements NamespacedResourceType<KafkaMirrorMaker2> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMirrorMaker2Type.class);


    public KafkaMirrorMaker2Type() {
    }

    @Override
    public String getKind() {
        return KafkaMirrorMaker2.RESOURCE_KIND;
    }

    @Override
    public MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, Resource<KafkaMirrorMaker2>> getClient() {
        return kafkaMirrorMaker2Client();
    }

    @Override
    public void createInNamespace(String namespace, KafkaMirrorMaker2 resource) {
        getClient().inNamespace(namespace).resource(resource).create();
    }

    @Override
    public void updateInNamespace(String namespace, KafkaMirrorMaker2 resource) {
        getClient().inNamespace(namespace).resource(resource).update();
    }

    @Override
    public void deleteFromNamespace(String namespace, String name) {
        getClient().inNamespace(namespace).withName(name).delete();
    }

    @Override
    public void replaceInNamespace(String namespace, String name, Consumer<KafkaMirrorMaker2> consumer) {
        KafkaMirrorMaker2 toBeUpdated = getClient().inNamespace(namespace).withName(name).get();
        consumer.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public void create(KafkaMirrorMaker2 resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(String name) {
        getClient().withName(name).delete();
    }

    @Override
    public void update(KafkaMirrorMaker2 resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public void replace(String name, Consumer<KafkaMirrorMaker2> editor) {
        KafkaMirrorMaker2 toBeUpdated = getClient().withName(name).get();
        editor.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public boolean waitForReadiness(KafkaMirrorMaker2 resource) {
        KafkaMirrorMaker2 kafkaMirrorMaker2 = kafkaMirrorMaker2Client().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName())
            .get();

        boolean isReady = kafkaMirrorMaker2.getStatus().getConditions().stream()
            .anyMatch(condition -> condition.getType().equals("Ready") && condition.getStatus().equals("True"));

        if (isReady) {
            LOGGER.info("KafkaMirrorMaker2 {}/{} is Ready", resource.getMetadata().getNamespace(), resource.getMetadata().getName());
            return true;
        } else {
            LOGGER.debug("KafkaMirrorMaker2 {}/{} is not ready yet. Waiting...", resource.getMetadata().getNamespace(),
                resource.getMetadata().getName());
            return false;
        }
    }

    @Override
    public boolean waitForDeletion(KafkaMirrorMaker2 resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get() == null;
    }

    public static MixedOperation<KafkaMirrorMaker2, KafkaMirrorMaker2List, Resource<KafkaMirrorMaker2>> kafkaMirrorMaker2Client() {
        return Crds.kafkaMirrorMaker2Operation(KubeResourceManager.getKubeClient().getClient());
    }
}
