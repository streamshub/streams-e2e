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
import io.strimzi.api.kafka.model.connector.KafkaConnector;
import io.strimzi.api.kafka.model.connector.KafkaConnectorList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class KafkaConnectorType implements NamespacedResourceType<KafkaConnector> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConnectorType.class);


    public KafkaConnectorType() {
    }

    @Override
    public String getKind() {
        return KafkaConnector.RESOURCE_KIND;
    }

    @Override
    public MixedOperation<KafkaConnector, KafkaConnectorList, Resource<KafkaConnector>> getClient() {
        return kafkaConnectorClient();
    }

    @Override
    public void createInNamespace(String namespace, KafkaConnector resource) {
        getClient().inNamespace(namespace).resource(resource).create();
    }

    @Override
    public void updateInNamespace(String namespace, KafkaConnector resource) {
        getClient().inNamespace(namespace).resource(resource).update();
    }

    @Override
    public void deleteFromNamespace(String namespace, String name) {
        getClient().inNamespace(namespace).withName(name).delete();
    }

    @Override
    public void replaceInNamespace(String namespace, String name, Consumer<KafkaConnector> consumer) {
        KafkaConnector toBeUpdated = getClient().inNamespace(namespace).withName(name).get();
        consumer.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public void create(KafkaConnector resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(String name) {
        getClient().withName(name).delete();
    }

    @Override
    public void update(KafkaConnector resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public void replace(String name, Consumer<KafkaConnector> editor) {
        KafkaConnector toBeUpdated = getClient().withName(name).get();
        editor.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public boolean waitForReadiness(KafkaConnector resource) {
        KafkaConnector kafkaConnector = kafkaConnectorClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName())
            .get();

        boolean isReady = kafkaConnector.getStatus().getConditions().stream()
            .anyMatch(condition -> condition.getType().equals("Ready") && condition.getStatus().equals("True"));

        if (isReady) {
            LOGGER.info("KafkaConnector {}/{} is Ready", resource.getMetadata().getNamespace(), resource.getMetadata().getName());
            return true;
        } else {
            LOGGER.debug("KafkaConnector {}/{} is not ready yet. Waiting...", resource.getMetadata().getNamespace(),
                resource.getMetadata().getName());
            return false;
        }
    }

    @Override
    public boolean waitForDeletion(KafkaConnector resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get() == null;
    }

    public static MixedOperation<KafkaConnector, KafkaConnectorList, Resource<KafkaConnector>> kafkaConnectorClient() {
        return Crds.kafkaConnectorOperation(KubeResourceManager.getKubeClient().getClient());
    }
}
