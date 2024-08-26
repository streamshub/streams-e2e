/*
 * Copyright Skodjob authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.apicurio.resources;

import io.apicurio.registry.operator.api.v1.model.ApicurioRegistry;
import io.apicurio.registry.operator.api.v1.model.apicurioregistrystatus.Conditions;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.skodjob.testframe.interfaces.ResourceType;
import io.skodjob.testframe.resources.KubeResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Consumer;

public class ApicurioRegistryType implements ResourceType<ApicurioRegistry> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApicurioRegistryType.class);


    public ApicurioRegistryType() {
    }

    @Override
    public String getKind() {
        return "ApicurioRegistry";
    }

    @Override
    public MixedOperation<ApicurioRegistry, KubernetesResourceList<ApicurioRegistry>,
        Resource<ApicurioRegistry>> getClient() {
        return KubeResourceManager.getKubeClient().getClient().resources(ApicurioRegistry.class);
    }

    @Override
    public void create(ApicurioRegistry resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(ApicurioRegistry resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).delete();
    }

    @Override
    public void update(ApicurioRegistry resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public void replace(ApicurioRegistry resource, Consumer<ApicurioRegistry> editor) {
        ApicurioRegistry toBeUpdated = getClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName()).get();
        editor.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public boolean isReady(ApicurioRegistry resource) {
        ApicurioRegistry fd = getClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName())
            .get();

        Optional<Conditions> c = fd.getStatus().getConditions().stream().filter(conditions ->
            conditions.getType().equals("Ready")).findFirst();
        boolean isReady = c.isPresent() && c.get().getStatus().getValue().equals("True");

        if (isReady) {
            LOGGER.info("ApicurioRegistry {}/{} is Ready", resource.getMetadata().getNamespace(), resource.getMetadata().getName());
            return true;
        } else {
            LOGGER.debug("ApicurioRegistry {}/{} is not ready yet. Waiting...", resource.getMetadata().getNamespace(),
                resource.getMetadata().getName());
            return false;
        }
    }

    @Override
    public boolean isDeleted(ApicurioRegistry resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get() == null;
    }
}
