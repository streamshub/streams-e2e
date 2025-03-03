/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.flink.resoruces;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.skodjob.testframe.interfaces.ResourceType;
import io.skodjob.testframe.resources.KubeResourceManager;
import org.apache.flink.v1beta1.FlinkDeployment;
import org.apache.flink.v1beta1.FlinkDeploymentStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class FlinkDeploymentType implements ResourceType<FlinkDeployment> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkDeploymentType.class);


    public FlinkDeploymentType() {
    }

    @Override
    public String getKind() {
        return "FlinkDeployment";
    }

    @Override
    public MixedOperation<FlinkDeployment, KubernetesResourceList<FlinkDeployment>, Resource<FlinkDeployment>> getClient() {
        return KubeResourceManager.get().kubeClient().getClient().resources(FlinkDeployment.class);
    }

    @Override
    public void create(FlinkDeployment resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).create();
    }

    @Override
    public void delete(FlinkDeployment resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).delete();
    }

    @Override
    public void update(FlinkDeployment resource) {
        getClient().inNamespace(resource.getMetadata().getNamespace()).resource(resource).update();
    }

    @Override
    public void replace(FlinkDeployment resource, Consumer<FlinkDeployment> editor) {
        FlinkDeployment toBeUpdated = getClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName()).get();
        editor.accept(toBeUpdated);
        update(toBeUpdated);
    }

    @Override
    public boolean isReady(FlinkDeployment resource) {
        FlinkDeployment fd = getClient().inNamespace(resource.getMetadata().getNamespace())
            .withName(resource.getMetadata().getName())
            .get();

        boolean isReady = fd.getStatus().getJobManagerDeploymentStatus()
            .equals(FlinkDeploymentStatus.JobManagerDeploymentStatus.READY);

        if (isReady) {
            LOGGER.info("FlinkDeployment {}/{} is Ready", resource.getMetadata().getNamespace(), resource.getMetadata().getName());
            return true;
        } else {
            LOGGER.debug("FlinkDeployment {}/{} is not ready yet. Waiting...", resource.getMetadata().getNamespace(),
                resource.getMetadata().getName());
            return false;
        }
    }

    @Override
    public boolean isDeleted(FlinkDeployment resource) {
        return getClient().inNamespace(resource.getMetadata().getNamespace()).withName(resource.getMetadata().getName()).get() == null;
    }
}
