/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.flink.templates;

import org.apache.flink.v1beta1.FlinkDeploymentBuilder;
import org.apache.flink.v1beta1.FlinkDeploymentSpec;
import org.apache.flink.v1beta1.flinkdeploymentspec.Job;

import java.util.List;
import java.util.Map;

/**
 * FlinkDeployment templates
 */
public class FlinkDeploymentTemplate {

    /**
     * Return default flink deployment for sql runner
     *
     * @param namespace namespace of flink deployment
     * @param name      name of deployment
     * @param args      args for sql runner
     * @return flink deployment builder
     */
    public static FlinkDeploymentBuilder defaultFlinkDeployment(String namespace, String name, List<String> args) {
        return new FlinkDeploymentBuilder()
            .withNewMetadata()
                .withName(name)
                .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
                .withImage("quay.io/streamshub/flink-sql-runner:latest")
                .withFlinkVersion(FlinkDeploymentSpec.FlinkVersion.v1_19)
                .withFlinkConfiguration(
                    Map.of("taskmanager.numberOfTaskSlots", "1")
                )
                .withServiceAccount("flink")
                .withNewPodTemplate()
                    .withKind("Pod")
                    .withNewMetadata()
                        .withName(name)
                    .endFlinkdeploymentspecMetadata()
                    .withNewSpec()
                        .addNewContainer()
                            .withName("flink-main-container")
                            .withImage("quay.io/streamshub/flink-sql-runner:latest")
                            .withImagePullPolicy("Always")
                            .addNewVolumeMount()
                                .withName("product-inventory-vol")
                                .withMountPath("/opt/flink/data")
                            .endFlinkdeploymentspecVolumeMount()
                            .addNewVolumeMount()
                                .withName("flink-logs")
                                .withMountPath("/opt/flink/log")
                            .endFlinkdeploymentspecVolumeMount()
                            .addNewVolumeMount()
                                .withName("flink-artifacts")
                                .withMountPath("/opt/flink/artifacts")
                            .endFlinkdeploymentspecVolumeMount()
                        .endFlinkdeploymentspecContainer()
                        .addNewVolume()
                            .withName("product-inventory-vol")
                            .withNewConfigMap()
                                .withName("product-inventory")
                                .addNewItem()
                                    .withKey("productInventory.csv")
                                    .withPath("productInventory.csv")
                                .endFlinkdeploymentspecItem()
                            .endFlinkdeploymentspecConfigMap()
                        .endFlinkdeploymentspecVolume()
                        .addNewVolume()
                            .withName("flink-logs")
                            .withNewEmptyDir()
                            .endFlinkdeploymentspecEmptyDir()
                        .endFlinkdeploymentspecVolume()
                        .addNewVolume()
                            .withName("flink-artifacts")
                            .withNewEmptyDir()
                            .endFlinkdeploymentspecEmptyDir()
                        .endFlinkdeploymentspecVolume()
                    .endFlinkdeploymentspecSpec()
                .endPodTemplate()
                .withNewJobManager()
                    .withNewResource()
                        .withCpu(1.0)
                        .withMemory("2048m")
                    .endResource()
                .endJobManager()
                .withNewTaskManager()
                    .withNewResource()
                        .withCpu(1.0)
                        .withMemory("2048m")
                    .endTaskmanagerResource()
                .endTaskManager()
                .withNewJob()
                    .withJarURI("local:///opt/streamshub/flink-sql-runner.jar")
                    .withParallelism(1L)
                    .withUpgradeMode(Job.UpgradeMode.stateless)
                    .withArgs(args)
                .endJob()
            .endSpec();
    }
}