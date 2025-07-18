/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.flink.templates;

import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.skodjob.testframe.utils.KubeUtils;
import io.streams.Environment;
import org.apache.flink.v1beta1.FlinkDeploymentBuilder;
import org.apache.flink.v1beta1.FlinkDeploymentSpec;
import org.apache.flink.v1beta1.flinkdeploymentspec.Job;

import java.util.Collections;
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
        FlinkDeploymentBuilder fb = new FlinkDeploymentBuilder()
            .withNewMetadata()
            .withName(name)
            .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
            .withFlinkVersion(FlinkDeploymentSpec.FlinkVersion.valueOf(Environment.FLINK_VERSION))
            .withFlinkConfiguration(
                Map.of(
                    "taskmanager.numberOfTaskSlots", "1"
                )
            )
            .withServiceAccount("flink")
            .withNewPodTemplate()
            .withKind("Pod")
            .withNewMetadata()
            .withName(name)
            .endMetadata()
            .withNewSpec()
            .addNewContainer()
            .withName("flink-main-container")
            .withImagePullPolicy("Always")
            .endContainer()
            .endSpec()
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
            .endResource()
            .endTaskManager()
            .withNewJob()
            .withJarURI("local:///opt/streamshub/flink-sql-runner.jar")
            .withParallelism(1L)
            .withUpgradeMode(Job.UpgradeMode.stateless)
            .withArgs(args)
            .endJob()
            .endSpec();

        if (!Environment.FLINK_SQL_RUNNER_IMAGE.isEmpty()) {
            fb.editOrNewSpec()
                .withImage(Environment.FLINK_SQL_RUNNER_IMAGE)
                .endSpec();
        }

        return fb;
    }

    /**
     * Return default flink deployment for sql runner
     *
     * @param namespace     namespace of flink deployment
     * @param name          name of deployment
     * @param sqlStatements list of SQL statements that will be executed
     * @return flink deployment builder
     */
    public static FlinkDeploymentBuilder flinkExampleDeployment(String namespace, String name, List<String> sqlStatements) {
        return defaultFlinkDeployment(namespace, name, sqlStatements)
            .editSpec()
            .editPodTemplate()
            .editSpec()
            .editFirstContainer()
            .addNewVolumeMount()
            .withName("product-inventory-vol")
            .withMountPath("/opt/flink/data")
            .endVolumeMount()
            .endContainer()
            .addNewVolume()
            .withName("product-inventory-vol")
            .withNewConfigMap()
            .withName("product-inventory")
            .addNewItem()
            .withKey("productInventory.csv")
            .withPath("productInventory.csv")
            .endItem()
            .endConfigMap()
            .endVolume()
            .endSpec()
            .endPodTemplate()
            .endSpec();
    }

    /**
     * Return default flink deployment with sql configmap for sql runner
     *
     * @param namespace         namespace of flink deployment
     * @param name              name of deployment
     * @param sqlConfigMapName  name of ConfigMap with SQL_STATEMENTS key
     * @return flink deployment builder
     */
    public static FlinkDeploymentBuilder flinkExampleDeploymentWithSqlConfigMap(String namespace, String name, String sqlConfigMapName) {
        return flinkExampleDeployment(namespace, name, null)
                .editSpec()
                .editPodTemplate()
                .editSpec()
                .editFirstContainer()
                .addNewEnvFrom()
                .withNewConfigMapRef()
                .withName(sqlConfigMapName)
                .endConfigMapRef()
                .endEnvFrom()
                .endContainer()
                .endSpec()
                .endPodTemplate()
                .endSpec();
    }

    /**
     * Returns default kube pvc for flink state backend
     *
     * @param namespace namespace
     * @param name      name
     * @return pvc builder
     */
    public static PersistentVolumeClaimBuilder getFlinkPVC(String namespace, String name) {
        String accessMode = "ReadWriteOnce";
        if (KubeUtils.isOcp() && KubeUtils.isMultinode()) {
            accessMode = "ReadWriteMany";
        }
        return new PersistentVolumeClaimBuilder()
            .withNewMetadata()
            .withName(name)
            .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
            .withAccessModes(accessMode)
            .withNewResources()
            .withRequests(Collections.singletonMap(
                "storage",
                new Quantity("100Gi")))
            .endResources()
            .endSpec();
    }
}
