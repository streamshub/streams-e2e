/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.minio;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.streams.constants.TestConstants;
import java.util.Map;

public class MinioInstaller {

    public static final String MINIO = "minio";
    public static final String ADMIN_CREDS = "minioadminLongerThan16BytesForFIPS";
    public static final String MINIO_STORAGE_ALIAS = "local";
    public static final int MINIO_PORT = 9000;
    public static final int MINIO_CONSOLE_PORT = 9090;
    private static final String MINIO_IMAGE = "quay.io/minio/minio:RELEASE.2025-06-13T11-33-47Z";

    /**
     * Deploy minio to a specific namespace, creates service for it and init client inside the Minio pod
     * @param namespace where Minio will be installed to
     */
    public static void deployMinio(String namespace) {
        // Create a Minio deployment
        Deployment minioDeployment = new DeploymentBuilder()
            .withNewMetadata()
            .withName(MINIO)
            .withNamespace(namespace)
            .withLabels(Map.of(TestConstants.DEPLOYMENT_TYPE, MINIO))
            .endMetadata()
            .withNewSpec()
            .withReplicas(1)
            .withNewSelector()
            .withMatchLabels(Map.of(TestConstants.APP_POD_LABEL, MINIO))
            .endSelector()
            .withNewTemplate()
            .withNewMetadata()
            .withLabels(Map.of(TestConstants.APP_POD_LABEL, MINIO))
            .endMetadata()
            .withNewSpec()
            .addNewContainer()
            .withName(MINIO)
            .withImage(MINIO_IMAGE)
            .withArgs("server", "/data", "--console-address", ":" + MINIO_CONSOLE_PORT)
            .addToEnv(new EnvVar("MINIO_ROOT_USER", ADMIN_CREDS, null))
            .addToEnv(new EnvVar("MINIO_ROOT_PASSWORD", ADMIN_CREDS, null))
            .addNewPort()
            .withContainerPort(MINIO_PORT)
            .endPort()
            .withVolumeMounts(new VolumeMountBuilder()
                .withName("minio-storage")
                .withMountPath("/data")
                .build())
            .endContainer()
            .withVolumes(new VolumeBuilder()
                .withName("minio-storage")
                .withNewEmptyDir()
                .endEmptyDir()
                .build())
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();

        // Create the deployment
        KubeResourceManager.get().createResourceWithWait(minioDeployment);

        // Create a service to expose Minio
        Service minioService = new ServiceBuilder()
            .withNewMetadata()
            .withName(MINIO)
            .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
            .withSelector(Map.of(TestConstants.APP_POD_LABEL, MINIO))
            .addNewPort()
            .withName("api")
            .withPort(MINIO_PORT)
            .withTargetPort(new IntOrString(MINIO_PORT))
            .endPort()
            .addNewPort()
            .withName("console")
            .withPort(MINIO_CONSOLE_PORT)
            .withTargetPort(new IntOrString(MINIO_CONSOLE_PORT))
            .endPort()
            .endSpec()
            .build();

        KubeResourceManager.get().createResourceWithoutWait(minioService);
        // NetworkPolicyResource.allowNetworkPolicyAllIngressForMatchingLabel(namespace, MINIO, Map.of(TestConstants.APP_POD_LABEL, MINIO));

        initMinioClient(namespace);
    }

    /**
     * Init client inside the Minio pod. This allows other commands to be executed during the tests.
     * @param namespace where Minio is installed
     */
    private static void initMinioClient(String namespace) {
        final LabelSelector labelSelector = new LabelSelectorBuilder().withMatchLabels(Map.of(TestConstants.APP_POD_LABEL, MINIO)).build();
        final String minioPod = KubeResourceManager.get().kubeClient().listPods(namespace, labelSelector).get(0).getMetadata().getName();

        KubeResourceManager.get().kubeCmdClient().inNamespace(namespace).execInPod(minioPod,
            "mc",
            "alias",
            "set",
            MINIO_STORAGE_ALIAS,
            "http://localhost:" + MINIO_PORT,
            ADMIN_CREDS, ADMIN_CREDS);
    }

    /**
     * Create bucket in Minio instance in specific namespace.
     * @param namespace Minio location
     * @param bucketName name of the bucket that will be created and used within the tests
     */
    public static void createBucket(String namespace, String bucketName) {
        final LabelSelector labelSelector = new LabelSelectorBuilder().withMatchLabels(Map.of(TestConstants.APP_POD_LABEL, MINIO)).build();
        final String minioPod = KubeResourceManager.get().kubeClient().listPods(namespace, labelSelector).get(0).getMetadata().getName();

        KubeResourceManager.get().kubeCmdClient().inNamespace(namespace).execInPod(minioPod,
            "mc",
            "mb",
            MINIO_STORAGE_ALIAS + "/" + bucketName);
    }
}
