/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.utils.kube;

import io.skodjob.testframe.resources.KubeResourceManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Kubernetes cluster utils
 */
public class ClusterUtils {

    private static final Logger LOGGER = LogManager.getLogger(ClusterUtils.class);

    private ClusterUtils() {
    }

    /**
     * Is current cluster openshift
     *
     * @return true if cluster is openshift
     */
    public static boolean isOcp() {
        return KubeResourceManager.getKubeCmdClient()
            .exec(false, false, "api-versions").out().contains("openshift.io");
    }

    /**
     * Is multinode cluster
     *
     * @return true if cluster is multinode
     */
    public static boolean isMultinode() {
        return KubeResourceManager.getKubeClient().getClient().nodes().list().getItems().size() > 1;
    }
}
