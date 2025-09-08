/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.certmanager.templates;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.skodjob.testframe.resources.KubeResourceManager;

import java.io.IOException;
import java.util.List;

public class CertManagerCaTemplate {

    public static List<HasMetadata> defaultCA(String namespace) throws IOException {
        List<HasMetadata> resources = KubeResourceManager.get().readResourcesFromFile(
            CertManagerCaTemplate.class.getClassLoader().getResourceAsStream("cert-manager/ca-issuer.yaml"));
        return resources.stream().peek(r -> r.getMetadata().setNamespace(namespace)).toList();
    }
}
