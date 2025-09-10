/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.keycloak.templates;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.skodjob.testframe.resources.KubeResourceManager;

import java.io.IOException;
import java.util.List;

public class KeycloakDeploymentTemplate {

    public static List<HasMetadata> defaultKeycloakDeployment(String namespace) throws IOException {
        List<HasMetadata> resources = KubeResourceManager.get().readResourcesFromFile(
            KeycloakDeploymentTemplate.class.getClassLoader().getResourceAsStream("keycloak/database.yaml"));
        resources.addAll(KubeResourceManager.get().readResourcesFromFile(
            KeycloakDeploymentTemplate.class.getClassLoader().getResourceAsStream("keycloak/keycloak.yaml")));
        return resources.stream().peek(r -> r.getMetadata().setNamespace(namespace)).toList();
    }

    public static List<HasMetadata> defaultKeycloakRealm(String namespace) throws IOException {
        List<HasMetadata> resources = KubeResourceManager.get().readResourcesFromFile(
            KeycloakDeploymentTemplate.class.getClassLoader().getResourceAsStream("keycloak/keycloak-realm.yaml"));
        return resources.stream().peek(r -> r.getMetadata().setNamespace(namespace)).toList();
    }
}
