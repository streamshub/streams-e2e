/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.flink.templates;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;

import java.util.List;

/**
 * Flink RBAC related resources
 */
public class FlinkRBAC {

    /**
     * Returns RBAC resource for running flink deployment outside operator namespace
     *
     * @param namespace namespace for running deployment
     * @return list of resources to create
     */
    public static List<HasMetadata> getFlinkRbacResources(String namespace) {
        return List.of(
            new ServiceAccountBuilder()
                .withNewMetadata()
                .withName("flink")
                .withNamespace(namespace)
                .addToLabels("app.kubernetes.io/name", "flink-kubernetes-operator")
                .endMetadata()
                .build(),
            new RoleBuilder()
                .withNewMetadata()
                .withName("flink")
                .withNamespace(namespace)
                .addToLabels("app.kubernetes.io/name", "flink-kubernetes-operator")
                .endMetadata()
                .withRules(
                    new PolicyRuleBuilder()
                        .withApiGroups("")
                        .withResources(
                            "pods",
                            "configmaps",
                            "pods/finalizers"
                        )
                        .withVerbs("*")
                        .build(),
                    new PolicyRuleBuilder()
                        .withApiGroups("apps")
                        .withResources(
                            "deployments",
                            "deployments/finalizers"
                        )
                        .withVerbs("*")
                        .build(),
                    new PolicyRuleBuilder()
                        .withApiGroups("")
                        .withResources(
                            "secrets"
                        )
                        .withVerbs("get", "list")
                        .build()
                )
                .build(),
            new RoleBindingBuilder()
                .withNewMetadata()
                .withName("flink")
                .withNamespace(namespace)
                .addToLabels("app.kubernetes.io/name", "flink-kubernetes-operator")
                .endMetadata()
                .withNewRoleRef()
                .withName("flink")
                .withKind("Role")
                .endRoleRef()
                .addNewSubject()
                .withName("flink")
                .withNamespace(namespace)
                .withKind("ServiceAccount")
                .endSubject()
                .build()
        );
    }
}
