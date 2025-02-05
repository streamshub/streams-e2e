/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.constants;

public interface KubeResourceConstants {
    // Kubernetes default resources
    String DEPLOYMENT = "deployment";
    String SUBSCRIPTION = "subscription";
    String OPERATOR_GROUP = "operatorgroup";
    String CONFIGMAPS = "configmaps";
    String SECRET = "secret";
    String ROLE = "role";
    String ROLE_BINDING = "rolebinding";
    String SERVICE_ACCOUNT = "serviceaccount";
    String JOB = "job";
    String NODE = "node";
    String PV = "pv";
    String PVC = "pvc";
    String STATEFUL_SET = "statefulset";
    String REPLICA_SET = "replicasets";
    String SERVICE = "service";
    String ROUTE = "route";
    String NETWORK_POLICY = "networkpolicies";
    String INGRESS = "ingress";

    // Custom resources
    String FLINK_DEPLOYMENT = "FlinkDeployment";
    String APICURIO_REGISTRY = "ApicurioRegistry";
}
