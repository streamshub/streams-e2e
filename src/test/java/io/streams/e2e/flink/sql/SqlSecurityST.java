/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.e2e.flink.sql;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.qameta.allure.Allure;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.streams.e2e.Abstract;
import io.streams.operands.flink.templates.FlinkRBAC;
import io.streams.operands.keycloak.templates.KeycloakDeploymentTemplate;
import io.streams.operators.InstallableOperator;
import io.streams.operators.OperatorInstaller;
import io.streams.operators.manifests.KeycloakManifestInstaller;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static io.streams.constants.TestTags.FLINK;
import static io.streams.constants.TestTags.FLINK_SQL_RUNNER;
//import static io.streams.constants.TestTags.SMOKE;

@Tag(FLINK)
@Tag(FLINK_SQL_RUNNER)
@SuiteDoc(
    description = @Desc("This test suite verifies that flink-sql can uses security kafka connection"),
    beforeTestSteps = {
        @Step(value = "Deploy the Strimzi Kafka operator", expected = "Strimzi operator is deployed"),
        @Step(value = "Deploy the Flink Kubernetes operator", expected = "Flink operator is deployed"),
        @Step(value = "Deploy the Keycloak Kubernetes operator", expected = "Keycloak operator is deployed"),
        @Step(value = "Deploy the Apicurio operator", expected = "Apicurio operator is deployed"),
        @Step(value = "Deploy the cert-manager operator", expected = "Cert-manager operator is deployed")
    },
    labels = {
        @Label(value = FLINK_SQL_RUNNER),
        @Label(value = FLINK),
    }
)
public class SqlSecurityST extends Abstract {
    final String kafkaClusterName = "my-cluster";

    @BeforeAll
    void prepareOperators() throws Exception {
        Allure.step("Install required operators", () -> OperatorInstaller.installRequiredOperators(
            InstallableOperator.FLINK,
            InstallableOperator.APICURIO,
            InstallableOperator.KEYCLOAK,
            InstallableOperator.STRIMZI,
            InstallableOperator.CERT_MANAGER));
    }

    @TestDoc(
        description = @Desc("Test verifies sql-runner.jar works integrated with kafka, " +
            "apicurio and uses scram-sha for kafka authentication"),
        steps = {
            @Step(value = "Create namespace, serviceaccount and roles for Flink", expected = "Resources created"),
            @Step(value = "Deploy Apicurio registry", expected = "Apicurio registry is up and running"),
            @Step(value = "Deploy Kafka my-cluster with scram-sha auth", expected = "Kafka is up and running"),
            @Step(value = "Create KafkaUser with scram-sha secret", expected = "KafkaUser created"),
            @Step(value = "Deploy strimzi-kafka-clients producer with payment data generator",
                expected = "Client job is created and data are sent to flink.payment.data topic"),
            @Step(value = "Deploy FlinkDeployment with sql which gets data from flink.payment.data topic filter " +
                "payment of type paypal and send data to flink.payment.paypal topic, for authentication is used " +
                "secret created by KafkaUser and this secret is passed into by secret interpolation",
                expected = "FlinkDeployment is up and tasks are deployed and it sends filtered " +
                    "data into flink.payment.paypal topic"),
            @Step(value = "Deploy strimzi-kafka-clients consumer as job and consume messages from" +
                "kafka topic flink.payment.paypal",
                expected = "Consumer is deployed and it consumes messages"),
            @Step(value = "Verify that messages are present", expected = "Messages are present"),
        },
        labels = {
            @Label(value = FLINK_SQL_RUNNER),
            @Label(value = FLINK),
        }
    )
    @Test
    //@Tag(SMOKE)
    void testKeycloakUsers() {
        String namespace = "flink-keycloak";
        String kafkaUser = "test-user";

        Allure.step("Prepare " + namespace + " namespace", () -> {
            // Create namespace
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                new NamespaceBuilder().withNewMetadata().withName(namespace).endMetadata().build());

            // Add flink RBAC
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                FlinkRBAC.getFlinkRbacResources(namespace).toArray(new HasMetadata[0]));
        });

        Allure.step("Deploy Keycloak", () -> {
            // Create keycloak
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                KeycloakDeploymentTemplate.defaultKeycloakDeployment(
                    KeycloakManifestInstaller.OPERATOR_NS).toArray(new HasMetadata[0]));

            Thread.sleep(30_000);

            KubeResourceManager.get().createOrUpdateResourceWithWait(
                KeycloakDeploymentTemplate.defaultKeycloakRealm(
                    KeycloakManifestInstaller.OPERATOR_NS).toArray(new HasMetadata[0]));

            Thread.sleep(120_000);
        });
    }
}
