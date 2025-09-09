/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.e2e.flink.sql;

import io.apicurio.registry.serde.avro.AvroKafkaSerializer;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.NamespaceBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.qameta.allure.Allure;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.streams.e2e.Abstract;
import io.streams.clients.kafka.StrimziKafkaClients;
import io.streams.clients.kafka.StrimziKafkaClientsBuilder;
import io.streams.operands.apicurio.templates.ApicurioRegistryTemplate;
import io.streams.operands.flink.templates.FlinkDeploymentTemplate;
import io.streams.operands.strimzi.templates.KafkaUserTemplate;
import io.streams.sql.TestStatements;
import io.skodjob.testframe.utils.JobUtils;
import io.skodjob.testframe.TestFrameConstants;
import io.streams.utils.StrimziClientUtils;
import io.streams.utils.TestUtils;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationScramSha512;
import io.strimzi.api.kafka.model.user.KafkaUserScramSha512ClientAuthentication;
import org.apache.flink.v1beta1.FlinkDeployment;
import io.streams.operands.certmanager.templates.CertManagerCaTemplate;
import io.streams.operands.flink.templates.FlinkRBAC;
import io.streams.operands.keycloak.templates.KeycloakDeploymentTemplate;
import io.streams.operands.strimzi.templates.KafkaNodePoolTemplate;
import io.streams.operands.strimzi.templates.KafkaTemplate;
import io.streams.operands.strimzi.resources.KafkaType;
import io.streams.operators.InstallableOperator;
import io.streams.operators.OperatorInstaller;
import io.streams.operators.manifests.KeycloakManifestInstaller;
import io.strimzi.api.kafka.model.common.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static io.streams.constants.TestTags.FLINK;
import static io.streams.constants.TestTags.FLINK_SQL_RUNNER;
import static io.streams.constants.TestTags.SMOKE;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
            "apicurio and uses keycloak for kafka authentication"),
        steps = {
            @Step(value = "Create namespace, serviceaccount and roles for Flink", expected = "Resources created"),
            @Step(value = "Deploy Apicurio registry", expected = "Apicurio registry is up and running"),
            @Step(value = "Deploy Keycloak realm", expected = "Keyclaok realm is imported"),
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
    @Tag(SMOKE)
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
            // Create CA
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                CertManagerCaTemplate.defaultCA(KeycloakManifestInstaller.OPERATOR_NS).toArray(new HasMetadata[0]));

            // Create keycloak
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                KeycloakDeploymentTemplate.defaultKeycloakDeployment(
                    KeycloakManifestInstaller.OPERATOR_NS).toArray(new HasMetadata[0]));

            Thread.sleep(30_000);

            KubeResourceManager.get().createOrUpdateResourceWithWait(
                KeycloakDeploymentTemplate.defaultKeycloakRealm(
                    KeycloakManifestInstaller.OPERATOR_NS).toArray(new HasMetadata[0]));
        });

        Allure.step("Copy secrets from keycloak namespace to test namespace", () -> {
            Secret keycloakTlsSecret = KubeResourceManager.get().kubeClient().getClient().secrets()
                .inNamespace(KeycloakManifestInstaller.OPERATOR_NS)
                .withName("keycloak-tls-secret")
                .get();

            Secret copiedSecret = new SecretBuilder(keycloakTlsSecret)
                .editMetadata()
                .withNamespace(namespace)
                .withName("keycloak-tls-secret")
                .withResourceVersion(null)
                .withUid(null)
                .endMetadata()
                .build();

            Secret clientSecret = new SecretBuilder()
                .withNewMetadata()
                .withName("kafka-client")
                .withNamespace(namespace)
                .endMetadata()
                .withData(
                    Collections.singletonMap("clientSecret",
                        Base64.encodeBase64String("secret".getBytes(StandardCharsets.UTF_8)))
                )
                .build();

            KubeResourceManager.get().createOrUpdateResourceWithWait(copiedSecret, clientSecret);
        });

        Allure.step("Create kafka with Oauth 2.0 keycloak authentication", () -> {
            // Create kafka
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                KafkaNodePoolTemplate.defaultKafkaNodePoolJbod(namespace, "dual-role",
                    3, kafkaClusterName, List.of(ProcessRoles.BROKER, ProcessRoles.CONTROLLER)).build());

            KubeResourceManager.get().createOrUpdateResourceWithWait(
                KafkaTemplate.defaultKafka(namespace, kafkaClusterName)
                    .editSpec()
                    .editKafka()
                    .withListeners(
                        new GenericKafkaListenerBuilder()
                            .withName("plain")
                            .withTls(false)
                            .withType(KafkaListenerType.INTERNAL)
                            .withPort((9092))
                            .withAuth(new KafkaListenerAuthenticationScramSha512())
                            .build(),
                        new GenericKafkaListenerBuilder()
                            .withName("unsecure")
                            .withTls(false)
                            .withType(KafkaListenerType.INTERNAL)
                            .withPort((9094))
                            .build(),
                        new GenericKafkaListenerBuilder()
                            .withName("oauth2")
                            .withTls(true)
                            .withType(KafkaListenerType.INTERNAL)
                            .withPort((9093))
                            .withAuth(new KafkaListenerAuthenticationOAuthBuilder()
                                .withValidIssuerUri("https://keycloak-service.keycloak.svc.cluster.local:8443" +
                                    "/realms/streams-e2e")
                                .withJwksEndpointUri("https://keycloak-service.keycloak.svc.cluster.local:8443" +
                                    "/realms/streams-e2e/protocol/openid-connect/certs")
                                .withUserNameClaim("preferred_username")
                                .withTlsTrustedCertificates(new CertSecretSourceBuilder()
                                    .withSecretName("keycloak-tls-secret")
                                    .withCertificate("tls.crt")
                                    .build()
                                )
                                .build())
                            .build()
                    )
                    .endKafka()
                    .endSpec()
                    .build());
        });

        String bootstrapServerUnsecure = KafkaType.kafkaClient().inNamespace(namespace).withName(kafkaClusterName).get()
            .getStatus().getListeners().stream().filter(l -> l.getName().equals("unsecure"))
            .findFirst().get().getBootstrapServers();
        String bootstrapServerOAuth = KafkaType.kafkaClient().inNamespace(namespace).withName(kafkaClusterName).get()
            .getStatus().getListeners().stream().filter(l -> l.getName().equals("oauth2"))
            .findFirst().get().getBootstrapServers();
        String bootstrapServerAuth = KafkaType.kafkaClient().inNamespace(namespace).withName(kafkaClusterName).get()
            .getStatus().getListeners().stream().filter(l -> l.getName().equals("plain"))
            .findFirst().get().getBootstrapServers();

        Allure.step("Create kafka scram sha user", () -> {
            // Create kafka scram sha user
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                KafkaUserTemplate.defaultKafkaUser(namespace, kafkaUser, kafkaClusterName)
                    .editSpec()
                    .withAuthentication(new KafkaUserScramSha512ClientAuthentication())
                    .endSpec()
                    .build());
        });

        String registryUrl = "http://apicurio-registry-service." + namespace + ".svc:8080/apis/ccompat/v6";

        Allure.step("Deploy apicurio registry", () -> {
            // Create topic for ksql apicurio
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                ApicurioRegistryTemplate.apicurioKsqlTopic(namespace, kafkaClusterName, 3));

            // Add apicurio
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                ApicurioRegistryTemplate.defaultApicurioRegistry("apicurio-registry", namespace,
                    bootstrapServerUnsecure).build());
        });

        Allure.step("Get user secret configuration");
        // Get user secret jaas configuration
        final String saslJaasConfigEncrypted = KubeResourceManager.get().kubeClient().getClient().secrets()
            .inNamespace(namespace).withName(kafkaUser).get().getData().get("sasl.jaas.config");
        final String saslJaasConfigDecrypted = TestUtils.decodeFromBase64(saslJaasConfigEncrypted);

        String producerName = "kafka-producer";
        Allure.step("Create kafka producer and produce payment data", () -> {
            // Run internal producer and produce data
            StrimziKafkaClients kafkaProducerClient = new StrimziKafkaClientsBuilder()
                .withProducerName(producerName)
                .withNamespaceName(namespace)
                .withTopicName("flink.payment.data")
                .withBootstrapAddress(bootstrapServerAuth)
                .withMessageCount(10000)
                .withUsername(kafkaUser)
                .withDelayMs(10)
                .withMessageTemplate("payment_fiat")
                .withAdditionalConfig(
                    StrimziClientUtils.getApicurioAdditionalProperties(AvroKafkaSerializer.class.getName(),
                        "http://apicurio-registry-service." + namespace + ".svc:8080/apis/registry/v2") + "\n"
                        + "sasl.mechanism=SCRAM-SHA-512\n"
                        + "security.protocol=SASL_PLAINTEXT\n"
                        + "sasl.jaas.config=" + saslJaasConfigDecrypted
                )
                .build();

            KubeResourceManager.get().createResourceWithWait(
                kafkaProducerClient.producerStrimzi()
            );
        });

        Allure.step("Deploy flink application with OAuth authentication", () -> {
            // Deploy flink with OAuth filter sql statement
            FlinkDeployment flink = FlinkDeploymentTemplate.defaultFlinkDeployment(namespace,
                    "flink-oauth", List.of(TestStatements.getTestFlinkFilterOAuth(
                        bootstrapServerOAuth, registryUrl, namespace)))
                .editSpec()
                .editPodTemplate()
                .editOrNewSpec()
                .editFirstContainer()
                .addNewVolumeMount()
                .withMountPath("/opt/kafka-ca-cert")
                .withName("kafka-ca-cert")
                .endVolumeMount()
                .addNewVolumeMount()
                .withMountPath("/opt/keycloak-ca-cert")
                .withName("keycloak-ca-cert")
                .endVolumeMount()
                .endContainer()
                .addNewVolume()
                .withName("kafka-ca-cert")
                .withNewSecret()
                .withSecretName(kafkaClusterName + "-cluster-ca-cert")
                .addNewItem()
                .withKey("ca.crt")
                .withPath("ca.crt")
                .endItem()
                .endSecret()
                .endVolume()
                .addNewVolume()
                .withName("keycloak-ca-cert")
                .withNewSecret()
                .withSecretName("keycloak-tls-secret")
                .addNewItem()
                .withKey("ca.crt")
                .withPath("ca.crt")
                .endItem()
                .endSecret()
                .endVolume()
                .endSpec()
                .endPodTemplate()
                .endSpec()
                .build();
            KubeResourceManager.get().createOrUpdateResourceWithWait(flink);
        });

        Allure.step("Consume filtered messages", () -> {
            // Run consumer and check if data are filtered
            String consumerName = "kafka-consumer";
            StrimziKafkaClients kafkaConsumerClient = new StrimziKafkaClientsBuilder()
                .withConsumerName(consumerName)
                .withNamespaceName(namespace)
                .withTopicName("flink.payment.paypal")
                .withBootstrapAddress(bootstrapServerAuth)
                .withMessageCount(10)
                .withAdditionalConfig(
                    "sasl.mechanism=SCRAM-SHA-512\n" +
                        "security.protocol=SASL_PLAINTEXT\n" +
                        "sasl.jaas.config=" + saslJaasConfigDecrypted
                )
                .withConsumerGroup("flink-filter-test-group").build();

            KubeResourceManager.get().createResourceWithWait(
                kafkaConsumerClient.consumerStrimzi()
            );

            JobUtils.waitForJobSuccess(namespace, kafkaConsumerClient.getConsumerName(),
                TestFrameConstants.GLOBAL_TIMEOUT_MEDIUM);
            String consumerPodName = KubeResourceManager.get().kubeClient().listPodsByPrefixInName(namespace, consumerName)
                .get(0).getMetadata().getName();
            String log = KubeResourceManager.get().kubeClient().getLogsFromPod(namespace, consumerPodName);
            assertTrue(log.contains("\"type\":\"paypal\""));
            assertFalse(log.contains("\"type\":\"creditCard\""));
        });
    }
}
