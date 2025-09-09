/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.sql;

import java.util.HashMap;
import java.util.Map;

public class TestStatements {
    // Private constructor
    private TestStatements() {

    }

    public static String getTestSqlExample(String bootstrap, String registryUrl) {
        // CREATE TABLE ProductInventoryTable ( product_id STRING, category STRING, stock STRING, rating STRING ) " +
        // "WITH ( 'connector' = 'filesystem', 'path' = '/opt/flink/data/productInventory.csv', " +
        // "'format' = 'csv', 'csv.ignore-parse-errors' = 'true' )
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE ProductInventoryTable ( product_id STRING, category STRING, stock STRING, rating STRING )");

        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put("path", "/opt/flink/data/productInventory.csv");
        additionalProperties.put("format", "csv");
        additionalProperties.put("csv.ignore-parse-errors", "true");

        SqlWith sqlWith = new SqlWithBuilder()
            .withSqlStatement(builder.toString())
            .withConnector("filesystem")
            .withAdditionalProperties(additionalProperties)
            .build();

        String part1 = sqlWith.generateSql();

        // CREATE TABLE ClickStreamTable " +
        // "( user_id STRING, product_id STRING, `event_time` TIMESTAMP(3) METADATA FROM 'timestamp', " +
        //     "WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND ) WITH ( 'connector' = 'kafka', " +
        //     "'topic' = 'flink.click.streams', 'properties.bootstrap.servers' = " +
        //     "'my-cluster-kafka-bootstrap.flink.svc:9092', 'properties.group.id' = 'click-stream-group', " +
        //     "'value.format' = 'avro-confluent', 'value.avro-confluent.url' = " +
        //     "'http://apicurio-registry-service.flink.svc:8080/apis/ccompat/v6', 'scan.startup.mode' = " +
        //     "'latest-offset' )
        builder = new StringBuilder();
        builder.append("CREATE TABLE ClickStreamTable (");
        builder.append("user_id STRING, product_id STRING, `event_time` TIMESTAMP(3) METADATA FROM 'timestamp', ");
        builder.append("WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND )");

        additionalProperties = new HashMap<>();
        additionalProperties.put("properties.group.id", "click-stream-group");
        additionalProperties.put("value.format", "avro-confluent");
        additionalProperties.put("value.avro-confluent.url", registryUrl);
        additionalProperties.put("scan.startup.mode", "latest-offset");

        sqlWith = new SqlWithBuilder()
            .withSqlStatement(builder.toString())
            .withConnector("kafka")
            .withTopic("flink.click.streams")
            .withBootstrapServer(bootstrap)
            .withAdditionalProperties(additionalProperties)
            .build();

        String part2 = sqlWith.generateSql();

        // CREATE TABLE SalesRecordTable ( invoice_id STRING, user_id STRING, product_id STRING, " +
        //     "quantity STRING, unit_cost STRING, `purchase_time` TIMESTAMP(3) METADATA FROM 'timestamp', " +
        //         "WATERMARK FOR purchase_time AS purchase_time - INTERVAL '1' SECOND ) WITH ( 'connector' = 'kafka', " +
        //         "'topic' = 'flink.sales.records', 'properties.bootstrap.servers' = " +
        //         "'my-cluster-kafka-bootstrap.flink.svc:9092', 'properties.group.id' = 'sales-record-group', " +
        //         "'value.format' = 'avro-confluent', 'value.avro-confluent.url' = " +
        //         "'http://apicurio-registry-service.flink.svc:8080/apis/ccompat/v6', 'scan.startup.mode' = " +
        //         "'latest-offset' )
        builder = new StringBuilder();
        builder.append("CREATE TABLE SalesRecordTable (");
        builder.append("invoice_id STRING, user_id STRING, product_id STRING, quantity STRING, unit_cost STRING, ");
        builder.append("`purchase_time` TIMESTAMP(3) METADATA FROM 'timestamp', ");
        builder.append("WATERMARK FOR purchase_time AS purchase_time - INTERVAL '1' SECOND )");

        additionalProperties = new HashMap<>();
        additionalProperties.put("properties.group.id", "sales-record-group");
        additionalProperties.put("value.format", "avro-confluent");
        additionalProperties.put("value.avro-confluent.url", registryUrl);
        additionalProperties.put("scan.startup.mode", "latest-offset");

        sqlWith = new SqlWithBuilder()
            .withSqlStatement(builder.toString())
            .withConnector("kafka")
            .withTopic("flink.sales.records")
            .withBootstrapServer(bootstrap)
            .withAdditionalProperties(additionalProperties)
            .build();

        String part3 = sqlWith.generateSql();

        // CREATE TABLE CsvSinkTable ( user_id STRING, top_product_ids STRING, " +
        //    "`event_time` TIMESTAMP(3), PRIMARY KEY(`user_id`) NOT ENFORCED ) WITH ( 'connector' = 'upsert-kafka', " +
        //        "'topic' = 'flink.recommended.products', 'properties.bootstrap.servers' = " +
        //        "'my-cluster-kafka-bootstrap.flink.svc:9092', 'properties.client.id' = " +
        //        "'recommended-products-producer-client', 'properties.transaction.timeout.ms' = '800000', " +
        //        "'key.format' = 'csv', 'value.format' = 'csv', 'value.fields-include' = 'ALL' )
        builder = new StringBuilder();
        builder.append("CREATE TABLE CsvSinkTable ( user_id STRING, top_product_ids STRING, `event_time` TIMESTAMP(3), ");
        builder.append("PRIMARY KEY(`user_id`) NOT ENFORCED )");

        additionalProperties = new HashMap<>();
        additionalProperties.put("properties.client.id", "recommended-products-producer-client");
        additionalProperties.put("properties.transaction.timeout.ms", "800000");
        additionalProperties.put("key.format", "csv");
        additionalProperties.put("value.format", "csv");
        additionalProperties.put("value.fields-include", "ALL");

        sqlWith = new SqlWithBuilder()
            .withSqlStatement(builder.toString())
            .withConnector("upsert-kafka")
            .withBootstrapServer(bootstrap)
            .withTopic("flink.recommended.products")
            .withAdditionalProperties(additionalProperties)
            .build();

        String part4 = sqlWith.generateSql();

        // CREATE TEMPORARY VIEW clicked_products AS SELECT DISTINCT c.user_id, c.event_time, p.product_id, p.category " +
        //    "FROM ClickStreamTable AS c JOIN ProductInventoryTable AS p ON c.product_id = p.product_id
        builder = new StringBuilder();
        builder.append("CREATE TEMPORARY VIEW clicked_products AS SELECT DISTINCT c.user_id, c.event_time, p.product_id, p.category ");
        builder.append("FROM ClickStreamTable AS c JOIN ProductInventoryTable AS p ON c.product_id = p.product_id;");
        String part5 = builder.toString();

        // CREATE TEMPORARY VIEW category_products AS SELECT cp.user_id, cp.event_time, p.product_id, " +
        // "p.category, p.stock, p.rating, sr.user_id as purchased FROM clicked_products cp JOIN " +
        //     "ProductInventoryTable AS p ON cp.category = p.category LEFT JOIN SalesRecordTable sr ON " +
        //     "cp.user_id = sr.user_id AND p.product_id = sr.product_id WHERE p.stock > 0 GROUP BY p.product_id, " +
        //     "p.category, p.stock, cp.user_id, cp.event_time, sr.user_id, p.rating
        builder = new StringBuilder();
        builder.append("CREATE TEMPORARY VIEW category_products AS SELECT cp.user_id, cp.event_time, p.product_id, ");
        builder.append("p.category, p.stock, p.rating, sr.user_id as purchased ");
        builder.append("FROM clicked_products cp ");
        builder.append("JOIN ProductInventoryTable AS p ON cp.category = p.category ");
        builder.append("LEFT JOIN SalesRecordTable sr ON cp.user_id = sr.user_id ");
        builder.append("AND p.product_id = sr.product_id WHERE p.stock > 0 ");
        builder.append("GROUP BY p.product_id, p.category, p.stock, cp.user_id, cp.event_time, sr.user_id, p.rating;");
        String part6 = builder.toString();

        // CREATE TEMPORARY VIEW " +
        // "top_products AS SELECT cp.user_id, cp.event_time, cp.product_id, cp.category, cp.stock, cp.rating, " +
        //     "cp.purchased, ROW_NUMBER() OVER (PARTITION BY cp.user_id ORDER BY cp.purchased DESC, cp.rating DESC) " +
        //     "AS rn FROM category_products cp
        builder = new StringBuilder();
        builder.append("CREATE TEMPORARY VIEW top_products AS SELECT cp.user_id, cp.event_time, cp.product_id, cp.category, ");
        builder.append("cp.stock, cp.rating, cp.purchased, ROW_NUMBER() OVER (PARTITION BY cp.user_id ");
        builder.append("ORDER BY cp.purchased DESC, cp.rating DESC) AS rn ");
        builder.append("FROM category_products cp;");
        String part7 = builder.toString();

        // INSERT INTO CsvSinkTable SELECT user_id, LISTAGG(product_id, ',') " +
        // "AS top_product_ids, TUMBLE_END(event_time, INTERVAL '5' SECOND) FROM top_products WHERE rn <= 6 GROUP " +
        //     "BY user_id, TUMBLE(event_time, INTERVAL '5' SECOND);
        builder = new StringBuilder();
        builder.append("INSERT INTO CsvSinkTable SELECT user_id, LISTAGG(product_id, ',') AS top_product_ids, ");
        builder.append("TUMBLE_END(event_time, INTERVAL '5' SECOND) FROM top_products WHERE rn <= 6 GROUP BY user_id, ");
        builder.append("TUMBLE(event_time, INTERVAL '5' SECOND);");
        String part8 = builder.toString();

        return part1 + part2 + part3 + part4 + part5 + part6 + part7 + part8;
    }

    public static String getTestFlinkFilter(String bootstrap, String registryUrl, String kafkaUser, String namespace) {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE payment_fiat (paymentDetails ROW<transactionId STRING, type STRING, " +
            "amount DOUBLE, currency STRING, `date` STRING, status STRING>, payer ROW<name STRING, payerType STRING, " +
            "accountNumber STRING, bank STRING, billingAddress ROW<street STRING, city STRING, state STRING, " +
            "country STRING, postalCode STRING>, cardNumber STRING, cardType STRING, expiryDate STRING>, " +
            "payee ROW<name STRING, payeeType STRING, accountNumber STRING, bank STRING, address ROW<street STRING, " +
            "city STRING, state STRING, country STRING, postalCode STRING>>)");

        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put("properties.group.id", "flink-filter-group");
        additionalProperties.put("value.format", "avro-confluent");
        additionalProperties.put("value.avro-confluent.url", registryUrl);
        // Startup mode for Kafka consumer, we set earliest to catch even previously sent messages
        additionalProperties.put("scan.startup.mode", "earliest-offset");
        additionalProperties.put("properties.security.protocol", "SASL_PLAINTEXT");
        additionalProperties.put("properties.sasl.mechanism", "SCRAM-SHA-512");
        additionalProperties.put("properties.sasl.jaas.config",
            "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=" + kafkaUser + " password={{secret:" + namespace + "/" + kafkaUser + "/password}}\\;");

        SqlWith sqlWith = new SqlWithBuilder()
            .withSqlStatement(builder.toString())
            .withConnector("kafka")
            .withTopic("flink.payment.data")
            .withBootstrapServer(bootstrap)
            .withAdditionalProperties(additionalProperties)
            .build();

        String part1 = sqlWith.generateSql();

        builder = new StringBuilder();
        builder.append("CREATE TABLE paypal ( transactionId STRING, type STRING )");

        additionalProperties = new HashMap<>();
        additionalProperties.put("properties.client.id", "flink-filter-paypal");
        additionalProperties.put("properties.transaction.timeout.ms", "800000");
        additionalProperties.put("key.format", "raw");
        additionalProperties.put("key.fields", "transactionId");
        additionalProperties.put("value.format", "json");
        additionalProperties.put("value.fields-include", "ALL");
        additionalProperties.put("properties.security.protocol", "SASL_PLAINTEXT");
        additionalProperties.put("properties.sasl.mechanism", "SCRAM-SHA-512");
        additionalProperties.put("properties.sasl.jaas.config",
            "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=" + kafkaUser + " password={{secret:" + namespace + "/" + kafkaUser + "/password}}\\;");

        sqlWith = new SqlWithBuilder()
            .withSqlStatement(builder.toString())
            .withConnector("kafka")
            .withBootstrapServer(bootstrap)
            .withTopic("flink.payment.paypal")
            .withAdditionalProperties(additionalProperties)
            .build();

        String part2 = sqlWith.generateSql();


        builder = new StringBuilder();
        builder.append("INSERT INTO paypal" +
            " SELECT paymentDetails.transactionId, paymentDetails.type " +
            "FROM payment_fiat WHERE paymentDetails.type = 'paypal';");

        String part3 = builder.toString();

        return part1 + part2 + part3;
    }

    public static String getTestFlinkFilterOAuth(String bootstrap, String registryUrl, String namespace) {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE payment_fiat (paymentDetails ROW<transactionId STRING, type STRING, " +
            "amount DOUBLE, currency STRING, `date` STRING, status STRING>, payer ROW<name STRING, payerType STRING, " +
            "accountNumber STRING, bank STRING, billingAddress ROW<street STRING, city STRING, state STRING, " +
            "country STRING, postalCode STRING>, cardNumber STRING, cardType STRING, expiryDate STRING>, " +
            "payee ROW<name STRING, payeeType STRING, accountNumber STRING, bank STRING, address ROW<street STRING, " +
            "city STRING, state STRING, country STRING, postalCode STRING>>)");

        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put("properties.group.id", "flink-oauth-filter-group");
        additionalProperties.put("value.format", "avro-confluent");
        additionalProperties.put("value.avro-confluent.url", registryUrl);
        additionalProperties.put("scan.startup.mode", "earliest-offset");
        additionalProperties.put("properties.security.protocol", "SASL_SSL");
        additionalProperties.put("properties.sasl.mechanism", "OAUTHBEARER");
        additionalProperties.put("properties.sasl.jaas.config",
            "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "oauth.ssl.truststore.location=\"/opt/keycloak-ca-cert/ca.crt\" " +
                "oauth.ssl.truststore.type=\"PEM\" " +
                "oauth.client.id=\"kafka-client\" " +
                "oauth.client.secret=\"{{secret:" + namespace + "/kafka-client/clientSecret}}\" " +
                "oauth.token.endpoint.uri=\"https://keycloak-service.keycloak.svc.cluster.local:8443/" +
                "realms/streams-e2e/protocol/openid-connect/token\"\\;");
        additionalProperties.put("properties.sasl.login.callback.handler.class", 
            "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        additionalProperties.put("properties.ssl.check.hostname", "false");
        additionalProperties.put("properties.ssl.endpoint.identification.algorithm", "");
        additionalProperties.put("properties.ssl.truststore.location", "/opt/kafka-ca-cert/ca.crt");
        additionalProperties.put("properties.ssl.truststore.type", "PEM");

        SqlWith sqlWith = new SqlWithBuilder()
            .withSqlStatement(builder.toString())
            .withConnector("kafka")
            .withTopic("flink.payment.data")
            .withBootstrapServer(bootstrap)
            .withAdditionalProperties(additionalProperties)
            .build();

        String part1 = sqlWith.generateSql();

        builder = new StringBuilder();
        builder.append("CREATE TABLE paypal ( transactionId STRING, type STRING )");

        additionalProperties = new HashMap<>();
        additionalProperties.put("properties.client.id", "flink-oauth-paypal");
        additionalProperties.put("properties.transaction.timeout.ms", "800000");
        additionalProperties.put("key.format", "raw");
        additionalProperties.put("key.fields", "transactionId");
        additionalProperties.put("value.format", "json");
        additionalProperties.put("value.fields-include", "ALL");
        additionalProperties.put("properties.security.protocol", "SASL_SSL");
        additionalProperties.put("properties.sasl.mechanism", "OAUTHBEARER");
        additionalProperties.put("properties.sasl.jaas.config",
            "org.apache.flink.kafka.shaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required " +
                "oauth.ssl.truststore.location=\"/opt/keycloak-ca-cert/ca.crt\" " +
                "oauth.ssl.truststore.type=\"PEM\" " +
                "oauth.client.id=\"kafka-client\" " +
                "oauth.client.secret=\"{{secret:" + namespace + "/kafka-client/clientSecret}}\" " +
                "oauth.token.endpoint.uri=\"https://keycloak-service.keycloak.svc.cluster.local:8443/" +
                "realms/streams-e2e/protocol/openid-connect/token\"\\;");
        additionalProperties.put("properties.sasl.login.callback.handler.class", 
            "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler");
        additionalProperties.put("properties.ssl.check.hostname", "false");
        additionalProperties.put("properties.ssl.endpoint.identification.algorithm", "");
        additionalProperties.put("properties.ssl.truststore.location", "/opt/kafka-ca-cert/ca.crt");
        additionalProperties.put("properties.ssl.truststore.type", "PEM");

        sqlWith = new SqlWithBuilder()
            .withSqlStatement(builder.toString())
            .withConnector("kafka")
            .withBootstrapServer(bootstrap)
            .withTopic("flink.payment.paypal")
            .withAdditionalProperties(additionalProperties)
            .build();

        String part2 = sqlWith.generateSql();

        builder = new StringBuilder();
        builder.append("INSERT INTO paypal" +
            " SELECT paymentDetails.transactionId, paymentDetails.type " +
            "FROM payment_fiat WHERE paymentDetails.type = 'paypal';");

        String part3 = builder.toString();

        return part1 + part2 + part3;
    }

    public static String getWrongConnectionSql() {
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE test (message STRING)");

        Map<String, String> additionalProperties = new HashMap<>();
        additionalProperties.put("properties.group.id", "flink-filter-group");
        additionalProperties.put("value.format", "avro-confluent");
        additionalProperties.put("value.avro-confluent.url", "not-exists-sr.cluster.local:5001");
        // Startup mode for Kafka consumer, we set earliest to catch even previously sent messages
        additionalProperties.put("scan.startup.mode", "earliest-offset");

        SqlWith sqlWith = new SqlWithBuilder()
            .withSqlStatement(builder.toString())
            .withConnector("kafka")
            .withTopic("topic.not.exists")
            .withBootstrapServer("not-exists-kafka.cluster.local:9092")
            .withAdditionalProperties(additionalProperties)
            .build();

        String part1 = sqlWith.generateSql();

        builder = new StringBuilder();
        builder.append("CREATE TABLE print_table ( message STRING ) WITH ('connector' = 'print');" +
            "INSERT INTO print_table SELECT * FROM test;");

        String part2 = builder.toString();

        return part1 + part2;
    }
}
