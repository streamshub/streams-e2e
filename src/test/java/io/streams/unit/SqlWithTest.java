/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.unit;

import io.streams.sql.SqlWith;
import io.streams.sql.SqlWithBuilder;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqlWithTest {
    @Test
    void testCreateTableWithFilesystemConnector() {
        String expectedSql = "CREATE TABLE ProductInventoryTable ( product_id STRING, category STRING, stock STRING, rating STRING ) " +
            "WITH ('connector' = 'filesystem', 'path' = '/opt/flink/data/productInventory.csv', " +
            "'csv.ignore-parse-errors' = 'true', 'format' = 'csv');";

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

        assertTrue(Objects.equals(sqlWith.generateSql(), expectedSql));
    }

    @Test
    void testCreateTableWithKafkaConnector() {
        String expectedSql = "CREATE TABLE ClickStreamTable (user_id STRING, product_id STRING, " +
            "`event_time` TIMESTAMP(3) METADATA FROM 'timestamp', " +
            "WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND ) " +
            "WITH ('connector' = 'kafka', 'properties.bootstrap.servers' = 'my-kafka.bootstrap', 'topic' = 'flink.click.streams', " +
            "'value.format' = 'avro-confluent', 'properties.group.id' = 'click-stream-group', " +
            "'value.avro-confluent.url' = 'https://apicurio.registry', 'scan.startup.mode' = 'latest-offset');";

        // CREATE TABLE ClickStreamTable " +
        // "( user_id STRING, product_id STRING, `event_time` TIMESTAMP(3) METADATA FROM 'timestamp', " +
        //     "WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND ) WITH ( 'connector' = 'kafka', " +
        //     "'topic' = 'flink.click.streams', 'properties.bootstrap.servers' = " +
        //     "'my-cluster-kafka-bootstrap.flink.svc:9092', 'properties.group.id' = 'click-stream-group', " +
        //     "'value.format' = 'avro-confluent', 'value.avro-confluent.url' = " +
        //     "'http://apicurio-registry-service.flink.svc:8080/apis/ccompat/v6', 'scan.startup.mode' = " +
        //     "'latest-offset' )
        StringBuilder builder = new StringBuilder();
        builder.append("CREATE TABLE ClickStreamTable (");
        builder.append("user_id STRING, product_id STRING, `event_time` TIMESTAMP(3) METADATA FROM 'timestamp', ");
        builder.append("WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND )");

        Map<String, String>  additionalProperties = new HashMap<>();
        additionalProperties.put("properties.group.id", "click-stream-group");
        additionalProperties.put("value.format", "avro-confluent");
        additionalProperties.put("value.avro-confluent.url", "https://apicurio.registry");
        additionalProperties.put("scan.startup.mode", "latest-offset");

        SqlWith sqlWith = new SqlWithBuilder()
            .withSqlStatement(builder.toString())
            .withConnector("kafka")
            .withTopic("flink.click.streams")
            .withBootstrapServer("my-kafka.bootstrap")
            .withAdditionalProperties(additionalProperties)
            .build();

        assertTrue(Objects.equals(sqlWith.generateSql(), expectedSql));
    }
}
