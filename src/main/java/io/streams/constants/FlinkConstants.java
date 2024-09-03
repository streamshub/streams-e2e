/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.constants;

public interface FlinkConstants {
    String TEST_SQL_EXAMPLE_STATEMENT =
        "CREATE TABLE ProductInventoryTable ( product_id STRING, category STRING, stock STRING, rating STRING ) " +
            "WITH ( 'connector' = 'filesystem', 'path' = '/opt/flink/data/productInventory.csv', " +
            "'format' = 'csv', 'csv.ignore-parse-errors' = 'true' ); CREATE TABLE ClickStreamTable " +
            "( user_id STRING, product_id STRING, `event_time` TIMESTAMP(3) METADATA FROM 'timestamp', " +
            "WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND ) WITH ( 'connector' = 'kafka', " +
            "'topic' = 'flink.click.streams', 'properties.bootstrap.servers' = " +
            "'my-cluster-kafka-bootstrap.flink.svc:9092', 'properties.group.id' = 'click-stream-group', " +
            "'value.format' = 'avro-confluent', 'value.avro-confluent.schema-registry.url' = " +
            "'http://apicurio-registry-service.flink.svc:8080/apis/ccompat/v6', 'scan.startup.mode' = " +
            "'latest-offset' ); CREATE TABLE SalesRecordTable ( invoice_id STRING, user_id STRING, product_id STRING, " +
            "quantity STRING, unit_cost STRING, `purchase_time` TIMESTAMP(3) METADATA FROM 'timestamp', " +
            "WATERMARK FOR purchase_time AS purchase_time - INTERVAL '1' SECOND ) WITH ( 'connector' = 'kafka', " +
            "'topic' = 'flink.sales.records', 'properties.bootstrap.servers' = " +
            "'my-cluster-kafka-bootstrap.flink.svc:9092', 'properties.group.id' = 'sales-record-group', " +
            "'value.format' = 'avro-confluent', 'value.avro-confluent.schema-registry.url' = " +
            "'http://apicurio-registry-service.flink.svc:8080/apis/ccompat/v6', 'scan.startup.mode' = " +
            "'latest-offset' ); CREATE TABLE CsvSinkTable ( user_id STRING, top_product_ids STRING, " +
            "`event_time` TIMESTAMP(3), PRIMARY KEY(`user_id`) NOT ENFORCED ) WITH ( 'connector' = 'upsert-kafka', " +
            "'topic' = 'flink.recommended.products', 'properties.bootstrap.servers' = " +
            "'my-cluster-kafka-bootstrap.flink.svc:9092', 'properties.client.id' = " +
            "'recommended-products-producer-client', 'properties.transaction.timeout.ms' = '800000', " +
            "'key.format' = 'csv', 'value.format' = 'csv', 'value.fields-include' = 'ALL' ); CREATE TEMPORARY " +
            "VIEW clicked_products AS SELECT DISTINCT c.user_id, c.event_time, p.product_id, p.category " +
            "FROM ClickStreamTable AS c JOIN ProductInventoryTable AS p ON c.product_id = p.product_id; " +
            "CREATE TEMPORARY VIEW category_products AS SELECT cp.user_id, cp.event_time, p.product_id, " +
            "p.category, p.stock, p.rating, sr.user_id as purchased FROM clicked_products cp JOIN " +
            "ProductInventoryTable AS p ON cp.category = p.category LEFT JOIN SalesRecordTable sr ON " +
            "cp.user_id = sr.user_id AND p.product_id = sr.product_id WHERE p.stock > 0 GROUP BY p.product_id, " +
            "p.category, p.stock, cp.user_id, cp.event_time, sr.user_id, p.rating; CREATE TEMPORARY VIEW " +
            "top_products AS SELECT cp.user_id, cp.event_time, cp.product_id, cp.category, cp.stock, cp.rating, " +
            "cp.purchased, ROW_NUMBER() OVER (PARTITION BY cp.user_id ORDER BY cp.purchased DESC, cp.rating DESC) " +
            "AS rn FROM category_products cp; INSERT INTO CsvSinkTable SELECT user_id, LISTAGG(product_id, ',') " +
            "AS top_product_ids, TUMBLE_END(event_time, INTERVAL '5' SECOND) FROM top_products WHERE rn <= 6 GROUP " +
            "BY user_id, TUMBLE(event_time, INTERVAL '5' SECOND);";
}
