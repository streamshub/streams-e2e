/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.unit;

import io.streams.sql.SqlWith;
import io.streams.sql.SqlWithBuilder;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.RenderKeywordCase;
import org.jooq.conf.RenderQuotedNames;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class SqlWithTest {
    @Test
    void testCreateTableWith() {
        String expectedSql = "CREATE TABLE ProductInventoryTable (product_id varchar(255), category varchar(255), " +
            "stock varchar(255), rating varchar(255)) WITH ('connector' = 'kafka', " +
            "'properties.bootstrap.servers' = 'fake-bootstrap', 'topic' = 'fake-topic', 'avro.config' = 'apicurio-registry-service');";

        Settings settings = new Settings()
            .withRenderFormatted(false) // Do not add new lines for pretty print SQL
            .withRenderKeywordCase(RenderKeywordCase.UPPER)
            .withRenderQuotedNames(RenderQuotedNames.NEVER); // Render names as-is without quoting

        DSLContext dsl = DSL.using(SQLDialect.DEFAULT, settings);

        // Generate a SQL statement using JOOQ
        String createTableSQL = dsl.createTable("ProductInventoryTable")
            .column("product_id", SQLDataType.VARCHAR.length(255))
            .column("category", SQLDataType.VARCHAR.length(255))
            .column("stock", SQLDataType.VARCHAR.length(255))
            .column("rating", SQLDataType.VARCHAR.length(255))
            .getSQL();

        Map<String, String> additionalProperties = Map.of("avro.config", "apicurio-registry-service");
        SqlWith sqlWith = new SqlWithBuilder()
            .withSqlStatement(createTableSQL)
            .withConnector("kafka")
            .withBootstrapServer("fake-bootstrap")
            .withTopic("fake-topic")
            .withAdditionalProperties(additionalProperties)
            .build();

        String finalSql = sqlWith.generateSql();

        assertTrue(finalSql.equals(expectedSql));
    }
}
