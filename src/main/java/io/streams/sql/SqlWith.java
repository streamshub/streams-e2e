/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.sql;

import io.sundr.builder.annotations.Buildable;

import java.security.InvalidParameterException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

@Buildable(editableEnabled = false, builderPackage = "io.fabric8.kubernetes.api.builder")
public class SqlWith {
    private String sqlStatement;
    private String connector;
    private String bootstrapServer;
    private String topic;
    private Map<String, String> additionalProperties = new HashMap<>();

    public String getSqlStatement() {
        return sqlStatement;
    }

    public void setSqlStatement(String sqlStatement) {
        if (sqlStatement.isEmpty()) {
            throw new InvalidParameterException("sqlStatement cannot be empty!");
        }
        this.sqlStatement = sqlStatement;
    }

    public String getConnector() {
        return connector;
    }

    public void setConnector(String connector) {
        if (connector == null || connector.isEmpty()) {
            throw new InvalidParameterException("Connector cannot be empty!");
        }
        this.connector = connector;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Map<String, String> getAdditionalProperties() {
        return Collections.unmodifiableMap(additionalProperties);
    }

    public void setAdditionalProperties(Map<String, String> additionalProperties) {
        this.additionalProperties = (additionalProperties == null || additionalProperties.isEmpty())
            ? new HashMap<>() : additionalProperties;
    }

    public String generateSql() {
        StringJoiner withClause = new StringJoiner(", ", "WITH (", ")");
        // Add connector
        withClause.add("'connector' = '" + connector + "'");
        // Add Kafka specific info if set
        if (bootstrapServer != null && !bootstrapServer.isEmpty()) {
            withClause.add("'properties.bootstrap.servers' = '" + bootstrapServer + "'");
        }
        if (topic != null && !topic.isEmpty()) {
            withClause.add("'topic' = '" + topic + "'");
        }
        // Add additional properties
        if (additionalProperties != null) {
            additionalProperties.forEach((key, value) -> withClause.add("'" + key + "' = '" + value + "'"));
        }

        return sqlStatement + " " + withClause + ";";
    }
}
