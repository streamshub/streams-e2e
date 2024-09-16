/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams;

import io.skodjob.testframe.environment.TestEnvironmentVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Environment {
    private static final Logger LOGGER = LoggerFactory.getLogger(Environment.class);

    private static final TestEnvironmentVariables ENVIRONMENT_VARIABLES = new TestEnvironmentVariables();
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm");
    public static final String USER_PATH = System.getProperty("user.dir");


    public static final Path LOG_DIR = ENVIRONMENT_VARIABLES.getOrDefault("LOG_DIR",
            Paths::get, Paths.get(USER_PATH, "target", "logs"))
        .resolve("test-run-" + DATE_FORMAT.format(LocalDateTime.now()));

    public static final String FLINK_SQL_RUNNER_IMAGE =
        ENVIRONMENT_VARIABLES.getOrDefault("SQL_RUNNER_IMAGE",
            "quay.io/streamshub/flink-sql-runner:latest");

    public static void printEnvVars() {
        LOGGER.info("Streams-e2e environment variables");
        ENVIRONMENT_VARIABLES.logEnvironmentVariables();
    }
}
