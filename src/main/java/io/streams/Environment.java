/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams;

import io.skodjob.testframe.environment.TestEnvironmentVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Environment and config store
 */
public class Environment {
    private static final Logger LOGGER = LoggerFactory.getLogger(Environment.class);
    private static final TestEnvironmentVariables ENVIRONMENT_VARIABLES = new TestEnvironmentVariables();
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm");
    public static final String USER_PATH = System.getProperty("user.dir");

    /**
     * Root log dir where related stuff to test run will be stored
     */
    public static final Path LOG_DIR = ENVIRONMENT_VARIABLES.getOrDefault("LOG_DIR",
            Paths::get, Paths.get(USER_PATH, "target", "logs"))
        .resolve("test-run-" + DATE_FORMAT.format(LocalDateTime.now()));

    /**
     * Flink default version passed to flink deployment
     */
    public static final String FLINK_VERSION =
        ENVIRONMENT_VARIABLES.getOrDefault("FLINK_VERSION", "");

    /**
     * Image of flink sql runner, default is latest upstream from quay.io
     */
    public static final String FLINK_SQL_RUNNER_IMAGE =
        ENVIRONMENT_VARIABLES.getOrDefault("SQL_RUNNER_IMAGE", "");

    /**
     * Flink operator bundle image to install operator using operator sdk, default is empty.
     */
    public static final String FLINK_OPERATOR_BUNDLE_IMAGE = ENVIRONMENT_VARIABLES.getOrDefault(
        "FLINK_OPERATOR_BUNDLE_IMAGE",
        "");

    /**
     * Strimzi operator bundle image to install operator using operator sdk, default is empty.
     */
    public static final String STRIMZI_OPERATOR_BUNDLE_IMAGE = ENVIRONMENT_VARIABLES.getOrDefault(
        "STRIMZI_OPERATOR_BUNDLE_IMAGE",
        "");

    /**
     * Use redhat catalog to install strimzi operator
     */
    public static final boolean INSTALL_STRIMZI_FROM_RH_CATALOG = ENVIRONMENT_VARIABLES.getOrDefault(
        "INSTALL_STRIMZI_FROM_RH_CATALOG",
        Boolean::valueOf,
        false);

    /**
     * Use redhat catalog to install apicurio operator
     */
    public static final boolean INSTALL_APICURIO_FROM_RH_CATALOG = ENVIRONMENT_VARIABLES.getOrDefault(
        "INSTALL_APICURIO_FROM_RH_CATALOG",
        Boolean::valueOf,
        false);

    /**
     * Use redhat catalog to install cert-manager operator
     */
    public static final boolean INSTALL_CERT_MANAGER_FROM_RH_CATALOG = ENVIRONMENT_VARIABLES.getOrDefault(
        "INSTALL_CERT_MANAGER_FROM_RH_CATALOG",
        Boolean::valueOf,
        false);

    /**
     * Prints configured environment variables or values from config
     */
    public static void printEnvVars() {
        LOGGER.info("Streams-e2e environment variables");
        ENVIRONMENT_VARIABLES.logEnvironmentVariables();
    }

    /**
     * Save config.yaml file from all env vars and values from config.yaml if it is set
     *
     * @throws IOException when saving is not possible
     */
    public static void saveConfig() throws IOException {
        ENVIRONMENT_VARIABLES.saveConfigurationFile(LOG_DIR.toString());
    }
}
