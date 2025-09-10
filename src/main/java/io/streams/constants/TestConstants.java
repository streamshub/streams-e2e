/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.constants;

import io.streams.Environment;

import java.nio.file.Path;
import java.nio.file.Paths;

public interface TestConstants {
    String LOG_COLLECT_LABEL = "streams-e2e";
    Path YAML_MANIFEST_PATH = Paths.get(Environment.USER_PATH, "operator-install-files");

    String ALWAYS_IMAGE_PULL_POLICY = "Always";
    String IF_NOT_PRESENT_IMAGE_PULL_POLICY = "IfNotPresent";

    String STRIMZI_TEST_CLIENTS_IMAGE = "quay.io/strimzi-test-clients/test-clients:latest-kafka-4.0.0";

    String STRIMZI_TEST_CLIENTS_LABEL_KEY = "strimzi-test-clients";
    String STRIMZI_TEST_CLIENTS_LABEL_VALUE = "true";

    // Labels
    String APP_POD_LABEL = "app";
    String DEPLOYMENT_TYPE = "deployment-type";
}
