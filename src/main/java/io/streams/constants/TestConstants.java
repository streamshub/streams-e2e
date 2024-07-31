/*
 * Copyright Skodjob authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.constants;

import io.streams.Environment;

import java.nio.file.Path;
import java.nio.file.Paths;

public interface TestConstants {
    String LOG_COLLECT_LABEL = "streams-e2e";
    Path YAML_MANIFEST_PATH = Paths.get(Environment.USER_PATH, "operator-install-files");
}
