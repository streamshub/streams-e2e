/*
 * Copyright Skodjob authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.utils;

import io.streams.Environment;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.nio.file.Path;
import java.nio.file.Paths;

public class TestUtils {
    public static Path getLogPath(String folderName, ExtensionContext context) {
        String testMethod = context.getDisplayName();
        String testClassName = context.getTestClass().map(Class::getName).orElse("NOCLASS");
        return getLogPath(folderName, testClassName, testMethod);
    }

    public static Path getLogPath(String folderName, TestInfo info) {
        String testMethod = info.getDisplayName();
        String testClassName = info.getTestClass().map(Class::getName).orElse("NOCLASS");
        return getLogPath(folderName, testClassName, testMethod);
    }

    public static Path getLogPath(String folderName, String testClassName, String testMethod) {
        Path path = Environment.LOG_DIR.resolve(Paths.get(folderName, testClassName));
        if (testMethod != null) {
            path = path.resolve(testMethod.replace("(", "").replace(")", ""));
        }
        return path;
    }
}
