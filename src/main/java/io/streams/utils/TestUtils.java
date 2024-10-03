/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.utils;

import io.streams.Environment;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

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

    /**
     * Decodes a byte[] from Base64.
     *
     * @param data    String that should be decoded.
     *
     * @return        Plain data in byte[].
     */
    public static byte[] decodeBytesFromBase64(String data)  {
        return Base64.getDecoder().decode(data);
    }

    /**
     * Decodes a byte[] from Base64.
     *
     * @param data    byte[] that should be decoded.
     *
     * @return        Plain data in byte[].
     */
    public static byte[] decodeBytesFromBase64(byte[] data)  {
        return Base64.getDecoder().decode(data);
    }

    /**
     * Decodes a String from Base64.
     *
     * @param data    String that should be decoded.
     *
     * @return        Plain data using US ASCII charset.
     */
    public static String decodeFromBase64(String data)  {
        return decodeFromBase64(data, StandardCharsets.US_ASCII);
    }

    /**
     * Decodes a String from Base64.
     *
     * @param data    String that should be decoded.
     * @param charset The charset for the return string
     *
     * @return        Plain data using specified charset.
     */
    public static String decodeFromBase64(String data, Charset charset)  {
        return new String(decodeBytesFromBase64(data), charset);
    }
}
