package io.streams;

import io.skodjob.testframe.environment.TestEnvironmentVariables;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Environment {
    private static final TestEnvironmentVariables ENVIRONMENT_VARIABLES = new TestEnvironmentVariables();
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm");
    private static final String USER_PATH = System.getProperty("user.dir");


    public static final Path LOG_DIR = ENVIRONMENT_VARIABLES.getOrDefault("LOG_DIR",
        Paths::get, Paths.get(USER_PATH, "target", "logs"))
        .resolve("test-run-" + DATE_FORMAT.format(LocalDateTime.now()));

    static {
        ENVIRONMENT_VARIABLES.logEnvironmentVariables();
    }
}
