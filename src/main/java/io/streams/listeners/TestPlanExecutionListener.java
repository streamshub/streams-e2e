/*
 * Copyright Skodjob authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.listeners;

import io.streams.Environment;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPlanExecutionListener implements TestExecutionListener {
    static final Logger LOGGER = LoggerFactory.getLogger(TestPlanExecutionListener.class);

    public void testPlanExecutionStarted(TestPlan testPlan) {
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
        LOGGER.info("                        Test run started");
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
        Environment.printEnvVars();
    }

    public void testPlanExecutionFinished(TestPlan testPlan) {
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
        LOGGER.info("                        Test run finished");
        LOGGER.info("=======================================================================");
        LOGGER.info("=======================================================================");
    }
}
