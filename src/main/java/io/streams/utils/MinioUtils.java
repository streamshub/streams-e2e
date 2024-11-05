/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.utils;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.skodjob.testframe.TestFrameConstants;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.skodjob.testframe.wait.Wait;
import io.streams.constants.TestConstants;
import io.streams.operands.minio.SetupMinio;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MinioUtils {
    private static final Logger LOGGER = LogManager.getLogger(MinioUtils.class);

    private MinioUtils() {

    }

    /**
     * Collect data from Minio about usage of a specific bucket
     *
     * @param namespace  Name of the Namespace where the Minio Pod is running
     * @param bucketName Name of the bucket for which we want to get info about its size
     * @return Overall statistics about the bucket in String format
     */
    public static String getBucketSizeInfo(String namespace, String bucketName) {
        final LabelSelector labelSelector = new LabelSelectorBuilder()
            .withMatchLabels(Map.of(TestConstants.APP_POD_LABEL, SetupMinio.MINIO))
            .build();
        final String minioPod = KubeResourceManager.getKubeClient()
            .listPods(namespace, labelSelector)
            .get(0)
            .getMetadata()
            .getName();

        return KubeResourceManager.getKubeCmdClient()
            .inNamespace(namespace)
            .execInPod(minioPod,
                "mc",
                "stat",
                "local/" + bucketName)
            .out();

    }

    /**
     * Parse out total size of bucket from the information about usage.
     *
     * @param bucketInfo String containing all stat info about bucket
     * @return Map consists of parsed size and it's unit
     */
    private static Map<String, Object> parseTotalSize(String bucketInfo) {
        Pattern pattern = Pattern.compile("Total size:\\s*(?<size>[\\d.]+)\\s*(?<unit>.*)");
        Matcher matcher = pattern.matcher(bucketInfo);

        if (matcher.find()) {
            return Map.of("size", Double.parseDouble(matcher.group("size")), "unit", matcher.group("unit"));
        } else {
            throw new IllegalArgumentException("Total size not found in the provided string");
        }
    }

    /**
     * Wait until size of the bucket is not 0 B.
     *
     * @param namespace  Minio location
     * @param bucketName bucket name
     */
    public static void waitForDataInMinio(String namespace, String bucketName) {
        Wait.until("data sync from Kafka to Minio",
            TestFrameConstants.GLOBAL_POLL_INTERVAL_MEDIUM,
            TestFrameConstants.GLOBAL_TIMEOUT,
            () -> {
                String bucketSizeInfo = getBucketSizeInfo(namespace, bucketName);
                Map<String, Object> parsedSize = parseTotalSize(bucketSizeInfo);
                double bucketSize = (Double) parsedSize.get("size");
                LOGGER.info("Collected bucket size: {} {}", bucketSize, parsedSize.get("unit"));
                LOGGER.debug("Collected bucket info:\n{}", bucketSizeInfo);

                return bucketSize > 0;
            });
    }

    /**
     * Wait until size of the bucket is 0 B.
     *
     * @param namespace  Minio location
     * @param bucketName bucket name
     */
    public static void waitForNoDataInMinio(String namespace, String bucketName) {
        Wait.until("data deletion in Minio", TestFrameConstants.GLOBAL_POLL_INTERVAL_MEDIUM, TestFrameConstants.GLOBAL_TIMEOUT, () -> {
            String bucketSizeInfo = getBucketSizeInfo(namespace, bucketName);
            Map<String, Object> parsedSize = parseTotalSize(bucketSizeInfo);
            double bucketSize = (Double) parsedSize.get("size");
            LOGGER.info("Collected bucket size: {} {}", bucketSize, parsedSize.get("unit"));
            LOGGER.debug("Collected bucket info:\n{}", bucketSizeInfo);

            return bucketSize == 0;
        });
    }
}
