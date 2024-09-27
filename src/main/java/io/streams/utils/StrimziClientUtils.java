/*
 * Copyright streamshub authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.utils;

import io.apicurio.registry.rest.v2.beans.IfExists;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.config.IdOption;

/**
 * Helper methods for strimzi kafka clients
 */
public class StrimziClientUtils {

    /**
     * Returns strimzi kafka client additional config for apicurio registry connection with auto create record
     *
     * @param serializer serializer name
     * @param registryUrl url for registry
     * @return strimzi kafka client formated additional properties
     */
    public static String getApicurioAdditionalProperties(String serializer, String registryUrl) {
        return "value.serializer" + "=" + serializer +
            System.lineSeparator() +
            SerdeConfig.REGISTRY_URL + "=" +
            registryUrl +
            System.lineSeparator() +
            SerdeConfig.ENABLE_CONFLUENT_ID_HANDLER + "=" + Boolean.TRUE +
            System.lineSeparator() +
            SerdeConfig.USE_ID + "=" + IdOption.contentId +
            System.lineSeparator() +
            SerdeConfig.ENABLE_HEADERS + "=" + Boolean.FALSE +
            System.lineSeparator() +
            SerdeConfig.AUTO_REGISTER_ARTIFACT + "=" + Boolean.TRUE +
            System.lineSeparator() +
            SerdeConfig.AUTO_REGISTER_ARTIFACT_IF_EXISTS + "=" + IfExists.RETURN.name();
    }
}
