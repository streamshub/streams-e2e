/*
 * Copyright Skodjob authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.streams.operands.apicurio.templates;

import io.apicurio.registry.operator.api.v1.model.ApicurioRegistryBuilder;

/**
 * Resources for apicurio registry
 */
public class ApicurioRegistryTemplate {

    public static ApicurioRegistryBuilder defaultApicurioRegistry(String name, String namespace) {
        return new ApicurioRegistryBuilder()
            .withNewMetadata()
            .withName(name)
            .withNamespace(namespace)
            .endMetadata()
            .withNewSpec()
            .withNewConfiguration()
            .withPersistence("mem")
            .endConfiguration()
            .endSpec();
    }
}
