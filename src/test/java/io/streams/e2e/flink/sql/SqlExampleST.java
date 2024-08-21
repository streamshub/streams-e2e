package io.streams.e2e.flink.sql;

import io.streams.e2e.Abstract;
import io.streams.operators.manifests.ApicurioRegistryManifestInstaller;
import io.streams.operators.manifests.CertManagerManifestInstaller;
import io.streams.operators.manifests.FlinkManifestInstaller;
import io.streams.operators.manifests.StrimziManifestInstaller;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static io.streams.constants.TestTags.SQL_EXAMPLE;

@Tag(SQL_EXAMPLE)
public class SqlExampleST extends Abstract {

    @BeforeAll
    void prepareOperators() throws IOException {
        CompletableFuture.allOf(
            StrimziManifestInstaller.install(),
            CertManagerManifestInstaller.install(),
            FlinkManifestInstaller.install(),
            ApicurioRegistryManifestInstaller.install()
        ).join();
    }

    @Test
    void testFlinkSqlExample() {
        //TODO
    }
}
