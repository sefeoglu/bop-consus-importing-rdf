package io.piveau.importing;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

@DisplayName("Testing the importer")
@ExtendWith(VertxExtension.class)
class ImportingTest {

    @BeforeEach
    void startImporter(Vertx vertx, VertxTestContext testContext) {
        vertx.deployVerticle(new MainVerticle(), testContext.completing());
    }

    @Test
    @DisplayName("pipe receiving")
    void sendPipe(Vertx vertx, VertxTestContext testContext) {
        vertx.fileSystem().readFile("src/test/resources/test-pipe.json", result -> {
            if (result.succeeded()) {
                JsonObject pipe = new JsonObject(result.result());
                testContext.completeNow();
            } else {
                testContext.failNow(result.cause());
            }
        });
    }

}
