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
public class ImportingTest {

    @BeforeEach
    public void startImporter(Vertx vertx, VertxTestContext testContext) {
        vertx.deployVerticle(new MainVerticle(), testContext.completing());
    }

    @Test
    @DisplayName("pipe receiving")
    @Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
    public void sendPipe(Vertx vertx, VertxTestContext testContext) {
        vertx.fileSystem().readFile("src/test/resources/test-pipe.json", result -> {
            if (result.succeeded()) {
                JsonObject pipe = new JsonObject(result.result());
                WebClient client = WebClient.create(vertx);
                client.post(8080, "localhost", "/pipe")
                        .putHeader("content-type", "application/json")
                        .sendJsonObject(pipe, testContext.succeeding(response -> testContext.verify(() -> {
                            if (response.statusCode() == 202) {
//                            testContext.completeNow();
                            } else {
                                testContext.failNow(new Throwable(response.statusMessage()));
                            }
                        })));
            } else {
                testContext.failNow(result.cause());
            }
        });
    }

}
