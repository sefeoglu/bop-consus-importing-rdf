package io.piveau.importing;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

@DisplayName("Testing the importer")
@ExtendWith(VertxExtension.class)
public class ImportingTest {

    @BeforeEach
    public void startImporter(Vertx vertx, VertxTestContext testContext) {
        vertx.deployVerticle(new MainVerticle(), testContext.succeeding(response -> testContext.verify(testContext::completeNow)));
    }

    @Test
    @DisplayName("pipe receiving")
    public void sendPipe(Vertx vertx, VertxTestContext testContext) {
        Path path = FileSystems.getDefault().getPath("src/test/resources/test-pipe.json");
        try {
            String pipeString = new String(Files.readAllBytes(path));
            JsonObject pipe = new JsonObject(pipeString);
            WebClient client = WebClient.create(vertx);
            client.post(8080, "localhost", "/pipe")
                    .putHeader("content-type", "application/json")
                    .sendJsonObject(pipe, response -> {
                        if (response.succeeded()) {

                        } else {

                        }
                    });
        } catch (IOException e) {
            testContext.failNow(e);
        }

        try {
            testContext.awaitCompletion(5, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
