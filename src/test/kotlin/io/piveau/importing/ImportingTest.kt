package io.piveau.importing

import io.vertx.core.AsyncResult
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.junit5.Checkpoint
import io.vertx.junit5.Timeout
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import java.util.concurrent.TimeUnit

@DisplayName("Testing the importer")
@ExtendWith(VertxExtension::class)
@ExperimentalCoroutinesApi
@FlowPreview
internal class ImportingTest {

    @BeforeEach
    fun startImporter(vertx: Vertx, testContext: VertxTestContext) {
        vertx.deployVerticle(MainVerticle(), DeploymentOptions(), testContext.completing())
    }

    @Test
    @DisplayName("pipe receiving")
    @Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
    @Disabled
    fun sendPipe(vertx: Vertx, testContext: VertxTestContext) {
        val checkpoint = testContext.checkpoint(2)
        sendPipe("test-pipe.json", vertx, testContext, checkpoint)
    }

    private fun sendPipe(pipeFile: String, vertx: Vertx, testContext: VertxTestContext, checkpoint: Checkpoint) {
        vertx.fileSystem().readFile(pipeFile) { result: AsyncResult<Buffer?> ->
            if (result.succeeded()) {
                val pipe = JsonObject(result.result())
                val client = WebClient.create(vertx)
                client.post(8080, "localhost", "/pipe")
                    .putHeader("content-type", "application/json")
                    .sendJsonObject(pipe) {
                        if (it.succeeded() && it.result().statusCode() == 202) {
                            checkpoint.flag()
                        } else {
                            testContext.failNow(Throwable("Sent json failed"))
                        }
                    }
            } else {
                testContext.failNow(result.cause())
            }
        }
    }
}