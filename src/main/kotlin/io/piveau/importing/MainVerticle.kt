package io.piveau.importing

import io.piveau.importing.rdf.ImportingRdfVerticle
import io.piveau.pipe.connector.PipeConnector
import io.vertx.core.DeploymentOptions
import io.vertx.core.Launcher
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.file.existsAwait
import io.vertx.kotlin.core.file.mkdirAwait
import io.vertx.kotlin.core.file.propsAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitResult
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import java.nio.file.FileAlreadyExistsException

@FlowPreview
@ExperimentalCoroutinesApi
class MainVerticle : CoroutineVerticle() {

    override suspend fun start() {

        if (!vertx.fileSystem().existsAwait("tmp")) {
            vertx.fileSystem().mkdirAwait("tmp")
        } else if (!vertx.fileSystem().propsAwait("tmp").isDirectory) {
            throw FileAlreadyExistsException("tmp")
        }

        vertx.deployVerticleAwait(ImportingRdfVerticle::class.qualifiedName ?: "", DeploymentOptions().setWorker(true))

        awaitResult<PipeConnector> { PipeConnector.create(vertx, DeploymentOptions(), it) }.publishTo(
            ImportingRdfVerticle.ADDRESS
        )
    }

}

@FlowPreview
@ExperimentalCoroutinesApi
fun main(args: Array<String>) {
    Launcher.executeCommand("run", *(args.plus(MainVerticle::class.java.name)))
}
