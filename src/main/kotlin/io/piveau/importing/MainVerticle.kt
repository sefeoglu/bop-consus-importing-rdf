package io.piveau.importing

import io.piveau.importing.rdf.ImportingRdfVerticle
import io.piveau.pipe.connector.PipeConnector
import io.vertx.core.DeploymentOptions
import io.vertx.core.Launcher
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import java.nio.file.FileAlreadyExistsException

class MainVerticle : CoroutineVerticle() {

    override suspend fun start() {

        if (!vertx.fileSystem().exists("tmp").await()) {
            vertx.fileSystem().mkdir("tmp").await()
        } else if (!vertx.fileSystem().props("tmp").await().isDirectory) {
            throw FileAlreadyExistsException("tmp")
        }

        vertx.deployVerticle(ImportingRdfVerticle::class.java, DeploymentOptions().setWorker(true)).await()

        PipeConnector.create(vertx, DeploymentOptions()).await().publishTo(ImportingRdfVerticle.ADDRESS)
    }

}

fun main(args: Array<String>) {
    Launcher.executeCommand("run", *(args.plus(MainVerticle::class.java.name)))
}
