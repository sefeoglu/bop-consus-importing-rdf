package io.piveau.importing

import io.piveau.importing.rdf.NewImportingRdfVerticle
import io.piveau.pipe.connector.PipeConnector
import io.vertx.core.DeploymentOptions
import io.vertx.core.Launcher
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitResult
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview

@FlowPreview
@ExperimentalCoroutinesApi
class MainVerticle : CoroutineVerticle() {

    override suspend fun start() {
        awaitResult<String> {
            vertx.deployVerticle(
                NewImportingRdfVerticle::class.java, DeploymentOptions().setWorker(true), it
            )
        }
        awaitResult<PipeConnector> { PipeConnector.create(vertx, DeploymentOptions(), it) }.publishTo(
            NewImportingRdfVerticle.ADDRESS
        )
    }

}

@FlowPreview
@ExperimentalCoroutinesApi
fun main(args: Array<String>) {
    Launcher.executeCommand("run", *(args.plus(MainVerticle::class.java.name)))
}
