package io.piveau.importing.rdf

import io.piveau.pipe.PipeContext
import io.piveau.rdf.RDFMimeTypes
import io.piveau.rdf.presentAs
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlin.coroutines.CoroutineContext

class ImportingRdfVerticle : CoroutineVerticle() {

    private lateinit var downloadSource: DownloadSource

    private var pulse: Long = 0

    override suspend fun start() {
        vertx.eventBus().consumer(ADDRESS) {
            launch(Dispatchers.IO) {
                handlePipe(it)
            }
        }

        val envStoreOptions = ConfigStoreOptions()
            .setType("env")
            .setConfig(
                JsonObject().put(
                    "keys",
                    JsonArray()
                        .add("PIVEAU_IMPORTING_SEND_LIST_DELAY")
                        .add("PIVEAU_IMPORTING_PREPROCESSING")
                        .add("PIVEAU_DEFAULT_PULSE")
                )
            )

        val config = ConfigRetriever.create(vertx, ConfigRetrieverOptions().addStore(envStoreOptions)).config.await()
        downloadSource = DownloadSource(vertx, WebClient.create(vertx), config)
        pulse = config.getLong("PIVEAU_DEFAULT_PULSE", 15)
    }

    private suspend fun handlePipe(message: Message<PipeContext>) {
        with(message.body()) {
            log.info("Import started.")

            val outputFormat = config.getString("outputFormat", RDFMimeTypes.NTRIPLES)
            val catalogue = config.getString("catalogue")

            val address = config.getString("address")
            val identifiers = mutableListOf<String>()

            downloadSource.pagesFlow(address, this)
                .cancellable()
                .flatMapConcat {
                    downloadSource.datasetsFlow(it, this)
                }
                .onEach {
                    delay(config.getLong("pulse", pulse))
                }
                .onCompletion {
                    when {
                        it != null -> setFailure(it)
                        else -> {
                            val dataInfo = JsonObject()
                                .put("content", "identifierList")
                                .put("catalogue", catalogue)
                            setResult(
                                JsonArray(identifiers).encode(),
                                "application/json",
                                dataInfo
                            ).forward()
                            log.info("Importing finished")
                        }
                    }
                }
                .collect { (dataset, dataInfo) ->
                    if (identifiers.contains(dataInfo.getString("identifier"))) {
                        log.warn("Duplicate dataset: {}", dataInfo.getString("identifier"))
                    }
                    identifiers.add(dataInfo.getString("identifier"))
                    dataInfo.put("counter", identifiers.size).put("catalogue", config.getString("catalogue"))
                    dataset.presentAs(outputFormat).let {
                        setResult(it, outputFormat, dataInfo).forward()
                        log.info("Data imported: {}", dataInfo)
                        log.debug("Data content: {}", it)
                    }
                    dataset.close()
                }
            setRunFinished()
        }
    }

    companion object {
        const val ADDRESS: String = "io.piveau.pipe.new.importing.rdf.queue"
    }

}