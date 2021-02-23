package io.piveau.importing.rdf

import io.piveau.pipe.PipeContext
import io.piveau.rdf.asString
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flatMapConcat
import kotlinx.coroutines.flow.onCompletion
import kotlin.properties.Delegates

@FlowPreview
@ExperimentalCoroutinesApi
class ImportingRdfVerticle : CoroutineVerticle() {

    private lateinit var downloadSource: DownloadSource
    private var defaultDelay by Delegates.notNull<Long>()

    override suspend fun start() {
        vertx.eventBus().consumer(ADDRESS, this::handlePipe)

        val envStoreOptions = ConfigStoreOptions()
            .setType("env")
            .setConfig(
                JsonObject().put(
                    "keys",
                    JsonArray().add("PIVEAU_IMPORTING_SEND_LIST_DELAY").add("PIVEAU_IMPORTING_PREPROCESSING")
                )
            )

        val config = ConfigRetriever.create(vertx, ConfigRetrieverOptions().addStore(envStoreOptions)).config.await()
        downloadSource = DownloadSource(vertx, WebClient.create(vertx), config)
        defaultDelay = config.getLong("PIVEAU_IMPORTING_SEND_LIST_DELAY", 8000L)
    }

    private fun handlePipe(message: Message<PipeContext>) {
        GlobalScope.launch(Dispatchers.IO) {
            with(message.body()) {
                log.info("Import started.")

                val outputFormat = config.getString("outputFormat", "application/n-triples")
                val delay = config.getLong("sendListDelay", defaultDelay)
                val catalogue = config.getString("catalogue")

                val address = config.getString("address")
                val identifiers = mutableListOf<String>()

                downloadSource.pagesFlow(address, this)
                    .flatMapConcat {
                        downloadSource.datasetsFlow(it, this)
                    }
                    .onCompletion {
                        when {
                            it != null -> setFailure(it)
                            else -> {
                                delay(delay)
                                val dataInfo = JsonObject()
                                    .put("content", "identifierList")
                                    .put("catalogue", catalogue)
                                setResult(
                                    JsonArray(identifiers).encodePrettily(),
                                    "application/json",
                                    dataInfo
                                ).forward()
                                log.info("Importing finished")
                            }
                        }
                    }
                    .collect { (dataset, dataInfo) ->
                        identifiers.add(dataInfo.getString("identifier"))
                        dataInfo.put("counter", identifiers.size).put("catalogue", config.getString("catalogue"))
                        dataset.asString(outputFormat).let {
                            setResult(it, outputFormat, dataInfo).forward()
                            log.info("Data imported: {}", dataInfo)
                            log.debug("Data content: {}", it)
                        }
                        dataset.close()
                    }

            }
        }
    }

    companion object {
        const val ADDRESS: String = "io.piveau.pipe.new.importing.rdf.queue"
    }

}