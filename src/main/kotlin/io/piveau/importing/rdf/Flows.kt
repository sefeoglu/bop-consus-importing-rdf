package io.piveau.importing.rdf

import io.piveau.pipe.PipeContext
import io.piveau.rdf.*
import io.piveau.utils.JenaUtils
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.ext.web.client.sendAwait
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.RDF

data class Page(val page: Model, val total: Int)
data class Dataset(val dataset: Model, val dataInfo: JsonObject)

@FlowPreview
class DownloadSource(private val client: WebClient, private val config: JsonObject) {

    private val preProcessing = config.getBoolean("PIVEAU_IMPORTING_PREPROCESSING", false)

    private fun pagesFlow(address: String, pipeContext: PipeContext): Flow<Page> = flow {
        var nextLink: String? = address
        val inputFormat = pipeContext.config.getString("inputFormat")
        val applyPreProcessing = pipeContext.config.getBoolean("preProcessing", preProcessing)
        val brokenHydra = config.getBoolean("brokenHydra", false)

        do {
            val response = client.getAbs(nextLink as String).sendAwait()
            nextLink = when (response.statusCode()) {
                in 200..299 -> {
                    val contentType = inputFormat ?: response.getHeader("Content-Type") ?: "application/rdf+xml"
                    if (contentType.isRDF) {

                        val page = if (applyPreProcessing) {
                            val (content, finalContentType) = preProcess(response.body().bytes, contentType, nextLink)
                            content.toByteArray().toModel(finalContentType)
                        } else {
                            response.body().bytes.toModel(contentType, nextLink)
                        }

                        val hydraPaging = HydraPaging.findPaging(page, if (brokenHydra) address else null)

                        emit(Page(page, hydraPaging.total))

                        hydraPaging.next
                    } else {
                        throw Throwable("${response.statusCode()} - ${response.statusMessage()}:\n${response.bodyAsString()}")
                    }
                }
                else -> throw Throwable("${response.statusCode()} - ${response.statusMessage()}:\n${response.bodyAsString()}")
            }
        } while (nextLink != null)
    }

    fun datasetsFlow(address: String, pipeContext: PipeContext): Flow<Dataset> = flow {
        coroutineScope {
            val removePrefix = pipeContext.config.getBoolean("removePrefix", false)
            val precedenceUriRef = pipeContext.config.getBoolean("precedenceUriRef", false)

            pagesFlow(address, pipeContext).collect { (page, total) ->
                page.listResourcesWithProperty(RDF.type, DCAT.Dataset).forEach {
                    JenaUtils.findIdentifier(it, removePrefix, precedenceUriRef)?.let { id ->
                        val dataInfo = JsonObject()
                            .put("total", total)
                            .put("identifier", id)

                        emit(Dataset(it.extractAsModel() ?: ModelFactory.createDefaultModel(), dataInfo))

                    } ?: pipeContext.log().warn("Could not extract an identifier from {}", it.uri)
                }
            }
        }
    }

}

val String.isRDF: Boolean
    get() = this in listOf(
        "application/rdf+xml",
        "application/ld+json",
        "application/n-triples",
        "text/turtle",
        "text/n3"
    )

