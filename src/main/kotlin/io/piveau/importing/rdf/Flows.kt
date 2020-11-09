package io.piveau.importing.rdf

import io.piveau.pipe.PipeContext
import io.piveau.rdf.*
import io.piveau.utils.JenaUtils
import io.vertx.core.Vertx
import io.vertx.core.file.OpenOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.codec.BodyCodec
import io.vertx.kotlin.core.file.closeAwait
import io.vertx.kotlin.core.file.createTempFileAwait
import io.vertx.kotlin.core.file.deleteAwait
import io.vertx.kotlin.core.file.openAwait
import io.vertx.kotlin.coroutines.awaitBlocking
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
import java.io.File

data class Page(val page: Model, val total: Int)
data class Dataset(val dataset: Model, val dataInfo: JsonObject)

@FlowPreview
class DownloadSource(private val vertx: Vertx, private val client: WebClient, private val config: JsonObject) {

    private val preProcessing = config.getBoolean("PIVEAU_IMPORTING_PREPROCESSING", false)

    private fun pagesFlow(address: String, pipeContext: PipeContext): Flow<Page> = flow {
        var nextLink: String? = address
        val inputFormat = pipeContext.config.getString("inputFormat")
        val applyPreProcessing = pipeContext.config.getBoolean("preProcessing", preProcessing)
        val brokenHydra = pipeContext.config.getBoolean("brokenHydra", false)

        do {
            val tmpFileName: String = vertx.fileSystem().createTempFileBlocking("tmp", "piveau", ".tmp", null)
            val stream = vertx.fileSystem().openAwait(tmpFileName, OpenOptions().setWrite(true))

            val response = client.getAbs(nextLink as String).`as`(BodyCodec.pipe(stream, true)).sendAwait()

            nextLink = when (response.statusCode()) {
                in 200..299 -> {
                    val contentType = inputFormat ?: response.getHeader("Content-Type") ?: "application/rdf+xml"
                    if (contentType.isRDF) {

                        val (fileName, content, finalContentType) = if (applyPreProcessing) {
                            val output = vertx.fileSystem().createTempFileBlocking("tmp", "piveau", ".tmp", null)
                            val (outputStream, finalContentType) = preProcess(File(tmpFileName).inputStream(), File(output).outputStream(), contentType, address)
                            outputStream.close()
                            Triple(output, File(output).inputStream(), finalContentType)
                        } else {
                            Triple("", File(tmpFileName).inputStream(), contentType)
                        }

                        val page = JenaUtils.read(content, finalContentType)
                        content.close()

                        if (fileName.isNotBlank()) {
                            vertx.fileSystem().deleteAwait(fileName)
                        }

                        val hydraPaging = HydraPaging.findPaging(page, if (brokenHydra) address else null)

                        val next = hydraPaging.next

                        emit(Page(page, hydraPaging.total))

                        next

                    } else {
                        throw Throwable("$nextLink: Content-Type $contentType is not an RDF content type. Content:\n${response.bodyAsString()}")
                    }
                }
                else -> throw Throwable("$nextLink: ${response.statusCode()} - ${response.statusMessage()}\n${response.bodyAsString()}")
            }

            vertx.fileSystem().deleteAwait(tmpFileName)
        } while (nextLink != null)
    }

    fun datasetsFlow(address: String, pipeContext: PipeContext): Flow<Dataset> = flow {
        coroutineScope {
            val removePrefix = pipeContext.config.getBoolean("removePrefix", false)
            val precedenceUriRef = pipeContext.config.getBoolean("precedenceUriRef", false)

            if (pipeContext.config.getBoolean("useTempFile", false)) {
                throw NotImplementedError("Using temp file is currently not supported")
            } else {
                pagesFlow(address, pipeContext).collect { (page, total) ->
                    page.listResourcesWithProperty(RDF.type, DCAT.Dataset).forEach {
                        JenaUtils.findIdentifier(it, removePrefix, precedenceUriRef)?.let { id ->
                            val dataInfo = JsonObject()
                                .put("total", total)
                                .put("identifier", id)

                            emit(Dataset(it.extractAsModel() ?: ModelFactory.createDefaultModel(), dataInfo))

                        } ?: pipeContext.log.warn("Could not extract an identifier from {}", it.uri)
                    }
                    page.close()
                }
            }
        }
    }

}
