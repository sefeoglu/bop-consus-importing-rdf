package io.piveau.importing.rdf

import io.piveau.pipe.PipeContext
import io.piveau.rdf.*
import io.piveau.utils.JenaUtils
import io.vertx.circuitbreaker.CircuitBreaker
import io.vertx.circuitbreaker.CircuitBreakerOptions
import io.vertx.core.Vertx
import io.vertx.core.buffer.Buffer
import io.vertx.core.file.OpenOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.codec.BodyCodec
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.RDF
import java.io.File

data class Page(val page: Model, val total: Int)
data class Dataset(val dataset: Model, val dataInfo: JsonObject)

@FlowPreview
class DownloadSource(private val vertx: Vertx, private val client: WebClient, config: JsonObject) {

    private val preProcessing = config.getBoolean("PIVEAU_IMPORTING_PREPROCESSING", false)

    private val circuitBreaker = CircuitBreaker
        .create("", vertx, CircuitBreakerOptions().setMaxRetries(2).setTimeout(120000))
        .retryPolicy { it * 2000L }

    fun pagesFlow(address: String, pipeContext: PipeContext): Flow<Page> = flow {
        var nextLink: String? = address
        val accept = pipeContext.config.getString("accept")
        val inputFormat = pipeContext.config.getString("inputFormat")
        val applyPreProcessing = pipeContext.config.getBoolean("preProcessing", preProcessing)
        val brokenHydra = pipeContext.config.getBoolean("brokenHydra", false)

        do {
            val tmpFileName: String = vertx.fileSystem().createTempFileBlocking("tmp", "piveau", ".tmp", null)
            val stream = vertx.fileSystem().open(tmpFileName, OpenOptions().setWrite(true)).await()

            val request = client.getAbs(nextLink as String).`as`(BodyCodec.pipe(stream, true))
            if (accept != null) {
                request.putHeader("Accept", accept)
            }

            val response = circuitBreaker.execute<HttpResponse<Buffer>> {
                    request.timeout(60000).send()
                }.await()

            nextLink = when (response.statusCode()) {
                in 200..299 -> {
                    val contentType = inputFormat ?: response.getHeader("Content-Type") ?: "application/rdf+xml"
                    if (contentType.isRDF) {

                        val (fileName, content, finalContentType) = if (applyPreProcessing) {
                            val output = vertx.fileSystem().createTempFileBlocking("tmp", "piveau", ".tmp", null)
                            val (outputStream, finalContentType) = preProcess(
                                File(tmpFileName).inputStream(),
                                File(output).outputStream(),
                                contentType,
                                address
                            )
                            Triple(output, File(output).inputStream(), finalContentType)
                        } else {
                            Triple("", File(tmpFileName).inputStream(), contentType)
                        }

                        val page = JenaUtils.read(content, finalContentType)

                        if (fileName.isNotBlank()) {
                            vertx.fileSystem().delete(fileName).await()
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

            vertx.fileSystem().delete(tmpFileName).await()
        } while (nextLink != null)
    }

    fun datasetsFlow(page: Page, pipeContext: PipeContext): Flow<Dataset> = flow {
        val removePrefix = pipeContext.config.getBoolean("removePrefix", false)
        val precedenceUriRef = pipeContext.config.getBoolean("precedenceUriRef", false)

        if (pipeContext.config.getBoolean("useTempFile", false)) {
            throw NotImplementedError("Using temp file is currently not supported")
        } else {
            val datasets = page.page.listResourcesWithProperty(RDF.type, DCAT.Dataset)
            while (datasets.hasNext()) {
                val dataset = datasets.next()
                JenaUtils.findIdentifier(dataset, removePrefix, precedenceUriRef)?.let { id ->
                    val dataInfo = JsonObject()
                        .put("total", page.total)
                        .put("identifier", id)

                    emit(Dataset(dataset.extractAsModel() ?: ModelFactory.createDefaultModel(), dataInfo))

                } ?: pipeContext.log.warn("Could not extract an identifier from {}", dataset.uri)
            }
        }
    }

}
