package io.piveau.importing.rdf

import io.piveau.pipe.PipeContext
import io.piveau.rdf.*
import io.piveau.utils.JenaUtils
import io.piveau.utils.gunzip
import io.vertx.circuitbreaker.CircuitBreaker
import io.vertx.circuitbreaker.CircuitBreakerOptions
import io.vertx.core.Vertx
import io.vertx.core.file.OpenOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.ext.web.codec.BodyCodec
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.Lang
import org.apache.jena.vocabulary.DCAT
import org.apache.jena.vocabulary.RDF
import org.slf4j.LoggerFactory
import java.io.File
import java.util.concurrent.TimeUnit

data class Page(val page: Model, val total: Int)
data class Dataset(val dataset: Model, val dataInfo: JsonObject)

class DownloadSource(private val vertx: Vertx, config: JsonObject) {

    private val log = LoggerFactory.getLogger(javaClass)

    private val preProcessing = config.getBoolean("PIVEAU_IMPORTING_PREPROCESSING", false)

    private val circuitBreaker = CircuitBreaker
        .create("importing", vertx, CircuitBreakerOptions().setMaxRetries(2).setTimeout(180000))
        .retryPolicy { _, c -> c * 2000L }

    private val executor = vertx.createSharedWorkerExecutor("modelReader", 20, 10, TimeUnit.MINUTES)

    fun pagesFlow(address: String, pipeContext: PipeContext): Flow<Page> = flow {


        val applyPreProcessing = pipeContext.config.getBoolean("preProcessing", preProcessing)
        val brokenHydra = pipeContext.config.getBoolean("brokenHydra", false)
 
        val finalFileName = address
        val contentType = "application/rdf+xml"
        
        if (contentType.isRDF) {
                    
            val (fileName, content, finalContentType) = if (applyPreProcessing) {
            val output = vertx.fileSystem().createTempFile("tmp", "piveau", ".tmp", null as String?).await()

                val (_, finalContentType) = preProcess(
                    File(finalFileName).inputStream(),
                    File(output).outputStream(),
                    contentType,
                    address
                )
                Triple(output, File(output).inputStream(), finalContentType)
            } else {
                Triple("", File(finalFileName).inputStream(), contentType)
            }

            val page = try {
                executor.executeBlocking {
                        try {
                            val model = JenaUtils.read(content, finalContentType, address)
                            it.complete(model)
                        } catch (e: Exception) {
                            it.fail(e)
                        }
                    }.await()
            } catch (e: Exception) {
                throw Throwable("$address: ${e.message}")
            }

            if (fileName.isNotBlank()) {
                vertx.fileSystem().delete(fileName)
            }

            val hydraPaging = HydraPaging.findPaging(page, if (brokenHydra) address else null)


            emit(Page(page, hydraPaging.total))


        } else {
            throw Throwable(" Content-Type $contentType is not an RDF content type.")
        }

    }

    fun datasetsFlow(page: Page, pipeContext: PipeContext): Flow<Dataset> = flow {
        val removePrefix = pipeContext.config.getBoolean("removePrefix", false)
        val precedenceUriRef = pipeContext.config.getBoolean("precedenceUriRef", false)

        if (pipeContext.config.getBoolean("useTempFile", false)) {
            throw NotImplementedError("Using temp file is currently not supported")
        } else {
            // cleanup some idiosyncrasies
            page.page.removeAll(null, RDF.type, DCAT.dataset)

            val datasets = page.page.listResourcesWithProperty(RDF.type, DCAT.Dataset).toSet()
            val total = if (page.total > 0) page.total else datasets.size
            datasets.forEach { dataset ->
                dataset.identify(removePrefix, precedenceUriRef)?.let { id ->
                    if (id.isNotBlank()) {
                        val dataInfo = JsonObject()
                            .put("total", total)
                            .put("identifier", id)

                        val datasetModel = dataset.extractAsModel() ?: ModelFactory.createDefaultModel()

                        // More idiosyncrasies
                        datasetModel.listStatements(null, RDF.type, DCAT.Catalog).toList().forEach { stmt ->
                            stmt.subject.extractAsModel()?.let { model ->
                                datasetModel.remove(model)
                            }
                        }
                        if (!datasetModel.isEmpty) {
                            emit(Dataset(datasetModel, dataInfo))
                        }
                    } else {
                        pipeContext.log.warn("Could not extract an identifier from dataset")
                        if (pipeContext.log.isDebugEnabled) {
                            pipeContext.log.debug(
                                dataset.extractAsModel()?.presentAs(Lang.TURTLE) ?: "no model"
                            )
                        }
                    }
                } ?: pipeContext.log.warn("Could not extract an identifier from dataset")
            }
        }
    }

}
