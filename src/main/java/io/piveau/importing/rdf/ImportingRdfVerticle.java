package io.piveau.importing.rdf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.piveau.pipe.connector.PipeContext;
import io.piveau.utils.Hash;
import io.piveau.utils.Hydra;
import io.piveau.utils.JenaUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ImportingRdfVerticle extends AbstractVerticle {

    private Logger log = LoggerFactory.getLogger(getClass());

    public static final String ADDRESS = "io.piveau.pipe.importing.rdf.queue";

    private WebClient client;

    @Override
    public void start(Future<Void> startFuture) {
        vertx.eventBus().consumer(ADDRESS, this::handlePipe);
        client = WebClient.create(vertx);

        startFuture.complete();
    }

    private void handlePipe(Message<PipeContext> message) {
        PipeContext pipeContext = message.body();

        pipeContext.log().info("Import started");

        JsonNode config = pipeContext.getConfig();
        if (config.path("address").isTextual()) {
            String url = config.get("address").textValue();
            fetchPage(url, pipeContext, new AtomicInteger());
        } else {
            pipeContext.setFailure("No source address provided.");
        }
    }

    private void fetchPage(String url, PipeContext pipeContext, AtomicInteger counter) {
        JsonNode config = pipeContext.getConfig();
        String outputFormat = config.path("outputFormat").asText("application/n-triples");

        client.getAbs(url).send(ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                Model page = readPage(response.bodyAsBuffer().getBytes(), response.getHeader("Content-Type"));
                pipeContext.log().debug("Page read");

                Hydra hydra = Hydra.findPaging(page);

                ResIterator it = page.listResourcesWithProperty(RDF.type, DCAT.Dataset);

                List<Resource> datasets = it.toList();
                datasets.forEach(resource -> {
                    try {
                        Model model = JenaUtils.extractResource(resource);
                        String identifier = JenaUtils.findIdentifier(resource);
                        String pretty = JenaUtils.prettyPrint(model, outputFormat);
                        ObjectNode dataInfo = new ObjectMapper().createObjectNode()
                                .put("total", hydra != null ? hydra.total() : datasets.size())
                                .put("counter", counter.incrementAndGet())
                                .put("identifier", identifier)
                                .put("hash", Hash.asHexString(pretty));
                        pipeContext.setResult(pretty, outputFormat, dataInfo).forward(client);
                        pipeContext.log().info("Data imported: " + dataInfo.toString());
                    } catch (Exception e) {
                        pipeContext.log().warn("Could not import data for " + resource.toString() + " (" + counter.incrementAndGet() + "): " + e.getMessage());
                    }
                });

                if (hydra != null) {
                    String next = hydra.next();
                    if (next != null) {
                        log.info(next);
                        fetchPage(next, pipeContext, counter);
                    } else {
                        pipeContext.log().info("Import finished");
                    }
                } else {
                    pipeContext.log().debug("No paging info found.");
                }

                page.close();
            } else {
                pipeContext.setFailure(ar.cause().getMessage());
            }
        });
    }

    private Model readPage(byte[] bytes, String contentType) {
        InputStream stream = new ByteArrayInputStream(bytes);

        Model model = ModelFactory.createDefaultModel();
        RDFDataMgr.read(model, stream, null, JenaUtils.mimeTypeToLang(contentType));

        return model;
    }

}
