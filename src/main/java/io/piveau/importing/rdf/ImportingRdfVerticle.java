package io.piveau.importing.rdf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.piveau.pipe.connector.PipeContext;
import io.piveau.rdf.PreProcessing;
import io.piveau.utils.HydraPaging;
import io.piveau.utils.JenaUtils;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.predicate.ResponsePredicate;
import io.vertx.ext.web.codec.BodyCodec;
import kotlin.Pair;
import org.apache.jena.atlas.web.ContentType;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ImportingRdfVerticle extends AbstractVerticle {

    private final Logger log = LoggerFactory.getLogger(getClass());

    public static final String ADDRESS = "io.piveau.pipe.importing.rdf.queue";

    private WebClient client;

    private int defaultDelay;
    private boolean preProcessing;

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.eventBus().consumer(ADDRESS, this::handlePipe);
        client = WebClient.create(vertx);

        ConfigStoreOptions envStoreOptions = new ConfigStoreOptions()
                .setType("env")
                .setConfig(new JsonObject().put("keys", new JsonArray().add("PIVEAU_IMPORTING_SEND_LIST_DELAY").add("PIVEAU_IMPORTING_PREPROCESSING")));
        ConfigRetriever retriever = ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(envStoreOptions));
        retriever.getConfig(ar -> {
            if (ar.succeeded()) {
                defaultDelay = ar.result().getInteger("PIVEAU_IMPORTING_SEND_LIST_DELAY", 8000);
                preProcessing = ar.result().getBoolean("PIVEAU_IMPORTING_PREPROCESSING", config().getBoolean("PIVEAU_IMPORTING_PREPROCESSING", false));
                startPromise.complete();
            } else {
                startPromise.fail(ar.cause());
            }
        });
        retriever.listen(change -> defaultDelay = change.getNewConfiguration().getInteger("PIVEAU_IMPORTING_SEND_LIST_DELAY", 8000));
    }

    private void handlePipe(Message<PipeContext> message) {
        PipeContext pipeContext = message.body();

        JsonObject config = pipeContext.getConfig();
        pipeContext.log().info("Import started.");

        String address = config.getString("address");
        fetchPage(address, pipeContext, new ArrayList<>());
    }

    private void fetchPage(String address, PipeContext pipeContext, List<String> identifiers) {
        JsonObject config = pipeContext.getConfig();
        String outputFormat = config.getString("outputFormat", "application/n-triples");

        boolean removePrefix = config.getBoolean("removePrefix", false);
        boolean precedenceUriRef = config.getBoolean("precedenceUriRef", false);
        boolean sendHash = config.getBoolean("sendHash", false);
        boolean applyPreProcessing = config.getBoolean("preProcessing", preProcessing);

        client.getAbs(address)
                .expect(ResponsePredicate.SC_SUCCESS).send(ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                String inputFormat = config.getString("inputFormat", ContentType.create(response.getHeader("Content-Type")).getContentType());

                byte[] content = response.bodyAsBuffer().getBytes();
                if (applyPreProcessing) {
                    Pair<byte[], String> processed = PreProcessing.preProcess(content, inputFormat, address);
                    content = processed.getFirst();
                    inputFormat = processed.getSecond();
                }

                Model page;
                try {
                    page = JenaUtils.read(content, inputFormat, address);
                } catch (Exception e) {
                    pipeContext.setFailure(e);
                    return;
                }

                log.debug(JenaUtils.write(page, Lang.TURTLE));

                ResIterator it = page.listResourcesWithProperty(RDF.type, DCAT.Dataset);

                boolean brokenHydra = config.getBoolean("brokenHydra", false);
                HydraPaging hydra = HydraPaging.findPaging(page, brokenHydra ? address : null);

                List<Resource> datasets = it.toList();
                datasets.forEach(resource -> {
                    try {
                        Model model = JenaUtils.extractResource(resource);
                        String identifier = JenaUtils.findIdentifier(resource, removePrefix, precedenceUriRef);
                        if (identifier == null) {
                            pipeContext.log().warn("Could not extract an identifier from {}", resource.toString());
                        } else {
                            identifiers.add(identifier);
                            String pretty = JenaUtils.write(model, outputFormat);
                            ObjectNode dataInfo = new ObjectMapper().createObjectNode()
                                    .put("total", hydra.getTotal() != 0 ? hydra.getTotal() : datasets.size())
                                    .put("counter", identifiers.size())
                                    .put("identifier", identifier)
                                    .put("catalogue", config.getString("catalogue"));
                            if (sendHash) {
                                dataInfo.put("hash", JenaUtils.canonicalHash(model));
                            }
                            pipeContext.setResult(pretty, outputFormat, dataInfo).forward();
                            pipeContext.log().info("Data imported: {}", dataInfo);
                            pipeContext.log().debug("Data content: {}", pretty);
                        }
                    } catch (Exception e) {
                        pipeContext.log().warn(resource.toString(), e);
                    }
                });

                String next = hydra.next();
                if (next != null) {
                    fetchPage(next, pipeContext, identifiers);
                } else {
                    pipeContext.log().info("Import metadata finished");
                    if (config.getBoolean("deletion", true)) {
                        int delay = pipeContext.getConfig().getInteger("sendListDelay", defaultDelay);
                        vertx.setTimer(delay, t -> {
                            ObjectNode info = new ObjectMapper().createObjectNode()
                                    .put("content", "identifierList")
                                    .put("catalogue", config.getString("catalogue"));
                            pipeContext.setResult(new JsonArray(identifiers).encodePrettily(), "application/json", info).forward();
                        });
                    }
                }

                page.close();
            } else {
                pipeContext.setFailure(ar.cause());
            }
        });
    }

}
