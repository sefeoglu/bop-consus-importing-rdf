package io.piveau.importing.rdf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.piveau.pipe.connector.PipeContext;
import io.piveau.utils.Hash;
import io.piveau.utils.Hydra;
import io.piveau.utils.JenaUtils;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
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

    @Override
    public void start(Future<Void> startFuture) {
        vertx.eventBus().consumer(ADDRESS, this::handlePipe);
        client = WebClient.create(vertx);

        ConfigStoreOptions envStoreOptions = new ConfigStoreOptions()
                .setType("env")
                .setConfig(new JsonObject().put("keys", new JsonArray().add("PIVEAU_IMPORTING_SEND_LIST_DELAY")));
        ConfigRetriever retriever = ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(envStoreOptions));
        retriever.getConfig(ar -> {
            if (ar.succeeded()) {
                defaultDelay = ar.result().getInteger("PIVEAU_IMPORTING_SEND_LIST_DELAY", 8000);
                startFuture.complete();
            } else {
                startFuture.fail(ar.cause());
            }
        });
    }

    private void handlePipe(Message<PipeContext> message) {
        PipeContext pipeContext = message.body();

        JsonNode config = pipeContext.getConfig();
        pipeContext.log().info("Import started.");

        String address = config.path("address").asText();
        fetchPage(address, pipeContext, new ArrayList<>());
    }

    private void fetchPage(String address, PipeContext pipeContext, List<String> identifiers) {
        JsonNode config = pipeContext.getConfig();
        String outputFormat = config.path("outputFormat").asText("application/n-triples");

        boolean removePrefix = config.path("removePrefix").asBoolean(false);
        boolean precedenceUriRef = config.path("precedenceUriRef").asBoolean(false);

        client.getAbs(address).send(ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                String inputFormat = config.path("inputFormat").asText(response.getHeader("Content-Type"));
                Model page;
                try {
                    page = JenaUtils.read(response.bodyAsBuffer().getBytes(), inputFormat, address);
                } catch (Exception e) {
                    pipeContext.setFailure(e);
                    return;
                }

                log.debug(JenaUtils.write(page, Lang.TURTLE));

                ResIterator it = page.listResourcesWithProperty(RDF.type, DCAT.Dataset);

                boolean brokenHydra = config.path("brokenHydra").asBoolean(false);
                Hydra hydra = Hydra.findPaging(page, brokenHydra ? address : null);

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
                                    .put("total", hydra.total() != 0 ? hydra.total() : datasets.size())
                                    .put("counter", identifiers.size())
                                    .put("identifier", identifier)
                                    .put("catalogue", config.path("catalogue").asText())
                                    .put("hash", pseudo(model));
                            pipeContext.setResult(pretty, outputFormat, dataInfo).forward(client);
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
                    int delay = pipeContext.getConfig().path("sendListDelay").asInt(defaultDelay);
                    vertx.setTimer(delay, t -> {
                        ObjectNode info = new ObjectMapper().createObjectNode()
                                .put("content", "identifierList")
                                .put("catalogue", config.path("catalogue").asText());
                        pipeContext.setResult(new JsonArray(identifiers).encodePrettily(), "application/json", info).forward(client);
                    });
                }

                page.close();
            } else {
                pipeContext.setFailure(ar.cause());
            }
        });
    }

    private String pseudo(Model model) {
        long totalHash = 0;

        StmtIterator it = model.listStatements();
        while (it.hasNext()) {
            Statement stm = it.nextStatement();
            totalHash = (totalHash + tripleHash(stm.asTriple())) % Integer.MAX_VALUE;

            if (stm.getSubject().isAnon()) {
                StmtIterator objIt = model.listStatements(null, null, stm.getSubject());
                while (objIt.hasNext()) {
                    totalHash = (totalHash + tripleHash(objIt.nextStatement().asTriple())) % Integer.MAX_VALUE;
                }
            }

            if (stm.getObject().isAnon()) {
                StmtIterator subjIt = stm.getObject().asResource().listProperties();
                while (subjIt.hasNext()) {
                    totalHash = (totalHash + tripleHash(subjIt.nextStatement().asTriple())) % Integer.MAX_VALUE;
                }
            }
        };

        return Long.toHexString(totalHash);
    }

    private long tripleHash(Triple triple) {

        Node subject = triple.getSubject();
        Node predicate = triple.getPredicate();
        Node object = triple.getObject();

        String content = subject.isBlank() ? "Magic_S" : subject.toString();
        content += predicate.toString();
        content += object.isBlank() ? "Magic_O" : object.toString();

        return Long.parseLong(Hash.asHexString(content), 16);
    }

}
