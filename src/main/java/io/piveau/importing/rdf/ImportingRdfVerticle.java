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
import io.vertx.core.json.JsonArray;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.RDF;

import java.net.URL;
import java.util.*;

public class ImportingRdfVerticle extends AbstractVerticle {

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

        JsonNode config = pipeContext.getConfig();
        String mode = config.path("mode").asText("metadata");
        pipeContext.log().info("Import started. Mode '{}'", mode);

        String address = config.path("address").asText();
        if (mode.equals("identifiers")) {
            fetchIdentifiers(address, pipeContext, new HashSet<>());
        } else {
            fetchPage(address, pipeContext, new ArrayList<>());
        }
    }

    private void fetchPage(String address, PipeContext pipeContext, List<String> identifiers) {
        JsonNode config = pipeContext.getConfig();
        String outputFormat = config.path("outputFormat").asText("text/turtle");

        client.getAbs(address).send(ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                String inputFormat = config.path("inputFormat").asText(response.getHeader("Content-Type"));
                Model page;
                try {
                    page = JenaUtils.read(response.bodyAsBuffer().getBytes(), inputFormat);
                } catch (Exception e) {
                    pipeContext.setFailure(e);
                    return;
                }

                ResIterator it = page.listResourcesWithProperty(RDF.type, DCAT.Dataset);

                boolean brokenHydra = config.path("brokenHydra").asBoolean(false);
                Hydra hydra = Hydra.findPaging(page, brokenHydra ? address : null);

                List<Resource> datasets = it.toList();
                datasets.forEach(resource -> {
                    try {
                        Model model = JenaUtils.extractResource(resource);
                        String identifier = JenaUtils.findIdentifier(resource);
                        identifiers.add(identifier);
                        String pretty = JenaUtils.write(model, outputFormat);
                        ObjectNode dataInfo = new ObjectMapper().createObjectNode()
                                .put("total", hydra.total() != 0 ? hydra.total() : datasets.size())
                                .put("counter", identifiers.size())
                                .put("identifier", identifier)
                                .put("hash", Hash.asHexString(pretty));
                        pipeContext.setResult(pretty, outputFormat, dataInfo).forward(client);
                        pipeContext.log().info("Data imported: {}", dataInfo);
                    } catch (Exception e) {
                        pipeContext.log().warn(resource.toString(), e);
                    }
                });

                String next = hydra.next();
                if (next != null) {
                    fetchPage(next, pipeContext, identifiers);
                } else {
                    pipeContext.log().info("Import metadata finished");
                    pipeContext.setResult(new JsonArray(identifiers).encodePrettily(), "application/json", new ObjectMapper().createObjectNode().put("content", "identifierList")).forward(client);
                }

                page.close();
            } else {
                pipeContext.setFailure(ar.cause());
            }
        });
    }

    private void fetchIdentifiers(String address, PipeContext pipeContext, Set<String> identifiers) {
        client.getAbs(address).send(ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                Model page = JenaUtils.read(response.bodyAsBuffer().getBytes(), response.getHeader("Content-Type"));

                List<Resource> datasets = page.listResourcesWithProperty(RDF.type, DCAT.Dataset).toList();
                datasets.forEach(resource -> {
                    String identifier = JenaUtils.findIdentifier(resource);
                    identifiers.add(identifier);
                });

                boolean brokenHydra = pipeContext.getConfig().path("brokenHydra").asBoolean(false);
                String next = Hydra.findPaging(page, brokenHydra ? address : null).next();
                if (next != null) {
                    fetchIdentifiers(next, pipeContext, identifiers);
                } else {
                    pipeContext.setResult(new JsonArray(new ArrayList<>(identifiers)).encodePrettily(), "application/json").forward(client);
                    pipeContext.log().info("Import identifiers finished");
                }

                page.close();
            } else {
                pipeContext.setFailure(ar.cause());
            }
        });
    }

}
