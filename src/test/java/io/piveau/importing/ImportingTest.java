package io.piveau.importing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.piveau.pipe.Pipe;
import io.piveau.pipe.PipeManager;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.shared.uuid.UUID_V4_Gen;
import org.apache.jena.sparql.vocabulary.FOAF;
import org.apache.jena.vocabulary.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@DisplayName("Testing the importer")
@ExtendWith(VertxExtension.class)
public class ImportingTest {

    @BeforeEach
    public void startImporter(Vertx vertx, VertxTestContext testContext) {
        vertx.deployVerticle(new MainVerticle(), testContext.completing());
    }

    @Test
    @DisplayName("pipe receiving")
    @Timeout(value = 5, timeUnit = TimeUnit.MINUTES)
    public void sendPipe(Vertx vertx, VertxTestContext testContext) {
        vertx.fileSystem().readFile("src/test/resources/test-pipe.json", result -> {
            if (result.succeeded()) {
                JsonObject pipe = new JsonObject(result.result());
                WebClient client = WebClient.create(vertx);
                client.post(8080, "localhost", "/pipe")
                        .putHeader("content-type", "application/json")
                        .sendJsonObject(pipe, testContext.succeeding(response -> testContext.verify(() -> {
                            if (response.statusCode() == 202) {
//                            testContext.completeNow();
                            } else {
                                testContext.failNow(new Throwable(response.statusMessage()));
                            }
                        })));
            } else {
                testContext.failNow(result.cause());
            }
        });
    }

    @Test
    @DisplayName("small converter script")
    void convert(Vertx vertx, VertxTestContext testContext) {

        Map<String, String> prefixes = Collections.unmodifiableMap(
                new HashMap<String, String>() {{
                    put("dcat", DCAT.NS);
                    put("skos", SKOS.uri);
                    put("foaf", FOAF.NS);
                    put("dct", DCTerms.NS);
                    put("dc", DC_11.NS);
                }});

        StringBuilder fileContent = new StringBuilder();

        Buffer lang = vertx.fileSystem().readFileBlocking("languages-skos.rdf");
        Model languages = ModelFactory.createDefaultModel();
        RDFDataMgr.read(languages, new ByteArrayInputStream(lang.getBytes()), Lang.RDFXML);

        Buffer buffer = vertx.fileSystem().readFileBlocking("repositories.json");
        JsonObject repos = new JsonObject(buffer);
        JsonArray all = repos.getJsonArray("result");

        all.iterator().forEachRemaining(obj -> {
            JsonObject repo = (JsonObject) obj;
            String type = repo.getString("type");
            String name = repo.getString("name").toLowerCase().replace(" ", "-");
            name = name.replaceAll("[^\\w\\s]", "-");

            if (type.equals("dcatap")) {
                String sourceUri = repo.getString("url");
                name = sourceUri.substring(sourceUri.lastIndexOf("/") + 1);
            }

            String description = repo.getString("description");
            String publisherName = repo.getString("publisher");
            String publisherEmail = repo.getString("publisherEmail", "");
            String homepage = repo.getString("homepage", "");
            String language = repo.getString("language", "");

            Model model = ModelFactory.createDefaultModel();
            Resource catalogue = model.createResource("https://europeandataportal.eu/id/catalogue/" + name, DCAT.Catalog);
            catalogue.addLiteral(DCTerms.title, model.createLiteral(repo.getString("name"), language));
            catalogue.addLiteral(DCTerms.description, model.createLiteral(description, language));

            ResIterator it = languages.listSubjectsWithProperty(languages.createProperty("http://publications.europa.eu/ontology/authority/legacy-code"), language);
            while (it.hasNext()) {
                Resource mappedCode = it.next();
                if (mappedCode.hasLiteral(DC_11.source, "iso-639-1")) {
                    ResIterator concepts = languages.listSubjectsWithProperty(languages.createProperty("http://publications.europa.eu/ontology/authority/op-mapped-code"), mappedCode);
                    if (concepts.hasNext()) {
                        catalogue.addProperty(DCTerms.language, concepts.next());
                        break;
                    }
                }
            }

            repo.getJsonArray("spatial", new JsonArray()).iterator().forEachRemaining(s -> {
                Resource spatial = model.createResource(s.toString());
                catalogue.addProperty(DCTerms.spatial, spatial);
            });

            Resource publisher = model.createResource(FOAF.Agent);
            publisher.addProperty(DCTerms.type, model.createResource("http://purl.org/adms/publishertype/NationalAuthority"));
            publisher.addProperty(FOAF.name, model.createLiteral(publisherName, language));
            if (!publisherEmail.isEmpty()) {
                publisher.addProperty(FOAF.mbox, model.createResource("mailto:" + publisherEmail));
            }
            if (!homepage.isEmpty()) {
                publisher.addProperty(FOAF.homepage, model.createResource(homepage, FOAF.Document));
            }

            catalogue.addProperty(DCTerms.publisher, publisher);

            switch (type) {
                case "dcatap":
                    generateConterraPipe(vertx, repo);
                    break;
                case "ckan":
                    generateCkanPipe(vertx, repo);
                    break;
                default:
            }

            fileContent.append("PUT http://odp-ckan01.ppe-aws.europeandataportal.eu:8082/catalogues/");
            fileContent.append(name);
            fileContent.append("\nContent-Type: application/rdf+xml\n");
            fileContent.append("Authorization: db916db8-8aeb-4915-a3b9-f6b264b5d983\n\n");

            model.setNsPrefixes(prefixes);

            ByteArrayOutputStream output = new ByteArrayOutputStream();
            RDFDataMgr.write(output, model, Lang.RDFXML);
            fileContent.append(output.toString());
            fileContent.append("\n\n###\n\n");
        });

        vertx.fileSystem().writeFileBlocking("src/test/resources/catalogues.http", Buffer.buffer(fileContent.toString()));

        testContext.completeNow();
    }

    private void generateConterraPipe(Vertx vertx, JsonObject repo) {
        String sourceUri = repo.getString("url");
        String shortName = sourceUri.substring(sourceUri.lastIndexOf("/") + 1);

        try {
            PipeManager pipeManager = PipeManager.create("pipe-template.json");
            Pipe pipe = pipeManager.getPipe();
            pipe.getHeader().setId(new UUID_V4_Gen().generateV4().asString());
            pipe.getHeader().setTitle("Harvester - " + repo.getString("name"));

            ObjectNode config = new ObjectMapper().createObjectNode();
            config.put("address", sourceUri + "?metadataPrefix=dcat_ap");
            config.put("outputFormat", "application/n-triples");
            pipe.getBody().getSegments().get(0).getBody().setConfig(config);

            ObjectNode config2 = (ObjectNode) pipe.getBody().getSegments().get(1).getBody().getConfig();
            config2.put("catalogue", shortName);

            String filename = "src/test/resources/pipes/pipe-" + shortName + ".json";
            vertx.fileSystem().writeFileBlocking(filename, Buffer.buffer(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(pipe)));
        } catch (IOException e) {

        }

    }

    private void generateCkanPipe(Vertx vertx, JsonObject repo) {
        String sourceUri = repo.getString("url");
        String name = repo.getString("name").toLowerCase().replace(" ", "-");
        name = name.replaceAll("[^\\w\\s]", "-");

        try {
            PipeManager pipeManager = PipeManager.create("pipe-ckan-template.json");
            Pipe pipe = pipeManager.getPipe();
            pipe.getHeader().setId(new UUID_V4_Gen().generateV4().asString());
            pipe.getHeader().setTitle("Harvester - " + repo.getString("name"));

            ObjectNode config = new ObjectMapper().createObjectNode();
            config.put("address", sourceUri);
            pipe.getBody().getSegments().get(0).getBody().setConfig(config);

            ObjectNode transConfig = (ObjectNode) pipe.getBody().getSegments().get(1).getBody().getConfig();
            ObjectNode repository = (ObjectNode) transConfig.path("repository");
            repository.put("script", "js/" + name + "-to-dcat-ap.js");
            ObjectNode params = (ObjectNode) transConfig.path("params");
            params.put("defaultLanguage", repo.getString("language"));

            ObjectNode config2 = (ObjectNode) pipe.getBody().getSegments().get(2).getBody().getConfig();
            config2.put("catalogue", name);

            String filename = "src/test/resources/pipes/pipe-" + name + ".json";
            vertx.fileSystem().writeFileBlocking(filename, Buffer.buffer(new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(pipe)));
        } catch (IOException e) {

        }

    }

    @Test
    @DisplayName("generate trigger from old harvester")
    void generateTriggers(Vertx vertx, VertxTestContext testContext) {
        WebClient client = WebClient.create(vertx);
        JsonObject triggers = new JsonObject();
        HttpRequest<Buffer> request = client.getAbs("https://www.europeandataportal.eu/MetadataTransformerService/rest/harvester");
        request.send(ar -> {
            if (ar.succeeded()) {
                HttpResponse<Buffer> response = ar.result();
                JsonObject result = response.bodyAsJsonObject();
                if (result.getBoolean("success")) {
                    JsonArray harvesters = result.getJsonArray("result");
                    harvesters.forEach(obj -> {
                        JsonObject harvester = (JsonObject) obj;
                        String frequency = harvester.getString("frequency", "manually");
                        if (!frequency.equals("manually")) {
                            Date scheduled = new Date(harvester.getLong("scheduled"));
                            JsonArray tmp = new JsonArray();
                            JsonObject metadata = new JsonObject().put("id", "metadata")
                                    .put("status", "enabled")
                                    .put("interval", new JsonObject().put("unit", frequency.toUpperCase()).put("value", 1))
                                    .put("next", scheduled.toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
                            JsonObject identifiers = new JsonObject().put("id", "identifiers")
                                    .put("status", "enabled")
                                    .put("interval", new JsonObject().put("unit", frequency.toUpperCase()).put("value", 1))
                                    .put("next", scheduled.toInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME))
                                    .put("config", new JsonObject().put("mode", "identifiers"));
                            tmp.add(metadata);
                            tmp.add(identifiers);
                            triggers.put(harvester.getString("name").replace(" to EDP", ""), tmp);
                        }
                    });
                    vertx.fileSystem().writeFileBlocking("src/test/resources/triggers.json", Buffer.buffer(triggers.encodePrettily()));
                }
                testContext.completeNow();
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

}
