package io.piveau.importing.job;

import com.fasterxml.jackson.databind.JsonNode;
import io.piveau.importing.utils.Hydra;
import io.piveau.importing.utils.JenaUtils;
import io.piveau.pipe.connector.PipeContext;
import io.reactivex.Flowable;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.ext.web.client.WebClient;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;

public class ImportingVerticle extends AbstractVerticle {

    private Logger log = LoggerFactory.getLogger(getClass());

    public static final String ADDRESS = "io.piveau.pipe.importing.queue";

    @Override
    public void start(Future<Void> startFuture) {
        vertx.eventBus().consumer(ADDRESS, this::handlePipe);
        startFuture.complete();
    }

    private void handlePipe(Message<PipeContext> message) {
        PipeContext pipeContext = message.body();

        pipeContext.log().info("Import started");

        JsonNode config = pipeContext.getConfig();
        if (config.path("address").isTextual()) {
            String url = config.get("address").textValue();
            fetchPage(url, pipeContext);
        } else {
            pipeContext.setFailure("No source address provided.");
        }

    }

    private void fetchPage(String url, PipeContext pipeContext) {
        WebClient client = WebClient.create(vertx);
        client.getAbs(url).send(ar -> {
            if (ar.succeeded()) {
                Model page = readPage(ar.result().bodyAsBuffer().getBytes());
                pipeContext.log().debug("page read");

                ResIterator it = page.listResourcesWithProperty(RDF.type, DCAT.Dataset);

                JsonNode config = pipeContext.getConfig();
                String outputFormat = config.path("outputFormat").textValue();
                Flowable.fromIterable(it.toList())
                        .map(JenaUtils::extractResource)
                        .map(JenaUtils::prettyPrint)
                        .subscribe(res -> pipeContext.setResult(res).forward(vertx));


                Hydra hydra = Hydra.findPaging(page);
                if (hydra != null) {
                    pipeContext.setTotal(hydra.total());
                    String next = hydra.next();
                    if (next != null) {
                        log.info(next);
                        fetchPage(next, pipeContext);
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

    private Model readPage(byte[] bytes) {
        InputStream stream = new ByteArrayInputStream(bytes);

        Dataset dataset = DatasetFactory.create();
        RDFDataMgr.read(dataset, stream, Lang.RDFXML);

        Model model = dataset.getDefaultModel();
        if (model.isEmpty()) {
            Iterator<String> names = dataset.listNames();
            if (names.hasNext()) {
                model = dataset.getNamedModel(names.next());
            }
        }
        dataset.close();

        return model;
    }

}
