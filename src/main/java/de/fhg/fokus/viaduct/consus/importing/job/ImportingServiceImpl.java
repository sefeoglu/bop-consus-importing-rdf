package de.fhg.fokus.viaduct.consus.importing.job;

import de.fhg.fokus.viaduct.consus.importing.utils.JenaUtils;
import io.reactivex.Flowable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Model;
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
import java.util.Iterator;

public class ImportingServiceImpl implements ImportingService {

    private Logger log = LoggerFactory.getLogger(getClass());

    private WebClient client;

    private EventBus eventBus;

    ImportingServiceImpl(WebClient client, EventBus eventBus, Handler<AsyncResult<ImportingService>> readyHandler) {
        this.client = client;
        this.eventBus = eventBus;
        readyHandler.handle(Future.succeededFuture(this));
    }

    @Override
    public ImportingService importRepository(JsonObject jobDescription, Handler<AsyncResult<Void>> resultHandler) {
        String url = jobDescription.getJsonObject("repository").getString("endpoint");
        String ebAddress = jobDescription.getJsonObject("pipe").getString("address");

        // flowable emitting pages, flatten to emitting datasets

        MessageProducer<JsonObject> followup = eventBus.publisher(ebAddress);
        fetchPage(url, followup);

        resultHandler.handle(Future.succeededFuture());
        return this;
    }

    private void fetchPage(String url, MessageProducer<JsonObject> followup) {
        client.getAbs(url).send(ar -> {
            if (ar.succeeded()) {
                Model page = readPage(ar.result().bodyAsBuffer().getBytes());
                log.debug("read page from {}", url);

                ResIterator it = page.listResourcesWithProperty(RDF.type, DCAT.Dataset);

                Flowable.fromIterable(it.toList())
                        .map(JenaUtils::extractResource)
                        .map(JenaUtils::prettyPrint)
                        .map(dataset -> new JsonObject().put("context", "id").put("data", dataset))
                        .subscribe(followup::send);

                String nextPage = nextPage(page);
                page.close();

                if (nextPage != null) {
                    fetchPage(nextPage, followup);
                } else {
                    log.debug("import finished, no more pages", ar.cause());
                }

            } else {
                log.debug("import finished with failure", ar.cause());
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

    private String nextPage(Model page) {
        ResIterator hydra = page.listResourcesWithProperty(RDF.type, page.createResource(JenaUtils.NS_HYDRA + "PagedCollection"));
        if (hydra.hasNext()) {
            Resource pagedCollection = hydra.nextResource();
            if (pagedCollection.hasProperty(page.createProperty(JenaUtils.NS_HYDRA + "nextPage"))) {
                return pagedCollection.getProperty(page.createProperty(JenaUtils.NS_HYDRA + "nextPage")).getString();
            }
//            totalItems = pagedCollection.getProperty(page.createProperty(JenaUtils.NS_HYDRA + "totalItems")).getInt();
        }
        return null;
    }
}
