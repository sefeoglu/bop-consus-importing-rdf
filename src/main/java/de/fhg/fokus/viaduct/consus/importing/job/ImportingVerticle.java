package de.fhg.fokus.viaduct.consus.importing.job;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.ext.web.client.WebClient;
import io.vertx.serviceproxy.ServiceBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportingVerticle extends AbstractVerticle {

    private Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        WebClient client = WebClient.create(vertx);
        ImportingService.create(client, vertx.eventBus(), ready -> {
            if (ready.succeeded()) {
                new ServiceBinder(vertx).setAddress("consus.importing.queue").register(ImportingService.class, ready.result());
                startFuture.complete();
            } else {
                startFuture.fail(ready.cause());
            }
        });
    }

}
