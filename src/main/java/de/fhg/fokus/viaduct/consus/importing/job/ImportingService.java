package de.fhg.fokus.viaduct.consus.importing.job;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;

@ProxyGen
public interface ImportingService {

    @Fluent
    ImportingService importRepository(JsonObject jobDescription, Handler<AsyncResult<Void>> resultHandler);

    static ImportingService create(WebClient client, EventBus eventBus, Handler<AsyncResult<ImportingService>> readyHandler) {
        return new ImportingServiceImpl(client, eventBus, readyHandler);
    }

    static ImportingService createProxy(Vertx vertx, String address) {
        return new ImportingServiceVertxEBProxy(vertx, address);
    }

}
