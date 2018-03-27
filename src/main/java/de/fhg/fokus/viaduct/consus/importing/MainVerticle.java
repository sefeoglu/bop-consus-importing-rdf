package de.fhg.fokus.viaduct.consus.importing;

import de.fhg.fokus.viaduct.consus.importing.job.ImportingService;
import de.fhg.fokus.viaduct.consus.importing.job.ImportingVerticle;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Launcher;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.api.contract.RouterFactoryOptions;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.core.http.HttpServer;
import io.vertx.reactivex.ext.dropwizard.MetricsService;
import io.vertx.reactivex.ext.healthchecks.HealthCheckHandler;
import io.vertx.reactivex.ext.healthchecks.HealthChecks;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import io.vertx.reactivex.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import io.vertx.reactivex.ext.web.handler.StaticHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainVerticle extends AbstractVerticle {

    private Logger log = LoggerFactory.getLogger(getClass());

    ImportingService importingService;

    @Override
    public void start(io.vertx.core.Future<Void> startFuture) throws Exception {
        MetricsService metrics = MetricsService.create(vertx);

        VertxOptions options = new VertxOptions();
        options.setEventBusOptions(new EventBusOptions().setClustered(true));

        Vertx.clusteredVertx(options, res -> {
            if (res.succeeded()) {
                Vertx vertx = res.result();
                EventBus eventBus = vertx.eventBus();
                System.out.println("We now have a clustered event bus: " + eventBus);
                eventBus.send("viaduct-consus.pipe.1", "Hi, how are you!");
            } else {
                System.out.println("Failed: " + res.cause());
            }
        });

        ConfigStoreOptions storeOptions = new ConfigStoreOptions()
                .setType("env")
                .setConfig(new JsonObject().put("keys", new JsonArray().add("SOME_VALUES")));

        ConfigRetriever retriever = ConfigRetriever.create(vertx.getDelegate(), new ConfigRetrieverOptions().addStore(storeOptions));
        Future<JsonObject> configFuture = ConfigRetriever.getConfigAsFuture(retriever);

        configFuture.compose(config -> vertx.deployVerticle(ImportingVerticle.class.getName(), new DeploymentOptions().setWorker(true).setConfig(config), result -> {
            if (result.succeeded()) {
                importingService = ImportingService.createProxy(vertx.getDelegate(), "consus.importing.queue");

                OpenAPI3RouterFactory.create(vertx, "webroot/openapi.yaml", ar -> {
                    if (ar.succeeded()) {
                        OpenAPI3RouterFactory routerFactory = ar.result();
                        RouterFactoryOptions roptions = new RouterFactoryOptions().setMountNotImplementedHandler(true).setMountValidationFailureHandler(true);
                        routerFactory.setOptions(roptions);
                        routerFactory.addHandlerByOperationId("importRepository", this::handleImport);

                        Router router = routerFactory.getRouter();

                        router.route("/*").handler(StaticHandler.create());

                        HealthChecks hc = HealthChecks.create(vertx);
                        hc.register("simple-health-check", future -> future.complete(Status.OK()));
                        router.get("/health").handler(HealthCheckHandler.createWithHealthChecks(hc));

                        HttpServer server = vertx.createHttpServer(new HttpServerOptions().setPort(8080));
                        server.requestHandler(router::accept).listen();

                        startFuture.complete();

                    } else {
                        // Something went wrong during router factory initialization
                        startFuture.fail(ar.cause());
                    }
                });
            } else {
                startFuture.fail(result.cause());
            }
        }), startFuture);
    }

    private void handleImport(RoutingContext context) {
        log.debug("handle import repository");

        JsonObject jobDescription = context.getBodyAsJson();
        importingService.importRepository(jobDescription, res -> {
            if (res.succeeded()) {
                context.response().setStatusCode(202).end();
            } else {
                context.response().setStatusCode(500).end();
            }
        });
    }

    public static void main(String[] args) {
        Launcher.executeCommand("run", MainVerticle.class.getName());
    }

}
