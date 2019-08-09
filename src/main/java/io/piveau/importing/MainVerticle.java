package io.piveau.importing;

import io.piveau.importing.rdf.ImportingRdfVerticle;
import io.piveau.pipe.connector.PipeConnector;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Launcher;
import io.vertx.core.Promise;

import java.util.Arrays;

public class MainVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.deployVerticle(ImportingRdfVerticle.class, new DeploymentOptions().setWorker(true), result -> {
            if (result.succeeded()) {
                PipeConnector.create(vertx, cr -> {
                    if (cr.succeeded()) {
                        cr.result().consumerAddress(ImportingRdfVerticle.ADDRESS);
                        startPromise.complete();
                    } else {
                        startPromise.fail(cr.cause());
                    }
                });
            } else {
                startPromise.fail(result.cause());
            }
        });
    }

    public static void main(String[] args) {
        String[] params = Arrays.copyOf(args, args.length + 1);
        params[params.length - 1] = MainVerticle.class.getName();
        Launcher.executeCommand("run", params);
    }

}
