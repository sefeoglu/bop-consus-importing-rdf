FROM openjdk:jre-alpine

ENV VERTICLE_FILE piveau-importing-rdf-0.0.1-alpha-fat.jar

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles

EXPOSE 8080

RUN addgroup -S vertx && adduser -S -g vertx vertx

# Copy your fat jar to the container
COPY target/$VERTICLE_FILE $VERTICLE_HOME/

RUN chown -R vertx $VERTICLE_HOME
RUN chmod -R g+w $VERTICLE_HOME

USER vertx

# Launch the verticle
WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["exec java -Xmx2048m -jar $VERTICLE_FILE -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory -Dvertx.metrics.options.enabled=true"]
