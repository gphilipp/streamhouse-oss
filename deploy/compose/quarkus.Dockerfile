# Image for a Quarkus service (control-plane, context-engine). The build context is the module's
# target/quarkus-app directory: run `make build` (mvn package) first.
FROM eclipse-temurin:21-jre
ARG PORT
WORKDIR /deployments
COPY lib/ lib/
COPY *.jar ./
COPY app/ app/
COPY quarkus/ quarkus/
USER 1001
EXPOSE ${PORT}
ENV JAVA_OPTS_APPEND="-XX:MaxRAMPercentage=75"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS_APPEND -jar quarkus-run.jar"]
