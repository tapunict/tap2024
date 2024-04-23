# Creating first container to compile Java code and create jar file
FROM maven:3-openjdk-8 AS build
WORKDIR /app

# Copying pom.xml file to container
COPY kafka-streams.examples/pom.xml .

# Downloading dependencies
RUN mvn -f ./pom.xml clean package

COPY kafka-streams.examples/src src

RUN mvn -f ./pom.xml package

FROM tap:kafka
ENV BUILD "kafka-streams.examples-0.1.jar"
WORKDIR /opt/kafka/
COPY --from=build /app/target/${BUILD}  /opt/kafka/libs
ENTRYPOINT [ "kafka-run-class.sh"]  