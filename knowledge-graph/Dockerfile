FROM ubuntu

RUN apt update
RUN apt install wget openjdk-17-jre openjdk-17-jdk maven -y

WORKDIR /home

RUN mkdir communication
RUN mkdir -p knowledge-graph/neo4j
RUN mkdir data-lake
COPY communication communication
COPY data-lake data-lake
COPY knowledge-graph knowledge-graph

WORKDIR communication
RUN mvn clean install -DskipTests

ARG MVNW=mvnw_unix
WORKDIR ../data-lake
RUN sed -e 's/\r$//' mvnw > mvnw_unix
RUN chmod +x mvnw_unix
RUN ./mvnw_unix clean install

WORKDIR ../knowledge-graph
RUN sed -e 's/\r$//' mvnw > mvnw_unix
RUN chmod +x mvnw_unix

ENTRYPOINT ["./mvnw_unix", "spring-boot:run"]
