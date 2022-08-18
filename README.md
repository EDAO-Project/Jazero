# Calypso
Welcome to Calypso: A semantic data lake microservice architecture for semantically augmented table search.

## Setup
From the root directory of this repository, place a knowledge graph turtle file in knowledge-graph/neo4j/ and run the following commands to install Neo4J in Docker and insert the knowledge graph into a dockerized Neo4J instance

```bash
docker build -f kg.dockerfile -t neo4j .
docker run --rm -v ${PWD}/knowledge-graph/neo4j:/srv neo4j bash -c "./install.sh /srv; ./import.sh <KG_FILE> neo4j-server; ./stop.sh neo4j-server"
```

Substitute `<KG>` with the knowledge graph file name. Now, the knowledge graph will be loaded into `knowledge-graph/neo4j/neo4j-server`.

## Starting Calypso
Setting up and running Calypso is very simple.
All you need is to have Docker and Docker-compose installed.

Start Calypso with the following simple command

```bash
docker-compose up
```

Now, Calypso is accessible on localhost or on the machine's IP address.

Alternatively, but not recommended, you can build each service manually and run the built .jar file.
This requires having Java 17 install and Maven.

First, enter the `communication` module to install it as a dependency with `mvn clean install`.
Enter each of the folders `data-lake`, `entity-linker`, and `knowledge graph` and build the executables with `./mvnw clean package`.
You can run the following script to do all of this in one go:

```bash
mkdir target && \
cd communication && \
mvn clean install && \
cd ../data-lake && \
./mvnw clean package && \
mv target/*.jar ../target && \
cd ../entity-linker
./mvnw clean package && \
mv target/*.jar ../target && \
cd ../knowledge-graph && \
./mvnw clean package && \
mv target/*.jar ../target && \
cd ..
```

Now, all executable .jar files are in the new folder `target`.
These can be executed with `java -jar ...`.

## Working with Calypso
[Here will be some instructions on using the CDLC driver and maybe also the API if we build that]

## Setting Up Calypso in an IDE
Most of the components in Calypso are dependent on the `communication` module.
Therefore, change directory to this module and run the following to install it as a dependency

```bash
cd communication
mvn clean install
```
