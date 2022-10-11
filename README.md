# Calypso
Welcome to Calypso: A semantic data lake microservice architecture for semantically augmented table search.

## Setup
### Loading Knowledge Graph
From the root directory of this repository, create a folder `input` in in `knowledge-graph/neo4j/`

```bash
mkdir knowledge-graph/neo4j/input
```

Place all your knowledge graph turtle files in this new folder `input`.
Run the following commands to install Neo4J in Docker and insert the knowledge graph into a dockerized Neo4J instance

```bash
docker build -f kg.dockerfile -t neo4j .
docker run --rm -v ${PWD}/knowledge-graph/neo4j:/srv neo4j bash -c "./install.sh /srv; ./import.sh neo4j-server; ./stop.sh neo4j-server"
```

Substitute `<KG>` with the knowledge graph file name or folder of graph files. This path is from the `knowledge-graph/neo4j/` as root. Now, the knowledge graph will be loaded into `knowledge-graph/neo4j/neo4j-server`.
If you substituted `<KG>` with a folder, pass directory to the folder where `knowledge-graph/neo4j/` directory is the root directory.

### Loading Calypso
Tables in Calypso are loaded and stored either natively on disk or in HDFS.
```bash
here will be instructions on how to load Calypso and choosing storage layer
```

Calypso can additionally use RDF embeddings for similarity search.
Generate an embeddings file of the knowledge graph using <a href="https://github.com/EDAO-Project/DBpediaEmbedding">this</a> repository and run the following command to load the embeddings into Calypso.

```bash

```

## Starting Calypso
Setting up and running Calypso is very simple.
All you need is to have Docker and Docker-compose installed. Make sure to have Docker-compose version 2+ installed.

Start Calypso with the following simple command

```bash
docker-compose up
```

Now, Calypso is accessible on localhost or on the machine's IP address.

Alternatively, but not recommended, you can build each service manually and run the built .jar file.
This requires having Java 17 install and Maven.

First, enter the `communication` module to install it as a dependency with `mvn clean install`. Now, do the same with the `storage` module.
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
[Describe how to populate with tables and how to insert embeddings]

## Setting Up Calypso in an IDE
Most of the components in Calypso are dependent on the `communication` module.
Therefore, change directory to this module and run the following to install it as a dependency

```bash
cd communication
mvn clean install
```
