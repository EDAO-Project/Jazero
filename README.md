# Jazero
Welcome to Jazero: A semantic data lake microservice architecture for semantically augmented table search.

## Setup
Make sure to have docker-compose version 2+ installed.

First, create a folder `kg` in the parent directory and move your knowledge graph (KG) files here.
**Jazero only supports .ttl (Turtle) files**.
Then, start an instance by running the following command:

```bash
./start
```

The first time you start an instance, the entity linker will construct its indexes which will take around 1 hour for a 10GB KG dataset.
Therefore, do not start loading data until the entity linker finishes constructing its own indexes.

<b>Important:</b> The first time the instance is started, an admin must be set.
This can be done <i>once</i> with the following command:

```bash
curl -H "Content-Type: application/json" -d '{"username": "admin", "password": "1234"}' http://localhost:8081/set-admin
```

Instead of `admin` and `1234`, you can choose your own credentials for the admin.

The following command will install the necessary plugins.
This can run in parallel with the index construction in the entity linker.

```bash
docker exec jazero_neo4j /scripts/install.sh
```

Once the download is complete, restart the Neo4J container and start populating the KG.

```bash
docker restart jazero_neo4j
docker exec jazero_neo4j /scripts/import.sh . /kg
```

Feel free to delete the contents of the `kg` folder once the construction of entity linker indexes and population of the KG have finished.

## Starting Jazero
Start Jazero with the following simple command:

```bash
./start.sh
```

Alternatively, but not recommended, you can build each service manually and run the built .jar file.
This requires having Java 17 and Maven installed.

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
These can be executed with `java -jar <JAR FILE>`.

## Working with Jazero
Here, we describe working with Jazero: how to load Jazero with tables, load indexes, load embeddings, and search Jazero.

### Loading Jazero
Tables in Jazero are loaded and stored either natively on disk or in HDFS (HDFS is not yet supported).
Loading of embeddings must be performed first, as the embeddings are used to construct indexes during loading of tables.

##### Loading Embeddings

One representation model in Jazero is by embeddings.
Consider using <a href="https://github.com/EDAO-Project/DBpediaEmbedding">this</a> repository to generate RDF2Vec embeddings.

Every entity embedding must be contained in one line in the embeddings file, and each value must be separated by the same delimiter.
This includes the entity itself. Below is an example of what an embeddings file of three entities would look like where the space character is the delimiter:

```
https://dbpedia.org/page/Barack_Obama -3.21 13.2122 53.32 -2.23 0.4353 8.231
https://dbpedia.org/page/Lionel_Messi 2.432 9.3213 -32.231 21.432 -21.022 53.1133
https://dbpedia.org/page/Eiffel_Tower -34.422 -7.231 5.312 -1.435 0.543 12.440
```

When loading the embeddings into Jazero, they will be stored in a Postgres instance.
You can access the embeddings directly in the Postgres instance by running the following command.

```bash
docker exec -it jazero_pg psql -U jazero embeddings
```

##### Loading Tables and Indexes

The tables must be in JSON format with the fields __id_, _numDataRows_, _numCols_, numNumericCols, and _rows_, where _rows_ is an array of table rows.
Each cell must contain a single field `text` which hold the content of the table cell.
The field _headers_ includes the header information of the table.

An example of a table is given below:

```json
{
  "_id": "1514-27", 
  "numCols": 2, 
  "numDataRows": 2,
  "numNumericCols": 0,
  "headers": [
    {
      "text": "Olympic event",
      "isNumeric": false
    },
    {
      "text": "Athlete",
      "isNumeric": false
    }
  ],
  "rows": [
    [
      {
        "text": "1900 Paris"
      }, 
      {
        "text": "Walter Tewksbury"
      }
    ],
    [
      {
        "text": "1904 St. Louis"
      }, 
      {
        "text": "Harry Hillman"
      }
    ]
  ]
}
```

Loading a table corpus of 100K tables will take around a full day, depending on the machine.

### Searching Jazero
Jazero utilizes the _query-by-example_ paradigm.
You construct a table query of knowledge graph entities, and Jazero will return a top-_K_ ranked list of semantically relevant tables.

The specific format of the query and its construction depends on the connector.
The following section will describe searching, as well as other operations, in Jazero using the different connectors.

### Connector

The repository for the connectors to communicate with Jazero can be found <a href="https://github.com/EDAO-Project/Jazero/tree/main/JDLC">here</a>.
There is both a C, Java connector, and Python connector.

<u>Remember to run the methods to insert tables and embeddings on the machine running Jazero. Only searching can be performed remotely.</u>

### Jazero Web
This repository has a Django web interface to interact with an instance of Jazero.
Build the Docker image from the project root.

```bash
docker build -t jazero_web -f JDLC/python/api/Dockerfile JDLC/python
```

Then, run a container of Jazero web.

```bash
docker run --rm --network="host" -d --name jazero -e JAZERO_HOST=<HOST> jazero_web
```

You can now access the Jazero web interface <a href="http://127.0.0.1:8084/jdlc/">here</a>.
Just substitute `<HOST>` with the host name of the running Jazero instance.
For demonstration purposes, we already have an instance of Jazero running, and it can be accessed using its web interface <a href="">here</a>.

You can stop the Jazero web interface with the following command.

```bash
docker stop jazero
```

## Setting Up Jazero in an IDE
Most of the components in Jazero are dependent on the `communication` module.
Therefore, change directory to this module and run the following to install it as a dependency

```bash
cd communication
mvn clean install
```

Now, do the same for the `storage` module, as the `data-lake` module depends on this.
