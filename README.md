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

The following command will install the necessary plugins.
This can run in parallel with the index construction in the entity linker.

```bash
docker exec jazero_neo4j /scripts/install.sh
```

Once you see a tabular output, restart the Neo4J container and start populating the KG.

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

Now, all executable .jar files are in the new folder respective `target`.
These can be executed with `java -jar <JAR FILE>`.

## Working with Jazero
Here we describe working with Jazero, how to load Jazero with tables, load indexes, load embeddings, and search Jazero.

### Loading Jazero
Tables in Jazero are loaded and stored either natively on disk or in HDFS (HDFS is not yet supported).
Loading of embeddings must be performed first as the embeddings are used to construct indexes during loading of tables.

##### Loading Embeddings

Jazero also uses embeddings to represent entities.
Consider using <a href="https://github.com/EDAO-Project/DBpediaEmbedding">this</a> repository to generate RDF embeddings.

Every entity embedding must be contained in one line in the embeddings file, and each value must be separated by the same delimiter.
This includes the entity itself. Below is an example of what an embeddings file of three entities could look like where the space character is the delimiter:

```
https://dbpedia.org/page/Barack_Obama -3.21 13.2122 53.32 -2.23 0.4353 8.231
https://dbpedia.org/page/Lionel_Messi 2.432 9.3213 -32.231 21.432 -21.022 53.1133
https://dbpedia.org/page/Eiffel_Tower -34.422 -7.231 5.312 -1.435 0.543 12.440
```

##### Loading Tables and Indexes

The tables must be in JSON format with the fields __id_, pgTitle, _numDataRows_, _numCols_, numNumericCols, and _rows_, where _rows_ is an array of table rows.
Each cell must contain a `text` property and a `links` property. The `links` property is an array of the URI of the entity in the cell. If the cell does not contain an entity with a URI, the array is left empty.

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

### Connector

The repository for the connectors to communicate with Jazero can be found <a href="https://github.com/EDAO-Project/Jazero/tree/main/JDLC">here</a>.
There is both a C, Java connector, and Python connector.

Remember to run the methods to insert tables and embeddings on the machine running Jazero. Only searching can be performed remotely.

To use the Python connector, follow <a href="https://github.com/EDAO-Project/Jazero/blob/main/JDLC/python/README.md">these</a> instructions.

To use the Java connector, build the CDLC library .jar file with Maven and Java 17 running `mvn clean install` in the `CDLC` folder.
The .jar file can be found in the `target` folder and can now be included in your project.
This also needs to be done in the folders `data-lake`, `storage`, and `communication`.

For the C connector, follow the instructions <a href="https://github.com/EDAO-Project/Jazero/blob/main/JDLC/C/README.md">here</a>.

##### Loading in Java
Once the Java CDLC .jar library file has been included in your project, use the class `CDLC` to communicate with Jazero.

Use the methods `insert` and `insertEmbeddings` to insert JSON tables and RDF embedding, respectively. 
These methods only work on the machine running Jazero.
The parameters are listed below:

`insert`
- Directory containing JSON tables on machine running Jazero
- Type of storage in which to store the JSON tables in Jazero ('native' or 'hdfs')
- Prefix string of entities in the JSON tables (e.g. 'https://en.wikipedia.org/')
- Prefix string of entities in the loaded knowledge graph (e.g. 'https://dbpedia.org/')

`insertEmbeddings`
- File containing embeddings
- Delimiter separating embedding values

##### Loading in Python
Initialize the `Connector` class and call the `insert` and `insertEmbeddings` methods to insert JSON tables and RDF embeddings, respectively.
These methods only work on the machine running Jazero.
The parameters are listed below:

`insert`
- Directory containing JSON tables on machine running Jazero
- Directory of the Jazero directory on the machine running Jazero
- Type of storage in which to store the JSON tables in Jazero ('native' or 'hdfs')
- Prefix string of entities in the JSON tables (e.g. 'https://en.wikipedia.org/')
- Prefix string of entities in the loaded knowledge graph (e.g. 'https://dbpedia.org/')

`insertEmbeddings`
- Directory of the Jazero directory on the machine running Jazero
- Embeddings file path on the machine running Jazero
- Delimiter in embeddings file

### Searching Jazero
Searching follows the _query-by-example_ paradigm. Since Jazero stores tables, the input queries are exemplar tables.
These tabular, exemplar queries contain examples of data of interest.

Searching can be performed remotely from the terminal using Python and the `cdlc.py` Python file found <a href="https://github.com/EDAO-Project/Jazero/tree/main/CDLC/python">here</a>.
Run `python cdlc.py -h` to see how search Jazero.

The search output is pure JSON containing all the relevant top-_K_ tables along some search statistics.

##### Searching in Java
Include the Java CDLC .jar file in your project and use `search` method with the following parameters to search Jazero:

`search`
- Top-_K_ most similar tables
- Type of scoring/comparing entities (by entity RDF types or entity embeddings)
- The query itself in tabular form

Construct a query instance of exemplar entities using the `QueryFactory` class.

##### Searching in Python
Import the `Connector` class from the `cdlc.py` Python file. Searching requires the following parameters:

`search`
- Top-_K_ highest ranking search results
- Type of scoring pairs of entities
- Type of similarity measurement between pairs of vectors of entity scores
- Tabular query

### Jazero Web
This repository has a Django web interface to interact with an instance of Jazero.
Navigate to `CDLC/python/api/` and build the Docker image.

```bash
docker build -t jazero_web -f Dockerfile ..
```

Then, run a container of Jazero web.

```bash
docker run --rm --network="host" -d --name jazero -e JAZERO_HOST=<HOST> web
```

You can now access the Jazero web interface <a href="http://127.0.0.1:8084/cdlc/">here</a>.
Just substitute `<HOST>` with the host name of the running Jazero instance.
For demonstration purposes, we already have an instance of Jazero running, and it can be accessed using its web interface <a href="">here</a>.

You can stop the Jazero web interface with the following command.

```bash
docker stop jazero_web
```

## Setting Up Jazero in an IDE
Most of the components in Jazero are dependent on the `communication` module.
Therefore, change directory to this module and run the following to install it as a dependency

```bash
cd communication
mvn clean install
```

Now, do the same for the `storage` module, as the `data-lake` module depends on this.
