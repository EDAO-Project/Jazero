# Calypso
Welcome to Calypso: A semantic data lake microservice architecture for semantically augmented table search.

## Setup
### Loading Knowledge Graph
From the root directory of this repository, create a folder `input` in in `knowledge-graph/neo4j/`

```bash
mkdir knowledge-graph/neo4j/input
```

Place all your knowledge graph turtle files in this new folder `input`.
Run the following commands in the project root directory to install Neo4J in Docker and insert the knowledge graph into a dockerized Neo4J instance

```bash
docker build -f kg.dockerfile -t neo4j .
docker run --rm -v ${PWD}/knowledge-graph/neo4j:/srv neo4j bash -c "./install.sh /srv; ./import.sh neo4j-server; ./stop.sh neo4j-server"
```

## Starting Calypso
Setting up and running Calypso is very simple.
All you need is to have Docker and Docker-compose installed. Make sure to have Docker-compose version 2+ installed.

Start Calypso with the following simple command

```bash
docker-compose up
```

The first time Calypso is started, the entity linker service will build a Lucene index, which can take around 1.5 hours for a 10GB KG.
Now, Calypso is accessible on localhost or on the machine's IP address.

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

## Working with Calypso
[Here will be some instructions on using the CDLC driver and maybe also the API if we build that]
[Describe how to populate with tables and how to insert embeddings]

Here we describe working with Calypso, how to load Calypso with tables, load indexes, load embeddings, and searching in Calypso.

### Loading Calypso
Tables in Calypso are loaded and stored either natively on disk or in HDFS (HDFS is not yet supported).

The tables must be in JSON format with the fields __id_, _numDataRows_, _numCols_, and _rows_, where _rows_ is an array of table rows.
An example of a table is given below:

```json
{
  "_id": "1514-27", 
  "numCols": 4, 
  "numDataRows": 26, 
  "rows": [
    [
      {
        "text": "1900 Paris", 
        "links": ["http://www.wikipedia.org/wiki/Athletics_at_the_1900_Summer_Olympics"]
      }, 
      {
        "text": "Walter Tewksbury", 
        "links": ["http://www.wikipedia.org/wiki/Walter_Tewksbury"]
      }
    ],
    [
      {
        "text": "1904 St. Louis", 
        "links": ["http://www.wikipedia.org/wiki/Athletics_at_the_1904_Summer_Olympics"]
      }, 
      {
        "text": "Harry Hillman", 
        "links": ["http://www.wikipedia.org/wiki/Harry_Hillman"]
      }
    ]
  ]
}
```

Calypso also uses embeddings to represent embeddings.
Consider using <a href="https://github.com/EDAO-Project/DBpediaEmbedding">this</a> repository to generate RDF embeddings.

Every entity embedding must be contained in one line in the embeddings file, and each value must be separated by the same delimiter.
This includes the entity itself. Below is an example of what an embeddings file of three entities could look like where the space character is the delimiter:

```
https://dbpedia.org/page/Barack_Obama -3.21 13.2122 53.32 -2.23 0.4353 8.231
https://dbpedia.org/page/Lionel_Messi 2.432 9.3213 -32.231 21.432 -21.022 53.1133
https://dbpedia.org/page/Eiffel_Tower -34.422 -7.231 5.312 -1.435 0.543 12.440
```

The repository for the connector to communicate with Calypso can be found <a href="https://github.com/EDAO-Project/Calypso/tree/main/CDLC">here</a>.
There is both a Java connector and Python connector.

To use the Java connector, build the CDLC library .jar file with Maven and Java 17 running `mvn clean install` in the `CDLC` folder.
The .jar file can be found in the `target` folder and can now be included in your project.
This also needs to be done in the folders `data-lake`, `storage`, and `communication`.

The Python connector can be found in the `cdlc.py` file <a href="https://github.com/EDAO-Project/Calypso/tree/main/CDLC/python">here</a>.
Import the `Connector` class and initialize it with a host name as constructor argument, such as `localhost`, `127.0.0.1`, or something else if you want to connect to Calypso from another machine
You can use the Python connector directly from the terminal. Just run `python cdlc.py -h` to see commands.
Remember to run the methods to insert tables and embeddings on the machine running Calypso. Only searching can be performed remotely.

Be aware that loading a table corpus of 200,000 tables will take approximately one week to load.

#### Loading in Java
Once the Java CDLC .jar library file has been included in your project, use the class `CDLC` to communicate with Calypso.

Use the methods `insert` and `insertEmbeddings` to insert JSON tables and RDF embedding, respectively. 
It is very important these methods are executed on the machine running Calypso!
The parameters are listed below:

`insert`
- Directory containing JSON tables on machine running Calypso
- Type of storage in which to store the JSON tables in Calypso ('native' or 'hdfs')
- Prefix string of entities in the JSON tables (e.g. 'https://en.wikipedia.org/')
- Prefix string of entities in the loaded knowledge graph (e.g. 'https://dbpedia.org/')

`insertEmbeddings`
- File containing embeddings
- Delimiter separating embedding values

#### Loading in Python
Initialize the `Connector` class and call the `insert` and `insertEmbeddings` methods to insert JSON tables and RDF embeddings, respectively.
It is very important these methods are executed on the machine running Calypso!
The parameters are listed below:

`insert`
- Directory containing JSON tables on machine running Calypso
- Directory of the Calypso directory on the machine running Calypso
- Type of storage in which to store the JSON tables in Calypso ('native' or 'hdfs')
- Prefix string of entities in the JSON tables (e.g. 'https://en.wikipedia.org/')
- Prefix string of entities in the loaded knowledge graph (e.g. 'https://dbpedia.org/')

`insertEmbeddings`
- Directory of the Calypso directory on the machine running Calypso
- Embeddings file path on the machine running Calypso
- Delimiter in embeddings file

### Searching Calypso
Searching follows the query-by-example paradigm. Since Calypso stores tables, the input queries are also tables.
These tabular queries contain examples of data of interest.

Searching can be performed remotely from the terminal using Python and the `cdlc.py` Python file found <a href="https://github.com/EDAO-Project/Calypso/tree/main/CDLC/python">here</a>.
Run `python cdlc.py -h` to see how search Calypso.

#### Searching in Java
Include the Java CDLC .jar file in your project and use `search` method with the following parameters to search Calypso:

`search`
- Top-_K_ most similar tables
- Type of scoring/comparing entities (by entity RDF types or entity embeddings)
- The query itself in tabular form

Construct a query instance of exemplar entities using the `QueryFactory` class.

#### Searching in Python
Import the `Connector` class from the `cdlc.py` Python file. Searching requires the following parameters:

`search`
- Top-K highest ranking search results
- Type of scoring pairs of entities
- Type of similarity measurement between pairs of vectors of entity scores
- Tabular query

## Setting Up Calypso in an IDE
Most of the components in Calypso are dependent on the `communication` module.
Therefore, change directory to this module and run the following to install it as a dependency

```bash
cd communication
mvn clean install
```

Now, do the same for the `storage` module, as the `data-lake` module depends on this.