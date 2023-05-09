# JDLC Connector
This is the Python connector to the Jazero semantic data lake.
It comes with a Python pip package and a Python3 script that can be used as a terminal tool.

## Installation
Install the JDLC connector with pip:

```bash
python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps example-package-YOUR-USERNAME-HERE
```

## Package Instructions
You can import the package in your Python code as follows:

```python
from jdlc import Connector as jdlc
...
```

The class `Connector` provides three functions:
- Insertion of embeddings
- Insertion of tables
- Semantic search of tables

The class only requires the IP address in its constructor.

Remember to perform insertion of embeddings before loading tables, as the embeddings are used to build the indexes.

Remember also that insertion of embeddings and tables must be performed locally on the machine running Jazero.

## Inserting Embeddings
Use the function `insertEmbeddings` with the following parameters:

- Directory of Jazero on the machine
- Path to the file containing the embeddings
- Delimiter used in the embeddings file

Below is an example of inserting embeddings:

```python
conn = Connector('localhost')
output = conn.insertEmbeddings('/home/Jazero', '/home/embeddings.txt', ' ')
```

The `output` is a string with some quick, simple insertion statistics.

## Loading Tables
Use the function `insert` with the following parameters:

- Directory containing tables
- Directory of Jazero on the machine
- Storage type (NATIVE for disk or HDFS for Apache HDFS)
- Prefix string of table cells (if all cells do not share the same prefix, use an empty string)
- Prefix string of knowledge graph entities
- Signature size of LSH index (default is 30)
- Band size of LSH signatures (default is 10)

Below is an example of inserting tables:

```python
conn = Connector('localhost')
output = conn.insert('/home/tables', '/home/Jazero', 'NATIVE', ' ', 'https://en.dbpedia.org', 128, 8)
```

The `output` is a string with some quick, simple insertion statistics.

## Searching
Use the function `search` with the following parameters:

- Top-_K_
- Scoring type (TYPE to use Jaccard similaroty of knowledge graph entity types or one of COSINE_NORM, 'COSINE_ABS, and COSINE_ANG to use cosine similarity of entity embeddings)
- Query represented as a list of lists (matrix) of query entities
- Similarity measure to aggregate intermediate entity score vectors (defauls is EUCLIDEAN, but alternative is COSINE)
- Type of LSH index for table pre-filtering (default is none, but one of TYPES and EMBEDDINGS can be used to approximately filter the search space from irrelevant tables using entity types or embeddings, respectively)

Below is an example of semantic table search:

```python
query = [
          [https://en.dbpedia.org/page/Barack_Obama, https://en.dbpedia.org/page/Joe_Biden, https://en.dbpedia.org/page/White_House], 
          [https://en.dbpedia.org/page/Joe_Biden, https://en.dbpedia.org/page/Kamala_Harris, https://en.dbpedia.org/page/White_House]
        ]
conn = Connector('loalhost') # this can now also be a remote IP
output = conn.search(10, TYPE, query)
```

The `output` is a JSON string containing all the top-_K_ results including some simple statistics about runtime and search space reduction from LSH pre-filtering.

## Terminal Instructions
You can simply use the <a href="https://github.com/EDAO-Project/Jazero/blob/main/JDLC/python/jdlc/jdlc.py">this file</a> directly in the terminal to perform insertion of embeddings and tables and to search for semantically relevant tables.

To list all operations and arguments, use the `-h` flag as such: `python jdlc.py -h`.

All operations use the same arguments as listed above when using the Python class.
Below is a list of examples for performing each operation.

### Inserting Embeddings Example

```python
python jdlc.py --host localhost --operation loadembeddings --jazerodir /home/Jazero --embeddings /home/embeddings.txt --delimiter ' '
```

### Inserting Tables Example

```python
python jdlc.py --host localhost --operation insert --location /home/tables --jazerodir /home/Jazero --storagetype NATIVE --tableentityprefix ' ' --kgentityprefix "https://en.dbpedia.org" --signaturesize 30 --bandsize 10
```

### Searching Tables Example

```python
python jdlc.py --host localhost --operation search --query /home/query.json --scoringtype COSINE_NORM --topk 100 --similaritymeasure EUCLIDEAN --prefilter EMBEDDINGS
```

The query must be a JSON on the following format:

```json
{
  "queries" [
    [https://en.dbpedia.org/page/Barack_Obama, https://en.dbpedia.org/page/Joe_Biden, https://en.dbpedia.org/page/White_House],
    [https://en.dbpedia.org/page/Joe_Biden, https://en.dbpedia.org/page/Kamala_Harris, https://en.dbpedia.org/page/White_House]
   ]
}
```
