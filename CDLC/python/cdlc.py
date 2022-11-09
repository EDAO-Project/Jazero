import requests
import shutil
import json
import argparse
import os

# Connector class
class Connector:
    def __init__(self, host):
        self.__host = 'http://' + host
        self.__sdlPort = 8081
        self.__entityLinkerPort = 8082
        self.__ekgPort = 8083
        self.__RELATIVE_MOUNT = '/knowledge-graph/neo4j'

    def getHost(self):
        return self.__host

    def isConnected(self):
        sdl = requests.get(self.__host + ':' + str(self.__sdlPort) + '/ping')
        entityLinker = requests.get(self.__host + ':' + str(self.__entityLinkerPort) + '/ping')
        ekg = requests.get(self.__host + ':' + str(self.__ekgPort) + '/ping')

        return sdl.status_code == 200 and entityLinker.status_code == 200 and ekg.status_code == 200

    def insertEmbeddings(self, calypsoDir, embeddingsFile, embeddingsDelimiter):
        mountedPath = calypsoDir + self.__RELATIVE_MOUNT
        shutil.copyfile(embeddingsFile, mountedPath + '/' + embeddingsFile.split('/')[-1])

        content = '{"file": "/home/' + self.__RELATIVE_MOUNT + '/' + embeddingsFile.split('/')[-1] + '", "delimiter": "' + embeddingsDelimiter + '"}'
        j = json.loads(content)
        req = requests.post(self.__host + ':' + str(self.__sdlPort) + '/embeddings', json = j)

        if (req.status_code != 200):
            return 'Failed inserting embeddings: ' + req.text

        return req.text

    # tablesDir: Absolute path to directory of tables to be loaded into Calypso
    # calypsoDir: Directory of the Calypso repository
    # storageType: Type of storage of tables in Calypso (must be one of 'native' and 'hdfs')
    # tableEntityPrefix: Prefix string of entities in the tables (if not all table entities share the same prefix, don't specify this parameter)
    # kgEntityPrefix: Prefix string of entities in the knowledge graph (if not all KG entities share the same prefix, don't specify this parameter)
    def insert(self, tablesDir, calypsoDir, storageType, tableEntityPrefix = '', kgEntityPrefix = ''):
        relativeTablesDir = self.__RELATIVE_MOUNT + "/tables"
        sharedDir = calypsoDir + "/" + relativeTablesDir
        shutil.copytree(tablesDir, sharedDir)

        headers = {'Content-Type': 'application/json', 'Storage-Type': storageType}
        content = '{"directory": "/home/' + relativeTablesDir + '", "table-prefix": "' + tableEntityPrefix + '", "kg-prefix": "' + kgEntityPrefix + '"}'
        j = json.loads(content)
        req = requests.post(self.__host + ':' + str(self.__sdlPort) + '/insert', json = j, headers = headers)

        if (req.status_code != 200):
            return 'Failed inserting tables: ' + req.text

        return req.text

    # topK: Top-K ranking results
    # scoringType: Type of scoring pairs of tables (must be one of 'TYPE', 'COSINE_NORM', 'COSINE_ABS', 'COSINE_ANG')
    # similarityMeasure: Type of similarity measurement of between vectors of entity scores using a scoring type (must be one of 'EUCLIDEAN', 'COSINE')
    # query: A table query of entity string representations
    def search(self, topK, scoringType, query, similarityMeasure = 'EUCLIDEAN'):
        useEmbeddings = 'true'
        cosFunction = scoringType.split('_')[-1] + '_COS'

        if (scoringType == 'TYPE'):
            useEmbeddings = 'false'

        content = '{"top-k": "' + str(topK) + '", "use-embeddings": "' + useEmbeddings + '", "cosine-function": "' + cosFunction + \
                  '", "single-column-per-query-entity": "true", "weighted-jaccard": "false", "adjusted-jaccard": "true", ' + \
                  '"use-max-similarity-per-column": "true", "similarity-measure": "' + similarityMeasure + '", "query": "' + self.__toString(query) + '"}'
        j = json.loads(content)
        req = requests.post(self.__host + ':' + str(self.__sdlPort) + '/search', json = j)

        if (req.status_code != 200):
            return 'Failed searching: ' + req.text

        return req.text

    def __toString(self, query):
        strTable = ''

        for row in query:
            for column in row:
                strTable += column + '<>'

            strTable = strTable[:-2] + '#'

        strTable = strTable[:-1]
        return strTable

# Use --host to specify host and -o for operation
# Operations:
#   search: -q <QUERY FILE NAME> -sq <SCORING TYPE ('TYPE', 'COSINE_NORM', 'COSINE_ABS', 'COSINE_ANG')> -k <TOP-K> -sm <SIMILARITY MEASURE ('EUCLIDEAN', 'COSINE')>
#             Query file must be in JSON format. The structure must be as follows:
#             {
#                 "queries": [
#                   [
#                       "<KG URI 1,1>",
#                       "<KG URI 1,2>"
#                       ...
#                       "<KG URI 1,m>"
#                   ],
#                   [
#                       "<KG URI 2,1>",
#                       "<KG URI 2,2>"
#                       ...
#                       "<KG URI 2,n>"
#                   ],
#                   ...
#                   [
#                       "<KG URI 3,1>",
#                       "<KG URI 3,2>"
#                       ...
#                       "<KG URI l,o>"
#                   ]
#               ]
#           }
#
#   insert: -loc <TABLE CORPUS DIRECTORY (absolute path on machine running Calypso)> -cd <CALYPSO DIRECTORY (absolute path on machine running Calypso)> -st <STORAGE TYPE ('native' and 'hdfs')> -tp <TABLE ENTITY PREFIX> -kgp <KG ENTITY PREFIX>
#   loadembeddings: -cd <CALYPSO DIRECTORY (absolute path on machine running Calypso)> -e <EMBEDDINGS FILE (absolute path on machine running Calypso)> -d <DELIMITER>
if __name__ == '__main__':
    parser = argparse.ArgumentParser('CDLC Connector')
    parser.add_argument('--host', metavar = 'Host', type = str, help = 'Host of machine on which Calypso is deployed', required = True)
    parser.add_argument('-o', '--operation', metavar = 'Op', type = str, help = 'Calypso operation to perform (search, insert, loadembeddings)', choices = ['search', 'insert', 'loadembeddings'], required = True)
    parser.add_argument('-q', '--query', metavar = 'Query', type = str, help = 'Query file path', required = False)
    parser.add_argument('-sq', '--scoringtype', metavar = 'ScoringType', type = str, help = 'Type of entity scoring (\'TYPE\', \'COSINE_NORM\', \'COSINE_ABS\', \'COSINE_ANG\')', choices = ['TYPE', 'COSINE_NORM', 'COSINE_ABS', 'COSINE_ANG'], required = False, default = 'TYPE')
    parser.add_argument('-k', '--topk', metavar = 'Top-K', type = str, help = 'Top-K value', required = False, default = '100')
    parser.add_argument('-sm', '--similaritymeasure', metavar = 'SimilarityMeasure', type = str, help = 'Similarity measure between vectors of entity scores (\'EUCLIDEAN\', \'COSINE\')', choices = ['EUCLIDEAN', 'COSINE'], required = False, default = 'EUCLIDEAN')
    parser.add_argument('-loc', '--location', metavar = 'Location', type = str, help = 'Absolute path to table corpus directory on machine running Calypso', required = False)
    parser.add_argument('-cd', '--calypsodir', metavar = 'CalypsoDirectory', type = str, help = 'Absolute path to Calypso directory on the machine running Calypso', required = False)
    parser.add_argument('-st', '--storagetype', metavar = 'StorageType', type = str, help = 'Type of storage for inserted table corpus', choices = ['native', 'hdfs'], required = False, default = 'native')
    parser.add_argument('-tp', '--tableentityprefix', metavar = 'TableEntityPrefix', type = str, help = 'Prefix of table entity URIs', required = False, default = '')
    parser.add_argument('-kgp', '--kgentityprefix', metavar = 'KGEntityPrefix', type = str, help = 'Prefix of KG entity IRIs', required = False, default = '')
    parser.add_argument('-e', '--embeddings', metavar = 'Embeddings', type = str, help = 'Absolute path to embeddings file on the machine running Calypso', required = False)
    parser.add_argument('-d', '--delimiter', metavar = 'Delimiter', type = str, help = 'Delimiter in embeddings file (see README)', required = False, default = ' ')

    args = parser.parse_args()
    host = args.host
    op = args.operation
    conn = Connector(host)
    output = None

    if (not conn.isConnected()):
        print('Calypso is not online')
        print('Make sure all 3 Calypso services are running (check with \'docker ps\')')
        exit(1)

    if (op == 'search'):
        queryFile = args.query
        scoringType = args.scoringtype
        topK = int(args.topk)
        similarityMeasure = args.similaritymeasure
        query = []

        if (queryFile == None):
            print('Missing query file')
            exit(1)

        elif (not os.path.exists(queryFile)):
            print('Query file \'' + queryFile + '\' does not exist')
            exit(1)

        with open(queryFile, 'r') as file:
            data = json.load(file)

            for row in data['queries']:
                rowData = []

                for element in row:
                    rowData.append(element)

                query.append(rowData)

        output = conn.search(topK, scoringType, query, similarityMeasure)

    elif (op == 'insert'):
        location = args.location
        calypso = args.calypsodir
        storageType = args.storagetype
        tableEntityPrefix = args.tableentityprefix
        kgEntityPrefix = args.kgentityprefix

        if (location == None):
            print('Missing table corpus location')
            exit(1)

        elif (calypso == None):
            print('Missing directory of Calypso repository')
            exit(1)

        output = conn.insert(location, calypso, storageType, tableEntityPrefix, kgEntityPrefix)

    elif (op == 'loadembeddings'):
        calypso = args.calypsodir
        embeddings = args.embeddings
        delimiter = args.delimiter

        if (calypso == None):
            print('Missing directory of Calypso repository')
            exit(1)

        elif (embeddings == None):
            print('Missing embeddings file')
            exit(1)

        output = conn.insertEmbeddings(calypso, embeddings, delimiter)

    print(output)