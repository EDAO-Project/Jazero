import requests
import shutil
import json

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

if __name__ == "__main__":
    pass