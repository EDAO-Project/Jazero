#!/bin/bash

file=$1
neo4j=$2

export NEO4J_HOME=$neo4j
export NEO4J_IMPORT=$neo4j"/import"

export DATA_IMPORT="${PWD}/files"
export NEO4J_DB_DIR=$NEO4J_HOME/neo4j-server/data/databases/graph.db
ulimit -n 65535

echo "Moving and cleaning"
FILE_CLEAN="$(basename "${file}")"
iconv -f utf-8 -t ascii -c "${file}" | grep -E '^<(https?|ftp|file)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[A-Za-z0-9\+&@#/%?=~_|]>\W<' | grep -Fv 'xn--b1aew' > ${NEO4J_IMPORT}/${FILE_CLEAN}

echo "Importing"
filename="$(basename "$(${NEO4J_IMPORT}/${FILE_CLEAN})")"

echo "importing $filename from ${NEO4J_HOME}"
${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'admin' "CALL  n10s.rdf.import.fetch(\"file://${NEO4J_IMPORT}/$filename\",\"Turtle\");"
rm -v ${NEO4J_IMPORT}/${FILE_CLEAN}

echo "Done"