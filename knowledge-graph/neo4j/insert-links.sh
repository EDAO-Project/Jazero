#!/bin/bash
#####################################################
# Populate the neo4j database for the www18 dataset #
#####################################################

neo4j=$1
folder=$2

export NEO4J_HOME=$neo4j
export NEO4J_IMPORT="${NEO4J_HOME}/import"
export DATA_IMPORT=$folder
export NEO4J_DB_DIR=$NEO4J_HOME/data/databases/graph.db
ulimit -n 65535

echo "Moving and cleaning"
#cp -r ${DATA_IMPORT}/* ${NEO4J_IMPORT}/
for FILEIN in ${DATA_IMPORT}/*.ttl
do
    FILE_CLEAN="$(basename "${FILEIN}")"
    iconv -f utf-8 -t ascii -c "${FILEIN}" | grep -E '^<(https?|ftp|file)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[A-Za-z0-9\+&@#/%?=~_|]>\W<' | grep -Fv 'xn--b1aew' > ${NEO4J_IMPORT}/${FILE_CLEAN}
done

echo "Importing"
for file in ${NEO4J_IMPORT}/*.ttl*; do
    # Extracting filename
    # echo $file
    echo ""
    filename="$(basename "${file}")"
    echo "Importing $filename from ${NEO4J_HOME}"
    ${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'admin' "CALL  n10s.rdf.import.fetch(\"file://${NEO4J_IMPORT}/$filename\",\"Turtle\");"
    rm -v ${file}
done

echo "Done"
