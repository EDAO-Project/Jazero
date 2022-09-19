#!/bin/bash

set -e

input="${PWD}/input"
neo4j="${PWD}/$1"

export NEO4J_HOME=$neo4j
export NEO4J_IMPORT=${NEO4J_HOME}"/import"

ulimit -n 65535

echo "Moving and cleaning"
rm -rf ${NEO4J_IMPORT}/*

FILE_MOVED="kg.ttl"
touch ${NEO4J_IMPORT}/${FILE_CLEAN}

for f in ${input}/* ; \
do
  FILE_CLEAN="$(basename "${f}")"
  iconv -f utf-8 -t ascii -c "${f}" | grep -E '^<(https?|ftp|file)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[A-Za-z0-9\+&@#/%?=~_|]>\W<' | grep -Fv 'xn--b1aew' > ${NEO4J_IMPORT}/${FILE_CLEAN}
done

mkdir -p kg

echo "Importing..."
for f in ${NEO4J_IMPORT}/* ; \
do
  filename="$(basename ${f})"

  echo "importing ${filename} from ${NEO4J_IMPORT}"
  ${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'admin' "CALL  n10s.rdf.import.fetch(\"file://${NEO4J_IMPORT}/${filename}\",\"Turtle\");"
  mv ${NEO4J_IMPORT}/${filename} kg
done

echo "Done"
