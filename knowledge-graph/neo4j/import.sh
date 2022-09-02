#!/bin/bash

set -e

file="${PWD}/$1"
neo4j="${PWD}/$2"

export NEO4J_HOME=$neo4j
export NEO4J_IMPORT=${NEO4J_HOME}"/import"

ulimit -n 65535

echo "Moving and cleaning"
rm ${NEO4J_IMPORT}/*

FILE_CLEAN="kg.ttl"
touch ${NEO4J_IMPORT}/${FILE_CLEAN}

if [ -d ${file} ]
then
  for f in ${file}/* ; \
  do
    iconv -f utf-8 -t ascii -c "${f}" | grep -E '^<(https?|ftp|file)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[A-Za-z0-9\+&@#/%?=~_|]>\W<' | grep -Fv 'xn--b1aew' >> ${NEO4J_IMPORT}/${FILE_CLEAN}
  done
else
  iconv -f utf-8 -t ascii -c "${file}" | grep -E '^<(https?|ftp|file)://[-A-Za-z0-9\+&@#/%?=~_|!:,.;]*[A-Za-z0-9\+&@#/%?=~_|]>\W<' | grep -Fv 'xn--b1aew' >> ${NEO4J_IMPORT}/${FILE_CLEAN}
fi

echo "importing $filename from ${NEO4J_HOME}"
${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'admin' "CALL  n10s.rdf.import.fetch(\"file://${NEO4J_IMPORT}/${FILE_CLEAN}\",\"Turtle\");"

echo "Done"
