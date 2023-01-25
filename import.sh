#!/bin/bash

set -e

KG_DATA_DIR=$1
INPUT_DIR="knowledge-graph/neo4j/input/"

if [ "$#" -ne 1 ]; then
    echo "Expected folder with KG files (.ttl)"
    exit 1
fi

if [ -d ${KG_DATA_DIR} ]
then
  mkdir -p ${INPUT_DIR}
  mv ${KG_DATA_DIR}/* ${INPUT_DIR}
  rmdir ${KG_DATA_DIR}
  echo "The KG files have been moved to '${input}'"
  echo
  echo "Setting up Neo4J and importing data..."

  docker build -f kg.dockerfile -t neo4j .
  docker run --rm -v ${PWD}/knowledge-graph/neo4j:/srv neo4j bash -c "./install.sh /srv; ./import.sh neo4j-server; ./stop.sh neo4j-server"
  echo
  echo "Done"
else
  echo "Directory '${KG_DATA_DIR}' does not exist"
fi