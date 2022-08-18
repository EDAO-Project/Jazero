#!/bin/bash

neo4j=$1

export NEO4J_HOME=$neo4j

echo "Exporting..."
${NEO4J_HOME}/bin/cypher-shell -u neo4j -p 'admin' "CALL  n10s.rdf.export.cypher(\"MATCH (n) RETURN n\");"
