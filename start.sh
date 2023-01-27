#!/bin/bash

set -e

mkdir -p index/neo4j
mkdir -p index/mappings
mkdir -p logs
mkdir -p .tables
docker-compose up