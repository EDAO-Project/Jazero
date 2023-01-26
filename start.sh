#!/bin/bash

set -e

mkdir -p index/neo4j
mkdir -p logs
mkdir -p .tables
docker-compose up