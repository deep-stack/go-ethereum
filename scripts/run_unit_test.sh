#!/bin/bash

set -e

# Build database image
docker build -t migrations-test https://github.com/vulcanize/ipld-eth-db.git#sharding

mkdir -p out

# Remove existing docker-multi-node directory
rm -rf out/docker-multi-node/

# Copy over files to setup multi-node database
ID=$(docker create migrations-test)
docker cp $ID:/app/docker-multi-node out/docker-multi-node/
docker rm -v $ID

# Spin up multi-node database
docker-compose -f out/docker-multi-node/docker-compose.test.yml -f docker-compose.yml up ipld-eth-db
sleep 20

# Run unit tests
go clean -testcache
make statedifftest

# Clean up
docker-compose -f out/docker-multi-node/docker-compose.test.yml -f docker-compose.yml down --remove-orphans --volumes
rm -rf out/docker-multi-node/
