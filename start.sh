#!/bin/bash

echo ""
echo "Starting Kafka clusters DC1/DC2, MirrorMaker, demo service"
echo "----------------------------------------------------------"
docker compose up -d

echo ""
echo "Checking container status"
echo "-------------------------"
docker ps -a

echo ""
