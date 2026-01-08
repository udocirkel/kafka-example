#!/bin/bash

echo ""
echo "Stopping Kafka clusters DC1/DC2, MirrorMaker, demo service"
echo "----------------------------------------------------------"
docker compose down

echo ""
echo "Checking container status"
echo "-------------------------"
docker ps -a

echo ""
