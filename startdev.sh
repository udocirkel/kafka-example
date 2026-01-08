#!/bin/bash

echo ""
echo "Starting Kafka broker and demo service"
echo "--------------------------------------"
docker compose -f docker-compose-dev.yml up -d

echo ""
echo "Checking container status"
echo "-------------------------"
docker ps -a

echo ""
