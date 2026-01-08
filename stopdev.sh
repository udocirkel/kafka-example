#!/bin/bash

echo ""
echo "Stopping Kafka broker and demo service"
echo "--------------------------------------"
docker compose -f docker-compose-dev.yml down

echo ""
echo "Checking container status"
echo "-------------------------"
docker ps -a

echo ""
