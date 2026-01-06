#!/bin/bash

echo ""
echo "Starting Kafka containers for datacenter failover demo"
echo "------------------------------------------------------"
docker compose -f docker-compose-dc-failover.yml up -d

echo ""
echo "Checking container status"
echo "-------------------------"
docker ps -a

echo ""
