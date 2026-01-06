#!/bin/bash

echo ""
echo "Stopping Kafka containers for datacenter failover demo"
echo "------------------------------------------------------"
docker compose -f docker-compose-dc-failover.yml down

echo ""
echo "Checking container status"
echo "-------------------------"
docker ps -a

echo ""
