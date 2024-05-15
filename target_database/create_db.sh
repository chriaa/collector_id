#!/bin/bash

# Variables
MYSQL_CONTAINER_NAME="collector_id"
MYSQL_ROOT_PASSWORD="bacillus"  # Set your desired root password
MYSQL_DATABASE="collector_orcid_match"       # Set your desired database name

# Pull the latest MySQL image
docker pull mysql:latest

# Run the MySQL container
docker run --name $MYSQL_CONTAINER_NAME -e MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD -d mysql:latest

# Wait for the MySQL server to start (adjust the sleep time if necessary)
echo "Waiting for MySQL to start..."
sleep 30

# Create a new MySQL database
docker exec $MYSQL_CONTAINER_NAME mysql -uroot -p$MYSQL_ROOT_PASSWORD -e "CREATE DATABASE IF NOT EXISTS $MYSQL_DATABASE;"

echo "Database $MYSQL_DATABASE created successfully!"