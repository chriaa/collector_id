#!/bin/bash

docker container stop collector_id
docker container rm collector_id


# Variables
MYSQL_CONTAINER_NAME="collector_id"
MYSQL_ROOT_PASSWORD="bacillus"  # Set your desired root password
MYSQL_DATABASE="collector_orcid_match"       # Set your desired database name
MYSQL_USER="christina"        # Set your desired username
MYSQL_PASSWORD="password123"  # Set your desired password for the new user
MYSQL_PORT=3310               # Set your desired port for MySQL

# Pull the latest MySQL image
docker pull mysql:latest

# Run the MySQL container
docker run --name $MYSQL_CONTAINER_NAME \
    -e MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD \
    -e MYSQL_DATABASE=$MYSQL_DATABASE \
    -e MYSQL_USER=$MYSQL_USER \
    -e MYSQL_PASSWORD=$MYSQL_PASSWORD \
    -p $MYSQL_PORT:3306 \
    -d mysql:latest

# Wait for the MySQL server to start (adjust the sleep time if necessary)
echo "Waiting for MySQL to start..."
sleep 30

# Grant all privileges to the new user on the database
docker exec $MYSQL_CONTAINER_NAME mysql -uroot -p$MYSQL_ROOT_PASSWORD -e "GRANT ALL PRIVILEGES ON *.* TO '$MYSQL_USER'@'%' IDENTIFIED BY '$MYSQL_PASSWORD'; FLUSH PRIVILEGES;"

echo "Database $MYSQL_DATABASE created successfully with user $MYSQL_USER and accessible on port $MYSQL_PORT!"
