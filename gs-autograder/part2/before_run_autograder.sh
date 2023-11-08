#!/bin/bash


nohup cassandra -R > cassandra.log 2>&1 &

# Wait for Cassandra to start
MAX_TRIES=30
COUNT=0
while ! cqlsh -e "DESCRIBE CLUSTER" > /dev/null 2>&1; do
    sleep 1
    COUNT=$((COUNT + 1))
    if [[ $COUNT -eq $MAX_TRIES ]]; then
        echo "Cassandra did not start after $MAX_TRIES seconds. Exiting."
        exit 1
    fi
done

cqlsh -e "CREATE KEYSPACE demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"

