#!/usr/bin/env bash

# packages required for autograder
apt-get install -y python3 openjdk-11-jdk-headless


# # install cassandra
echo "deb https://debian.cassandra.apache.org 41x main" | tee -a /etc/apt/sources.list.d/cassandra.sources.list
curl https://downloads.apache.org/cassandra/KEYS | apt-key add -
apt-get update
apt-get install -y cassandra

# Remove set ulimit for sake of auto grader
sed -i '72,73d' /etc/init.d/cassandra
