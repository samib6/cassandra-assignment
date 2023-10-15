#!/bin/bash

cqlsh -e "DROP KEYSPACE IF EXISTS demo"
pkill -f CassandraDaemon
