# Consistent Distributed Key-Value Store #

## Overview ##

**Goals**: The main goals of this assignment are to learn 

1. how to build a distributed data storage system with consistency requirements;
2. to work with a popular, widely used key-value data store.

**Parts**: You will implement a simple distributed data storage system using a so-called NoSQL key-value system, Cassandra, in two progressive parts:

1. Part 1: A single-node data store wrapper based on Cassandra and a non-blocking client;
2. Part 2: A distributed wrapper (or middleware) to ensure totally ordered writes on servers; 

Note that Cassandra itself has powerful distribution and consistency capabilities, but in this assignment, you will use Cassandra as a simple, single-node storage system and you will yourself implement the distributed coordination features in your server-side middleware as illustrated in the figure below (and detailed in Section 3).

![System overview](images/overview.png)
                                    

You need to implement the following two programs (shown in yellow) in the figure above: 

1. A server-side distributed middleware that uses Cassandra as a back-end data store; 
2. A client that interoperates with your distributed middleware. 

## Preparation ##

You will need to complete the following steps before you can get started on the assignment.

* **Step 1**: Check out the git repository for 590CC using the following shell command
    git clone git@bitbucket.org:distrsys/consistent-db.git

* Step 2: You need Java 8 or higher on your machine for the assignment code provided. You may need a higher minor or major version number depending on the Cassandra version you use.

* Step 3: Complete the Cassandra Getting Started tutorial. By the end of the tutorial, you should know how to write a Java client to create a keyspace, create a table, insert some records, read records, delete records, etc.


