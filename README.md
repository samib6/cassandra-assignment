# Consistent Distributed Key-Value Store #

## Overview ##

**Goals**: The main goals of this assignment are to learn 

1. how to build a distributed data storage system with consistency requirements;
2. to work with a popular, widely used key-value data store;

**Parts**: You will implement a simple distributed data storage system using a so-called NoSQL key-value system, Cassandra, in two progressive parts:

1. Part 1: A single-node data store wrapper based on Cassandra and a non-blocking client;
2. Part 2: A distributed wrapper (or middleware) to ensure totally ordered writes on servers; 

Note that Cassandra itself has powerful distribution and consistency capabilities, but in this assignment, you will use Cassandra as a simple, single-node storage system and you will yourself implement the distributed coordination features in your server-side middleware as illustrated in the figure below (and detailed in Section 3).

![System overview](overview.png)

                                    

You need to implement the following two programs (shown in yellow) in the figure above: 
A server-side distributed middleware that uses Cassandra as a back-end data store; 
A client that interoperates with your distributed middleware. 


* Quick summary
* Version
* [Learn Markdown](https://bitbucket.org/tutorials/markdowndemo)

### How do I get set up? ###

* Summary of set up
* Configuration
* Dependencies
* Database configuration
* How to run tests
* Deployment instructions

### Contribution guidelines ###

* Writing tests
* Code review
* Other guidelines

### Who do I talk to? ###

* Repo owner or admin
* Other community or team contact