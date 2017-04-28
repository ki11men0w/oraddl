#!/bin/sh

mkdir -p ./lib
curl -L --retry 5 https://github.com/ki11men0w/resources/raw/master/oracle/instantclient/libclntsh.so.12.1.gz     > ./lib/libclntsh.so.12.1.gz
curl -L --retry 5 https://github.com/ki11men0w/resources/raw/master/oracle/instantclient/libclntshcore.so.12.1.gz > ./lib/libclntshcore.so.12.1.gz
curl -L --retry 5 https://github.com/ki11men0w/resources/raw/master/oracle/instantclient/libipc1.so.gz            > ./lib/libipc1.so.gz
curl -L --retry 5 https://github.com/ki11men0w/resources/raw/master/oracle/instantclient/libmql1.so.gz            > ./lib/libmql1.so.gz
curl -L --retry 5 https://github.com/ki11men0w/resources/raw/master/oracle/instantclient/libnnz12.so.gz           > ./lib/libnnz12.so.gz
curl -L --retry 5 https://github.com/ki11men0w/resources/raw/master/oracle/instantclient/libons.so.gz             > ./lib/libons.so.gz
gzip -d ./lib/*.gz
ln -s libclntsh.so.12.1 ./lib/libclntsh.so
