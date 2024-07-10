#!/bin/bash

# https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/
#------------------------------------------------------------------------------
#-- Post/Sink to Local Mongo container

. ./.pwdmongolocal

# above file contains:
#
# LOCAL
# export MONGO_URL=mongodb://hostname:port/?directConnection=true


curl -X POST \
  -H "Content-Type: application/json" \
  --data '
      {"name": "mongo-local-salesbaskets-sink-pb",
        "config": {
          "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
          "connection.uri": "'${MONGO_URL}'",
          "key.converter": "org.apache.kafka.connect.storage.StringConverter",
          "value.converter":"io.confluent.connect.protobuf.ProtobufConverter",
          "value.converter.schema.registry.url":"http://schema-registry:8081",
          "value.converter.schemas.enable": true,
          "database":"MongoCom0",
          "collection":"pb_salesbaskets",
          "topics":"pb_salesbaskets"
          }
      }
      ' \
  http://localhost:8083/connectors -w "\n"


curl -X POST \
  -H "Content-Type: application/json" \
  --data '
      {"name": "mongo-local-salespayments-sink-pb",
        "config": {
          "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
          "connection.uri": "'${MONGO_URL}'",
          "key.converter": "org.apache.kafka.connect.storage.StringConverter",
          "value.converter":"io.confluent.connect.protobuf.ProtobufConverter",
          "value.converter.schema.registry.url":"http://schema-registry:8081",
          "value.converter.schemas.enable": true,
          "database":"MongoCom0",
          "collection":"pb_salespayments",
          "topics":"pb_salespayments"
          }
      }
      ' \
  http://localhost:8083/connectors -w "\n"

curl -X POST \
  -H "Content-Type: application/json" \
  --data '
     { "name": "mongo-local-salescompleted-sink-pb",
        "config": {
          "connector.class":"com.mongodb.kafka.connect.MongoSinkConnector",
          "connection.uri": "'${MONGO_URL}'",
          "key.converter": "org.apache.kafka.connect.storage.StringConverter",
          "value.converter":"io.confluent.connect.protobuf.ProtobufConverter",
          "value.converter.schema.registry.url":"http://schema-registry:8081",
          "value.converter.schemas.enable": true,
          "database":"MongoCom0",
          "collection":"pb_salescompleted",
          "topics":"pb_salescompleted"
          }
      }
      ' \
  http://localhost:8083/connectors -w "\n"



