#!/bin/bash


#docker exec broker kafka-topics --create --if-not-exists --bootstrap-server mbp.local:9092 --config retention.ms=604800000,cleanup.policy=delete --topic xpb_salesbaskets
docker exec broker kafka-topics --create --if-not-exists --bootstrap-server mbp.local:9092 --partitions 1 --replication-factor 1 --topic xpb_salespayments

# docker exec -it broker /bin/bash
# Now create Schemas