# Some Apache Flink Notes

So this follows the activities from the kSql section.

We use the same avro_salesbaskets and avro_salespayments source topics (this is because flink does not natively support Pb deserialization, you have to add the libraries to your containers). These 2 topics are then pulled into flink as 2 tables, avro serialized. We now create a table avro_salescompleted by joining the 2 source tables. 

We now run a query aggregate on sales per store per terminal per hour (and per 5 min for development purposes).

This all is backed by a Kafka topic which can be sinked via a connect back into our back end data store, for this project this being MongoDB ATLAS.