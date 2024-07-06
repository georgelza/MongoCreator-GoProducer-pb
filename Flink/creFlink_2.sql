
-- Source topic is Avro serialized (Pb requires a PB Serdes be compiled into the Flink containers, it's not included default)
-- key is based the invnumber (as it was used t join salesbaskets and salespayments)
-- Flink UI : http://localhost:9081/#/overview

CREATE TABLE avro_salesbaskets (
    INVOICENUMBER STRING,
    SALEDATETIME STRING,
    SALETIMESTAMP STRING,
    TERMINALPOINT STRING,
    NETT DOUBLE,
    VAT DOUBLE,
    TOTAL DOUBLE,
    STORE row<ID STRING, NAME STRING>,
    CLERK row<ID STRING, NAME STRING>,
    BASKETITEMS array<row<ID STRING, NAME STRING, BRAND STRING, CATEGORY STRING, PRICE DOUBLE, QUANTITY INT>>,
    SALESTIMESTAMP_WM AS TO_TIMESTAMP(FROM_UNIXTIME(CAST(SALETIMESTAMP AS BIGINT) / 1000)),
    WATERMARK FOR SALESTIMESTAMP_WM AS SALESTIMESTAMP_WM
) WITH (
    'connector' = 'kafka',
    'topic' = 'avro_salesbaskets',
    'properties.bootstrap.servers' = 'broker:29092',
    'scan.startup.mode' = 'earliest-offset',
    'properties.group.id' = 'testGroup',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.schema-registry.url' = 'http://schema-registry:8081',
    'value.fields-include' = 'EXCEPT_KEY'
);


CREATE TABLE avro_salespayments (
    INVOICENUMBER STRING,
    FINTRANSACTIONID STRING,
    PAYDATETIME STRING,
    PAYTIMESTAMP STRING,
    PAID DOUBLE,
    PAYTIMESTAMP_WM AS TO_TIMESTAMP(FROM_UNIXTIME(CAST(PAYTIMESTAMP AS BIGINT) / 1000)),
    WATERMARK FOR PAYTIMESTAMP_WM AS PAYTIMESTAMP_WM
) WITH (
    'connector' = 'kafka',
    'topic' = 'avro_salespayments',
    'properties.bootstrap.servers' = 'broker:29092',
    'scan.startup.mode' = 'earliest-offset',
    'properties.group.id' = 'testGroup',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.schema-registry.url' = 'http://schema-registry:8081',
    'value.fields-include' = 'EXCEPT_KEY'
);


--    'key.format' = 'raw',
--    'key.fields' = 'INVOICENUMBER',

-- https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/formats/avro-confluent/
-- Join 2 tables into avro_salescompleted_x
CREATE TABLE avro_salescompleted_x (
    -- one column mapped to the Kafka raw UTF-8 key
    the_kafka_key STRING,

      -- a few columns mapped to the Avro fields of the Kafka value
    INVOICENUMBER STRING,
    SALEDATETIME STRING,
    SALETIMESTAMP STRING,
    TERMINALPOINT STRING,
    NETT DOUBLE,
    VAT DOUBLE,
    TOTAL DOUBLE,
    STORE row<ID STRING, NAME STRING>,
    CLERK row<ID STRING, NAME STRING>,
    BASKETITEMS array<row<ID STRING, NAME STRING, BRAND STRING, CATEGORY STRING, PRICE DOUBLE, QUANTITY INT>>,
    FINTRANSACTIONID STRING,
    PAYDATETIME STRING,
    PAYTIMESTAMP STRING,
    PAID DOUBLE,
    SALESTIMESTAMP_WM AS TO_TIMESTAMP(FROM_UNIXTIME(CAST(SALETIMESTAMP AS BIGINT) / 1000)),
    PAYTIMESTAMP_WM AS TO_TIMESTAMP(FROM_UNIXTIME(CAST(PAYTIMESTAMP AS BIGINT) / 1000)),
    WATERMARK FOR SALESTIMESTAMP_WM AS SALESTIMESTAMP_WM
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'avro_salescompleted_x',
    'properties.bootstrap.servers' = 'broker:29092',
    'key.format' = 'raw',
    'key.fields' = 'the_kafka_key',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = 'http://schema-registry:8081',
    'value.fields-include' = 'EXCEPT_KEY'
);


-- Aggregate query/worker
Insert into avro_salescompleted_x
    SELECT 
        b.INVOICENUMBER as the_kafka_key,
        b.INVOICENUMBER,
        a.SALEDATETIME,
        a.SALETIMESTAMP,
        a.TERMINALPOINT,
        a.NETT,
        a.VAT,
        a.TOTAL,
        a.STORE,
        a.CLERK,
        a.BASKETITEMS,        
        b.FINTRANSACTIONID,
        b.PAYDATETIME,
        b.PAYTIMESTAMP,
        b.PAID
    FROM 
        avro_salesbaskets a LEFT JOIN 
        avro_salespayments b
    ON a.INVOICENUMBER = b.INVOICENUMBER;

