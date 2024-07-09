
-- Source topic is Avro serialized (Pb requires a PB Serdes be compiled into the Flink containers, it's not included default)
-- key is based the invnumber (as it was used t join salesbaskets and salespayments)
-- Flink UI : http://localhost:9081/#/overview

-- The below builds avro_salescompleted locally (on Apache Flink environment) as a output of a join, the results are inserted into avro_salescompleted.
-- After this we then do the per store per terminal per hour aggregation/calculations.
-- this is done here this way to "lighten" the load/dependency on Kafka stream processing, and well, as another method/arrow in quiver.

-- pull the avro_salesbaskets topic into Flink
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
    WATERMARK FOR SALESTIMESTAMP_WM AS SALESTIMESTAMP_WM,
    PRIMARY KEY (INVOICENUMBER) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'avro_salesbaskets',
    'properties.bootstrap.servers' = 'broker:29092',
    'key.format' = 'raw',
    'properties.group.id' = 'testGroup',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = 'http://schema-registry:8081',
    'value.fields-include' = 'ALL'
);

-- pull the avro_salespayments topic into Flink
CREATE TABLE avro_salespayments (
    INVOICENUMBER STRING,
    FINTRANSACTIONID STRING,
    PAYDATETIME STRING,
    PAYTIMESTAMP STRING,
    PAID DOUBLE,
    PAYTIMESTAMP_WM AS TO_TIMESTAMP(FROM_UNIXTIME(CAST(PAYTIMESTAMP AS BIGINT) / 1000)),
    WATERMARK FOR PAYTIMESTAMP_WM AS PAYTIMESTAMP_WM,
    PRIMARY KEY (INVOICENUMBER) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'avro_salespayments',
    'properties.bootstrap.servers' = 'broker:29092',
    'key.format' = 'raw',
    'properties.group.id' = 'testGroup',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = 'http://schema-registry:8081',
    'value.fields-include' = 'ALL'
);

-- Our avro_salescompleted output table which will push values to the Kafka topic.
-- https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/formats/avro-confluent/
CREATE TABLE avro_salescompleted_x (
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
    WATERMARK FOR SALESTIMESTAMP_WM AS SALESTIMESTAMP_WM,
    PRIMARY KEY (INVOICENUMBER) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'avro_salescompleted_x',
    'properties.bootstrap.servers' = 'broker:29092',
    'key.format' = 'raw',
    'properties.group.id' = 'testGroup',
    'value.format' = 'avro-confluent',
    'value.avro-confluent.url' = 'http://schema-registry:8081',
    'value.fields-include' = 'ALL'
);

-- Aggregate query/worker
-- Join 2 tables into avro_salescompleted_x - completed sales.
Insert into avro_salescompleted_x
    SELECT 
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
        avro_salespayments b LEFT JOIN
        avro_salesbaskets a
    ON b.INVOICENUMBER = a.INVOICENUMBER;
-- See https://lazypro.medium.com/flink-sql-performance-tuning-part-2-c102177b1ce1to optimize this query, above is version 1.
-- Here is a improved/less impactfull option.
-- this only brings into scope the data from either table that is less than 1 hour old.
Insert into avro_salescompleted_x
    SELECT 
        b.INVOICENUMBER,
        b.SALEDATETIME,
        b.SALETIMESTAMP,
        b.TERMINALPOINT,
        b.NETT,
        b.VAT,
        b.TOTAL,
        b.STORE,
        b.CLERK,
        b.BASKETITEMS,        
        a.FINTRANSACTIONID,
        a.PAYDATETIME,
        a.PAYTIMESTAMP,
        a.PAID
    FROM 
        avro_salespayments a,
        avro_salesbaskets b
    WHERE a.INVOICENUMBER = b.INVOICENUMBER
    AND CAST(a.PAYTIMESTAMP AS BIGINT) > CAST(b.SALETIMESTAMP AS BIGINT) 
    AND TO_TIMESTAMP(FROM_UNIXTIME(CAST(SALETIMESTAMP AS BIGINT) / 1000)) > (TO_TIMESTAMP(FROM_UNIXTIME(CAST(SALETIMESTAMP AS BIGINT) / 1000)) - INTERVAL '1' HOUR);

-- Improve further by using *_WM values that was pre casted to date/time fields n the 2 source tables.
Insert into avro_salescompleted_x
    SELECT 
        b.INVOICENUMBER,
        b.SALEDATETIME,
        b.SALETIMESTAMP,
        b.TERMINALPOINT,
        b.NETT,
        b.VAT,
        b.TOTAL,
        b.STORE,
        b.CLERK,
        b.BASKETITEMS,        
        a.FINTRANSACTIONID,
        a.PAYDATETIME,
        a.PAYTIMESTAMP,
        a.PAID
    FROM 
        avro_salespayments a,
        avro_salesbaskets b
    WHERE a.INVOICENUMBER = b.INVOICENUMBER
    AND PAYTIMESTAMP_WM > SALESTIMESTAMP_WM 
    AND SALESTIMESTAMP_WM > (SALESTIMESTAMP_WM - INTERVAL '1' HOUR);


-- Create sales per store per terminal per hour output table


-- Calculate sales per store per terminal per hour


-- Create sales per store per terminal per 5 min output table - dev purposes


-- Calculate sales per store per terminal per 5 min - dev purposes





-- OLD
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
    WATERMARK FOR SALESTIMESTAMP_WM AS SALESTIMESTAMP_WM,
    PRIMARY KEY (INVOICENUMBER) NOT ENFORCED
) WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'avro_salescompleted_x',
        'properties.bootstrap.servers' = 'broker:29092',
        'key.format' = 'raw',
        'properties.group.id' = 'testGroup',
        'value.format' = 'avro-confluent',
        'value.avro-confluent.url' = 'http://schema-registry:8081',
        'value.fields-include' = 'ALL'
);

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