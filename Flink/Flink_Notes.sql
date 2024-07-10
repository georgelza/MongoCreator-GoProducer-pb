
-- Flink UI : http://localhost:9081/#/overview

-- https://aiven.io/blog/preview-JSON-SQL-functions-apache-flink-1.15.0

-- Example

{
    "id": "message-1",
    "title": "Some Title",
    "properties": {
        "foo": "bar",
        "nested_foo":{
            "prop1" : "value1",
            "prop2" : "value2"
        }
    }
}

CREATE TABLE input(
                id VARCHAR,
                title VARCHAR,
                properties ROW(`foo` VARCHAR, `nested_foo` ROW(`prop1` VARCHAR, `prop2` VARCHAR))
        ) WITH (
            'connector' = 'kafka-0.11',
            'topic' = 'my-topic',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'python-test',
            'format' = 'json'
        );

SELECT properties.foo, properties.nested_foo.prop1 FROM input;

SELECT properties FROM input


SELECT * 
FROM sensors 
WHERE JSON_EXISTS(payload, '$.data');


SELECT DISTINCT JSON_VALUE(payload, '$.location') AS `city`
FROM sensors 
WHERE JSON_EXISTS(payload, '$.data');


SELECT 
  AVG(JSON_VALUE(payload, '$.data.value' RETURNING INTEGER)) AS `avg_temperature`, 
  JSON_VALUE(payload, '$.location') AS `city` 
FROM sensors 
WHERE JSON_VALUE(payload, '$.data.metric') = 'Temperature' 
GROUP BY JSON_VALUE(payload, '$.location');


SELECT 
  JSON_VALUE(payload, '$.location') as loc, 
  JSON_VALUE(payload, '$.data.metric') as metric,
  TO_TIMESTAMP(JSON_VALUE(payload, '$.timestamp')) as timestamp_value,
  MAX(JSON_VALUE(payload, '$.data.value')) as max_value
FROM sensors 
WHERE JSON_EXISTS(payload, '$.data')
  AND JSON_EXISTS(payload, '$.location')
GROUP BY 
  JSON_VALUE(payload, '$.data.metric'),
  JSON_VALUE(payload, '$.location'), 
  TO_TIMESTAMP(JSON_VALUE(payload, '$.timestamp'));




WITH sensors_with_max_metric AS (
  SELECT 
    JSON_VALUE(payload, '$.location') AS loc,
    JSON_VALUE(payload, '$.data.metric') AS metric,
    TO_TIMESTAMP(JSON_VALUE(payload, '$.timestamp')) AS timestamp_value,
    MAX(JSON_VALUE(payload, '$.data.value')) AS max_value
  FROM sensors
  WHERE JSON_EXISTS(payload, '$.data')
    AND JSON_EXISTS(payload, '$.location')
  GROUP BY 
    JSON_VALUE(payload, '$.data.metric'),
    JSON_VALUE(payload, '$.location'),
    TO_TIMESTAMP(JSON_VALUE(payload, '$.timestamp'))
)
SELECT 
  timestamp_value, 
  metric, 
  JSON_OBJECTAGG(KEY loc VALUE max_value) AS json_object_value
FROM sensors_with_max_metric
GROUP BY timestamp_value, metric;


SELECT 
  window_start, 
  window_end, 
  user_id, 
  sum(total_price) as total_total
FROM TABLE ( TUMBLE orders, DESCRIPTION(order_time), INTERVAL `1` MINUTES))
GROUP BY user_id, window_start, window_end


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
        avro_salespayments_x b LEFT JOIN
        avro_salesbaskets_x a
    ON b.INVOICENUMBER = a.INVOICENUMBER;

-- See https://lazypro.medium.com/flink-sql-performance-tuning-part-2-c102177b1ce1 to optimize this query, above is version 1.
-- See https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/joins/ Interval joins, to reduce data scope kept.
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


-- convert epoc string to timestamp
SELECT TO_TIMESTAMP(FROM_UNIXTIME(CAST(SALETIMESTAMP AS BIGINT) / 1000)) AS timestamp_value from avro_salescompleted;


- enabling Hive : https://www.decodable.co/blog/catalogs-in-flink-sql-hands-on
docker exec -it flink-jobmanager /bin/bash