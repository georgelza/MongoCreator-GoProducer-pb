
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


-- convert epoc string to timestamp
SELECT TO_TIMESTAMP(FROM_UNIXTIME(CAST(SALETIMESTAMP AS BIGINT) / 1000)) AS timestamp_value from avro_salescompleted;
