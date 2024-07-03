


-- Working with time/dates and timestamps in ksqldb
-- https://www.confluent.io/blog/ksqldb-2-0-introduces-date-and-time-data-types/

-- salesbaskets
CREATE STREAM pb_salesbaskets (
	   	InvoiceNumber VARCHAR,
	 	SaleDateTime VARCHAR,
	 	SaleTimestamp VARCHAR,
	  	TerminalPoint VARCHAR,
	   	Nett DOUBLE,
	  	Vat DOUBLE,
	 	Total DOUBLE,
       	Store STRUCT<
       		Id VARCHAR,
     		Name VARCHAR>,
     	Clerk STRUCT<
     		Id VARCHAR,
          	Name VARCHAR>,
    	BasketItems ARRAY< STRUCT<
			id VARCHAR,
        	Name VARCHAR,
          	Brand VARCHAR,
          	Category VARCHAR,
         	Price DOUBLE,
        	Quantity integer >>) 
WITH (KAFKA_TOPIC='pb_salesbaskets',
		    VALUE_FORMAT='ProtoBuf',
        	PARTITIONS=1);
       
CREATE STREAM pb_salesbaskets1 WITH (KAFKA_TOPIC='pb_salesbaskets1',
       VALUE_FORMAT='ProtoBuf',
       PARTITIONS=1)
       as  
		select
			InvoiceNumber,
	 		SaleDateTime,
		  	CAST(SaleTimestamp AS BIGINT) AS Sale_epoc_bigint,
	  		TerminalPoint,
	   		Nett,
	  		Vat,
	 		Total,
       		Store,
     		Clerk,
    		BasketItems 
		from pb_salesbaskets
			emit changes;

CREATE STREAM pb_salesbaskets2 WITH (KAFKA_TOPIC='pb_salesbaskets2',
       VALUE_FORMAT='ProtoBuf',
       PARTITIONS=1)
       as  
		select
			InvoiceNumber,
	 		SaleDateTime,
			TIMESTAMPTOSTRING(CAST(SaleTimestamp AS BIGINT), 'yyyy-MM-dd''T''HH:mm:ss.SSS') AS SaleTimestamp_str,
	  		TerminalPoint,
	   		Nett,
	  		Vat,
	 		Total,
       		Store,
     		Clerk,
    		BasketItems 
		from pb_salesbaskets
			emit changes;


-- salespayments       
CREATE STREAM pb_salespayments (
	      	InvoiceNumber VARCHAR,
	      	FinTransactionId VARCHAR,
	      	PayDateTime VARCHAR,
			PayTimestamp VARCHAR,
	      	Paid DOUBLE      )
WITH (KAFKA_TOPIC='pb_salespayments',
       		VALUE_FORMAT='ProtoBuf',
       		PARTITIONS=1);

CREATE STREAM pb_salespayments1 WITH (KAFKA_TOPIC='pb_salespayments1',
       VALUE_FORMAT='ProtoBuf',
       PARTITIONS=1)
       as  
		select   	
			InvoiceNumber,
	      	FinTransactionId,
	      	PayDateTime,
		  	CAST(PayTimestamp AS BIGINT) AS Pay_epoc_bigint,
	      	Paid  
		from pb_salespayments
			emit changes;


CREATE STREAM pb_salespayments2 WITH (KAFKA_TOPIC='pb_salespayments2',
       VALUE_FORMAT='ProtoBuf',
       PARTITIONS=1)
       as  
		select   	
			InvoiceNumber,
	      	FinTransactionId,
	      	PayDateTime,
		  	TIMESTAMPTOSTRING(CAST(PayTimestamp AS BIGINT), 'yyyy-MM-dd''T''HH:mm:ss.SSS') AS PayTimestamp_str,
	      	Paid  
		from pb_salespayments
			emit changes;



CREATE STREAM pb_salescompleted WITH (KAFKA_TOPIC='pb_salescompleted',
       VALUE_FORMAT='ProtoBuf',
       PARTITIONS=1)
       as  
select 
	b.InvoiceNumber InvNumber, 
	b.SaleDateTime,
	b.SaleTimestamp, 
	b.TerminalPoint,
	b.Nett,
	b.Vat,
	b.Total,
	b.store,
	b.clerk,
	b.BasketItems,
	p.FinTransactionId,
	p.PayDateTime,
	p.PayTimestamp,
	p.Paid
from 
	pb_salespayments p INNER JOIN
	pb_salesbaskets b
WITHIN 7 DAYS 
on b.InvoiceNumber = p.InvoiceNumber
emit changes;

CREATE STREAM json_salescompleted WITH (KAFKA_TOPIC='json_salescompleted',
       VALUE_FORMAT='Json',
       PARTITIONS=1)
       as  
select 
	b.InvoiceNumber InvNumber, 
	b.SaleDateTime,
	b.SaleTimestamp, 
	b.TerminalPoint,
	b.Nett,
	b.Vat,
	b.Total,
	b.store,
	b.clerk,
	b.BasketItems,
	p.FinTransactionId,
	p.PayDateTime,
	p.PayTimestamp,
	p.Paid
from 
	pb_salespayments p INNER JOIN
	pb_salesbaskets b
WITHIN 7 DAYS 
on b.InvoiceNumber = p.InvoiceNumber
emit changes;


------------------------------------------------------------------------------

CREATE TABLE json_sales_per_store WITH (KAFKA_TOPIC='json_sales_per_store',
       VALUE_FORMAT='Json',
       PARTITIONS=1)
       as  
SELECT  
	store->id as store_id,
	count(1) as sales_per_store
FROM pb_salescompleted
WINDOW TUMBLING (SIZE 5 MINUTE)
GROUP BY store->id 
EMIT FINAL;

-- limited to one store for POC, output in AVRO format
CREATE TABLE avro_sales_per_terminal_point WITH (KAFKA_TOPIC='avro_sales_per_terminal_point',
       FORMAT='AVRO',
       PARTITIONS=1)
       as  
SELECT 
	store->id as store_id,
	TerminalPoint as terminal_point,
    count(1) as sales_per_terminal
FROM pb_salescompleted
WINDOW TUMBLING (SIZE 5 MINUTE)
WHERE store->Id = '324213441'
GROUP BY store->id , TerminalPoint	
EMIT FINAL;

-- limited to one store for POC, output in JSON format
CREATE TABLE json_sales_per_terminal_point WITH (KAFKA_TOPIC='json_sales_per_terminal_point',
       FORMAT='JSON',
       PARTITIONS=1)
       as  
SELECT 
	store->id as store_id,
	TerminalPoint as terminal_point,
    count(1) as sales_per_terminal
FROM pb_salescompleted
WINDOW TUMBLING (SIZE 5 MINUTE)
WHERE store->Id = '324213441'
GROUP BY store->id , TerminalPoint	
EMIT FINAL;

-- this updates the totals incrementally as it grows, the above emit final only shows/updates at the close of the windows
select 
  store_id, 
  terminal_point,
  from_unixtime(WINDOWSTART) as Window_Start,
  from_unixtime(WINDOWEND) as Window_End,
  SALES_PER_TERMINAL
from AVRO_SALES_PER_TERMINAL_POINT EMIT CHANGES;