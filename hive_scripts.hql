-- создание STG таблиц
CREATE EXTERNAL TABLE ashanin.stg_billing (user_id INT, billing_period STRING, service STRING, tariff STRING, sum STRING, created_at STRING) 
	STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-ashanin/data_lake/stg/billing';

CREATE EXTERNAL TABLE ashanin.stg_issue (user_id STRING, start_time STRING, end_time STRING, title STRING, description STRING, service STRING) 
	STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-ashanin/data_lake/stg/issue';

CREATE EXTERNAL TABLE ashanin.stg_payment (user_id INT, pay_doc_type STRING, pay_doc_num INT, account STRING, phone STRING, billing_period STRING, pay_date STRING, sum DOUBLE) 
	STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-ashanin/data_lake/stg/payment';

CREATE EXTERNAL TABLE ashanin.stg_traffic (user_id INT, `timestamp` BIGINT, device_id STRING, device_ip_addr STRING, bytes_sent INT, bytes_received INT) 
	STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-ashanin/data_lake/stg/traffic';


-- создание ODS таблиц
CREATE EXTERNAL TABLE ashanin.ods_billing (user_id INT, billing_period DATE, service STRING, tariff STRING, sum INT, created_at DATE) 
	PARTITIONED BY (year STRING) 
	STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-ashanin/data_lake/ods/billing';

CREATE EXTERNAL TABLE ashanin.ods_issue (user_id INT, start_time TIMESTAMP, end_time TIMESTAMP, title STRING, description STRING, service STRING) 
	PARTITIONED BY (year STRING) 
	STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-ashanin/data_lake/ods/issue';

CREATE EXTERNAL TABLE ashanin.ods_payment (user_id INT, pay_doc_type STRING, pay_doc_num INT, account STRING, phone STRING, billing_period DATE, pay_date DATE, sum DECIMAL(10,2)) 
	PARTITIONED BY (year STRING) 
	STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-ashanin/data_lake/ods/payment';

CREATE EXTERNAL TABLE ashanin.ods_traffic (user_id INT, `timestamp` TIMESTAMP, device_id STRING, device_ip_addr STRING, bytes_sent INT, bytes_received INT) 
	PARTITIONED BY (year STRING) 
	STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-ashanin/data_lake/ods/traffic';


-- запись в ODS
INSERT OVERWRITE TABLE ashanin.ods_billing 
	PARTITION (year='2020') 
	SELECT user_id, cast(concat(billing_period, '-01') as DATE), service, tariff, cast(sum as INT), cast(created_at as DATE) 
	FROM ashanin.stg_billing 
	WHERE year(created_at) = '2020';

INSERT OVERWRITE TABLE ashanin.ods_issue 
	PARTITION (year='2020') 
	SELECT cast(user_id as INT), cast(start_time as TIMESTAMP), cast(end_time as TIMESTAMP), title, description, service 
	FROM ashanin.stg_issue 
	WHERE year(start_time) = '2020';

INSERT OVERWRITE TABLE ashanin.ods_payment 
	PARTITION (year='2020') 
	SELECT user_id, pay_doc_type, pay_doc_num, account, phone, cast(concat(billing_period, '-01') as DATE), cast(pay_date as DATE), cast(sum as DECIMAL(10,2)) 
	FROM ashanin.stg_payment 
	WHERE year(pay_date) = '2020';

INSERT OVERWRITE TABLE ashanin.ods_traffic 
	PARTITION (year='2020') 
	SELECT user_id, cast(`timestamp` as TIMESTAMP), device_id, device_ip_addr, bytes_sent, bytes_received 
	FROM ashanin.stg_traffic 
	WHERE year(from_unixtime(cast(`timestamp`/1000 as BIGINT))) = '2020';


-- создание DM таблицы
CREATE EXTERNAL TABLE ashanin.dm_bytes_received (user_id INT, max_traffic INT, min_traffic INT, avg_traffic INT) 
	PARTITIONED BY (year STRING) 
	STORED AS PARQUET LOCATION 'gs://rt-2021-03-25-16-47-29-sfunu-ashanin/data_lake/dm/bytes_received';


-- запись в DM таблицу
INSERT OVERWRITE TABLE ashanin.dm_bytes_received 
	PARTITION (year='2020') 
	SELECT user_id, max(bytes_received), min(bytes_received), cast(avg(bytes_received) as INT) 
	FROM ashanin.ods_traffic 
	WHERE `year` = '2020' 
	GROUP BY user_id;
