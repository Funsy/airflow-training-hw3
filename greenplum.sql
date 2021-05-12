-- создаем STG слой
create external table ashanin.stg_payment (user_id int, 
      pay_doc_type text, 
      pay_doc_num int, 
      account text, 
      phone text, 
      billing_period date, 
      pay_date date, 
      sum numeric(10,2))
  location ('pxf://rt-2021-03-25-16-47-29-sfunu-ashanin/data_lake/stg/payment/*/?PROFILE=gs:parquet') 
  FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

-- создаем VIEW
CREATE VIEW "rtk_de"."ashanin"."ods_v_payment" AS (
	SELECT
		user_id,
		pay_doc_type,
		pay_doc_num,
		account,
		phone,
		billing_period,
		pay_date,
		sum,
		user_id::TEXT AS USER_KEY,
		account::TEXT AS ACCOUNT_KEY,
		billing_period::TEXT AS BILLING_PERIOD_KEY,
		'PAYMENT - DATA LAKE'::TEXT AS RECORD_SOURCE,

		CAST((MD5(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''))) AS TEXT) AS USER_PK,
		CAST((MD5(NULLIF(UPPER(TRIM(CAST(account AS VARCHAR))), ''))) AS TEXT) AS ACCOUNT_PK,
		CAST((MD5(NULLIF(UPPER(TRIM(CAST(billing_period AS VARCHAR))), ''))) AS TEXT) AS BILLING_PERIOD_PK,
		CAST(MD5(NULLIF(CONCAT_WS('||',
			COALESCE(NULLIF(UPPER(TRIM(CAST(user_id AS VARCHAR))), ''), '^^'),
			COALESCE(NULLIF(UPPER(TRIM(CAST(account AS VARCHAR))), ''), '^^'),
			COALESCE(NULLIF(UPPER(TRIM(CAST(billing_period AS VARCHAR))), ''), '^^')
		), '^^||^^||^^')) AS TEXT) AS PAY_PK,
		CAST(MD5(CONCAT_WS('||',
			COALESCE(NULLIF(UPPER(TRIM(CAST(phone AS VARCHAR))), ''), '^^')
		)) AS TEXT) AS USER_HASHDIFF,
	    current_date as LOAD_DATE,
	    pay_date AS EFFECTIVE_FROM

	FROM "rtk_de"."ashanin"."ods_payment");
      
