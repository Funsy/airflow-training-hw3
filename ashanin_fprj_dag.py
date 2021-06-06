from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'alevanov'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2012, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl',
    default_args = default_args,
    description = 'Data WareHouse ETL tasks',
    schedule_interval = "0 0 1 1 *",
)

del_ods = PostgresOperator(
    task_id="del_ods",
    dag=dag,
    sql="""
        DELETE FROM rtk_de.alevanov.ods_payment WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }};
        """
)

ins_ods = PostgresOperator(
    task_id="ins_ods",
    dag=dag,
    sql="""
        INSERT INTO rtk_de.alevanov.ods_payment (SELECT * FROM rtk_de.alevanov.stg_payment WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }});
        """
)

del_ods >> ins_ods

reloading_ods = DummyOperator(task_id="reloading_ods", dag=dag)

ins_ods >> reloading_ods

ins_hub_user = PostgresOperator(
    task_id="ins_hub_user",
    dag=dag,
    sql="""
        INSERT INTO rtk_de.alevanov.hub_user (user_pk, user_key, load_date, record_source)
    (SELECT user_pk, user_key, load_date, record_source
        FROM rtk_de.alevanov.view_hub_user_etl);
    """
)

ins_hub_account = PostgresOperator(
    task_id="ins_hub_account",
    dag=dag,
    sql="""
        INSERT INTO rtk_de.alevanov.hub_account (ACCOUNT_PK, account_key, load_date, record_source)
    (SELECT ACCOUNT_PK, account_key, load_date, record_source
        FROM rtk_de.alevanov.view_hub_account_etl);
    """
)

ins_hub_payment = PostgresOperator(
    task_id="ins_hub_payment",
    dag=dag,
    sql="""
        INSERT INTO rtk_de.alevanov.hub_payment (pay_pk, pay_doc_type_id, pay_doc_num, load_date, record_source)
    (SELECT pay_pk, pay_doc_type_id, pay_doc_num, load_date, record_source
        FROM rtk_de.alevanov.view_hub_payment_etl);
    """
)

ins_hub_billing_period = PostgresOperator(
    task_id="ins_hub_billing_period",
    dag=dag,
    sql="""
        INSERT INTO rtk_de.alevanov.hub_billing_period (billing_period_pk, billing_period_key, load_date, record_source)
    (SELECT billing_period_pk, billing_period_key, load_date, record_source
        FROM rtk_de.alevanov.view_hub_billing_period_etl);
    """
)

reloading_ods >> ins_hub_user
reloading_ods >> ins_hub_account
reloading_ods >> ins_hub_payment
reloading_ods >> ins_hub_billing_period

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

ins_hub_user >> all_hubs_loaded
ins_hub_account >> all_hubs_loaded
ins_hub_payment >> all_hubs_loaded
ins_hub_billing_period >> all_hubs_loaded

ins_t_link_payment = PostgresOperator(
    task_id="ins_t_link_payment",
    dag=dag,
    sql="""
        INSERT INTO rtk_de.alevanov.t_link_payment (PAY_PK, USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, sum, pay_doc_type, pay_doc_num, effective_from, load_date, record_source)
    (SELECT PAY_PK, USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, sum, pay_doc_type, pay_doc_num, effective_from, load_date, record_source
        FROM rtk_de.alevanov.view_t_link_payment);
    """
)

all_hubs_loaded >> ins_t_link_payment

all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

ins_t_link_payment >> all_links_loaded

ins_sat_user_details = PostgresOperator(
    task_id="ins_sat_user_details",
    dag=dag,
    sql="""
        INSERT INTO rtk_de.alevanov.sat_user_details (USER_PK, USER_HASHDIFF, phone, effective_from, load_date, record_source)
    (SELECT USER_PK, USER_HASHDIFF, phone, effective_from, load_date, record_source
        FROM rtk_de.alevanov.view_sat_user_details_etl);
    """
)

ins_sat_payment_doc_type = PostgresOperator(
    task_id="ins_sat_payment_doc_type",
    dag=dag,
    sql="""
        INSERT INTO rtk_de.alevanov.sat_payment_doc_type (pk_id, pay_doc_type)
    (SELECT pk_id, pay_doc_type FROM rtk_de.alevanov.view_sat_payment_doc_type_etl);
    """
)

ins_sat_user_as_client = PostgresOperator(
    task_id="ins_sat_user_as_client",
    dag=dag,
    sql="""
        INSERT INTO rtk_de.alevanov.sat_user_as_client (USER_PK, legal_type, district, registered_at, billing_mode, is_vip, load_date, record_source)
    (SELECT USER_PK, legal_type, district, registered_at, billing_mode, is_vip, load_date, record_source
        FROM rtk_de.alevanov.view_sat_user_as_client_etl);
    """
)



all_links_loaded >> ins_sat_user_details
all_links_loaded >> ins_sat_payment_doc_type
all_links_loaded >> ins_sat_user_as_client

all_sats_loaded = DummyOperator(task_id="all_sats_loaded", dag=dag)

ins_sat_user_details >> all_sats_loaded
ins_sat_payment_doc_type >> all_sats_loaded
ins_sat_user_as_client >> all_sats_loaded

DataMart = DummyOperator(task_id="data_mart", dag=dag)
all_sats_loaded >> DataMart


create_temp_table = PostgresOperator(
    task_id="create_temp_table",
    dag=dag,
    sql="""
    create table alevanov.payment_report_temp_{{ execution_date.year }} as (
with raw_data as (
	select 
		legal_type,
		district,
		extract(year from registered_at) as registration_year,
		is_vip,
		extract(year from to_date(billing_period_key, 'YYYY-MM')) as billing_year,
		billing_period_key, 
		sum as billing_sum
			
		from alevanov.t_link_payment tlp 
			join alevanov.hub_billing_period hbp
			on tlp.billing_period_pk = hbp.billing_period_pk
			
			join alevanov.hub_user hu 
			on hu.user_pk = tlp.user_pk
			
			left join alevanov.sat_user_as_client suac on suac.user_pk = hu.user_pk --- вместо mdm."user" - sat_user_as_client
			)
select billing_year, legal_type, district, registration_year, is_vip, sum(billing_sum)
	from raw_data
where billing_year = {{ execution_date.year }}
group by billing_year, legal_type, district, registration_year, is_vip
order by billing_year, legal_type, district, registration_year, is_vip
);
"""
)

alter_temp_table = PostgresOperator(
    task_id="alter_temp_table",
    dag=dag,
    sql="""
    ALTER TABLE alevanov.payment_report_temp_{{ execution_date.year }} OWNER TO alevanov;
	"""
)

DataMart >> create_temp_table >> alter_temp_table

load_dim_dm = DummyOperator(task_id="load_dim_dm", dag=dag)
alter_temp_table >> load_dim_dm

ins_dim_billing_year = PostgresOperator(
    task_id="ins_dim_billing_year",
    dag=dag,
    sql="""
 	insert into alevanov.payment_report_dim_billing_year (billing_year_key)
	with distinct_temp as (
		select distinct billing_year from alevanov.payment_report_temp_{{ execution_date.year }}
	), distinct_tbl as (
		select distinct billing_year_key from alevanov.payment_report_dim_billing_year
	), new_values as (
		select billing_year, billing_year_key from distinct_temp
		left join distinct_tbl
		on billing_year_key = billing_year
		where billing_year_key is null
	)
	select billing_year from new_values;
        """
)

ins_dim_legal_type = PostgresOperator(
    task_id="ins_dim_legal_type",
    dag=dag,
    sql="""
        insert into alevanov.payment_report_dim_legal_type (legal_type_key)
	with distinct_temp as (
		select distinct legal_type from alevanov.payment_report_temp_{{ execution_date.year }}
	), distinct_tbl as (
		select distinct legal_type_key from alevanov.payment_report_dim_legal_type
	), new_values as (
		select legal_type, legal_type_key from distinct_temp
		left join distinct_tbl
		on legal_type_key = legal_type
		where legal_type_key is null
	)
	select legal_type from new_values;
        """
)

ins_dim_district = PostgresOperator(
    task_id="ins_dim_district",
    dag=dag,
    sql="""
	insert into alevanov.payment_report_dim_district (district_key)
	with distinct_temp as (
		select distinct district from alevanov.payment_report_temp_{{ execution_date.year }}
	), distinct_tbl as (
		select distinct district_key from alevanov.payment_report_dim_district
	), new_values as (
		select district, district_key from distinct_temp
		left join distinct_tbl
		on district_key = district
		where district_key is null
	)
	select district from new_values;
        """
)

ins_dim_registration_year = PostgresOperator(
    task_id="ins_dim_registration_year",
    dag=dag,
    sql="""
    	insert into alevanov.payment_report_dim_registration_year (registration_year_key)
	with distinct_temp as (
		select distinct registration_year from alevanov.payment_report_temp_{{ execution_date.year }}
	), distinct_tbl as (
		select distinct registration_year_key from alevanov.payment_report_dim_registration_year
	), new_values as (
		select registration_year, registration_year_key from distinct_temp
		left join distinct_tbl
		on registration_year_key = registration_year
		where registration_year_key is null
	)
	select registration_year from new_values;
        """
)


load_dim_dm >> ins_dim_billing_year
load_dim_dm >> ins_dim_legal_type
load_dim_dm >> ins_dim_district
load_dim_dm >> ins_dim_registration_year

all_dim_dm_loaded = DummyOperator(task_id="all_dim_dm_loaded", dag=dag)

ins_dim_billing_year >> all_dim_dm_loaded
ins_dim_legal_type >> all_dim_dm_loaded
ins_dim_district >> all_dim_dm_loaded
ins_dim_registration_year >> all_dim_dm_loaded

load_fact_dm = DummyOperator(task_id="load_fact_dm", dag=dag)

all_dim_dm_loaded >> load_fact_dm


ins_report_fact = PostgresOperator(
    task_id="ins_report_fact",
    dag=dag,
    sql="""
        insert into alevanov.payment_report_fact(billing_year_id, legal_type_id, district_id, registration_year_id, is_vip ,sum)
	        select dby.id , dlt.id, dd.id, dry.id, raw.is_vip, raw.sum
		        from alevanov.payment_report_temp_{{ execution_date.year }} raw 
			        join alevanov.payment_report_dim_billing_year dby on dby.billing_year_key = raw.billing_year
			        join alevanov.payment_report_dim_district dd on dd.district_key = raw.district
			        join alevanov.payment_report_dim_legal_type dlt on dlt.legal_type_key = raw.legal_type
			        join alevanov.payment_report_dim_registration_year dry on dry.registration_year_key = raw.registration_year;
        """
)



load_fact_dm >> ins_report_fact

all_fact_dm_loaded = DummyOperator(task_id="all_fact_dm_loaded", dag=dag)

ins_report_fact >> all_fact_dm_loaded


drop_temp_table = PostgresOperator(
    task_id="drop_temp_table",
    dag=dag,
    sql="""
    drop table if exists alevanov.payment_report_temp_{{ execution_date.year }};
    """
)

all_fact_dm_loaded >> drop_temp_table
