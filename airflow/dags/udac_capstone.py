from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator    
from helpers import SqlQueries
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'ronnald',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup' : False
}

dag = DAG('capstone',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@monthly')

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_capstone_events = StageToRedshiftOperator(
    task_id='staging_capstone_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id="aws_credentials",
    table="staging_capstone_events",
    s3_source="s3://rrm86capstone/input/events/",
    json_paths="auto",
    file_type="CSV"
)

stage_capstone_states = StageToRedshiftOperator(
    task_id='staging_capstone_states',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_capstone_states",
    s3_source="s3://rrm86capstone/input/states/states2.json",
    json_paths="auto",
    file_type="JSON"
)

stage_capstone_region = StageToRedshiftOperator(
    task_id='staging_capstone_region',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_capstone_region",
    s3_source="s3://rrm86capstone/input/region/region.json",
    json_paths="auto",
    file_type="JSON"
)

load_event_table = LoadFactOperator(
    task_id='load_event_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="capstone_events",
    query=SqlQueries.event_table_insert
)

load_states_table = LoadDimensionOperator(
    task_id='load_states_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='capstone_states',
    query=SqlQueries.states_table_insert,
    mode='delete-load'
)

load_region_table = LoadDimensionOperator(
    task_id='load_region_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='capstone_region',
    query=SqlQueries.region_table_insert,
    mode='delete-load'
)

load_time_table = LoadDimensionOperator(
    task_id='load_time_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='capstone_time',
    query=SqlQueries.time_table_insert,
    mode='delete-load'
)


run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    data_quality = [{"table":"capstone_events", "field":None,"result":0, "type":"count"},
                   {"table":"capstone_region", "field":"region","result":0, "type":"count-where"},
                   {"table":"capstone_states", "field":"state","result":0, "type":"count-where"}]
)


end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


start_operator >> stage_capstone_events 
start_operator >> stage_capstone_states
start_operator >> stage_capstone_region
stage_capstone_events >> load_event_table
stage_capstone_states >> load_event_table
stage_capstone_region >> load_event_table
load_event_table >> load_states_table
load_event_table >> load_region_table
load_event_table >> load_time_table
load_states_table >> run_quality_checks
load_region_table >> run_quality_checks
load_time_table >> run_quality_checks
run_quality_checks >> end_operator
