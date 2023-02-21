import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
'owner': 'airflow',
'start_date':datetime(2020, 1, 1),
}

dag_spark = DAG(
dag_id = "dprj_dag",
default_args=default_args,
schedule_interval="0 0 * * *",
)

users_calc = SparkSubmitOperator(
task_id='users_calc',
dag=dag_spark,
application ='/scripts/st2_event_city_mapping.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-25", "/user/nutslyc/data/geo", "/user/nutslyc/analytics"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

zones_calc = SparkSubmitOperator(
task_id='zones_calc',
dag=dag_spark,
application ='/scripts/st3_zones_data_mart.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-25", "/user/nutslyc/data/geo", "/user/nutslyc/analytics"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)

recomendations_calc = SparkSubmitOperator(
task_id='recomendations_calc',
dag=dag_spark,
application ='/scripts/st4_recomendations.py' ,
conn_id= 'yarn_spark',
application_args = ["2022-05-25", "/user/nutslyc/data/geo", "/user/nutslyc/analytics"],
conf={
"spark.driver.maxResultSize": "20g"
},
executor_cores = 1,
executor_memory = '1g'
)


users_calc >> zones_calc >> recomendations_calc