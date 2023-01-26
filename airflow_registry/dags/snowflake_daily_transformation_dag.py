from datetime import datetime

from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from airflow_registry.utils import slack_notifications as sn

def snowflake_daily_transformation_dag(sql_file_name, tags, snowflake_conn_id, template_searchpath, owner, optional_schedule_interval='0 6 * * *'):
  title = sql_file_name[:-4]
  with DAG(
      dag_id=title,
      start_date=datetime(2023, 1, 1),
      default_args={
          'snowflake_conn_id': snowflake_conn_id,
          'owner': owner,
          'on_failure_callback': sn.failure_alert
      },
      schedule_interval=optional_schedule_interval,
      max_active_runs=1,
      description= title + ' transformation',
      template_searchpath=template_searchpath,
      tags=tags,
      catchup=False
  ) as dag:
      transformation = SnowflakeOperator(
          task_id=title + '_transformation',
          sql=sql_file_name,
      )
