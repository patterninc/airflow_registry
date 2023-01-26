# AIRFLOW_REGISTRY
Pattern's personal airflow registry for custom operators, sensors, and hooks.


**How To Use In Your Airflow Project**

1. Add ```git-all``` to your packages.txt file.

2. Add ```git+https://github.com/patterninc/airflow_registry@main``` to your requirements.txt file



Once the package is installed, an import statement would look like this:
```
from airflow_registry.operators.s3ToPostgresOperator import S3ToPostgresOperator
from airflow_registry.utils import slack_notifications as sn
from datetime import datetime
from airflow import DAG

from grow_airflow_utils.environment import constants as const

with DAG(
    dag_id="example_dag",
    start_date=datetime(2022, 6, 20),
    default_args={
        'on_failure_callback': sn.failure_alert
    },
    schedule_interval='0 6 * * *',
) as dag:
```
