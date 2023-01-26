# AIRFLOW_REGISTRY
Pattern's personal airflow registry for custom operators, sensors, and hooks.


**How To Use In Your Airflow Project**

1. Add ```git-all``` to your packages.txt file.

2. Add ```git+https://github.com/patterninc/airflow_registry@main``` to your requirements.txt file



Once the package is installed, an import statement would look like this:

from airflow_registry.operators.s3ToPostgresOperator import S3ToPostgresOperator
