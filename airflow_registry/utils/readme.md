**HOW TO SET UP SLACK NOTIFICATIONS FOR YOUR REPOSITORY**
***
1. Create a slack app https://api.slack.com/docs/slack-button#register_your_slack_app
2. (Optional) create a channel in slack where you want alerts/notifications to appear
3. Create a new webhook and select the channel that the webhook will point to. (one app can have many webhooks)
4. To use the webhook in airflow, you can either add it to Parameter Store if you have a custom secrets backend set up, or simply add the connection in Airflow.

connection id: conn_id_here

connection type: http

host: https://hooks.slack.com/services/

Password: /T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX

5. Use the connection id in your code 


**HOW TO ADD FAILURE_ALERT TO A DAG**
***
1. Follow instructions in top level readme.md to import this repository
2. At the top of your dag, put 'from airflow_registroy.utils import slack_notifications as sn
3. in your DAG() parameters in default_args(), add 'on_failure_callback': sn.failure_alert
4. Follow the instructions above to create a webhook pointed at a slack channel, and have your connection id = 'slack_failure_alert'

**EXAMPLE DAG**
```
from datetime import datetime
from airflow_registry.utils import slack_notifications as sn
from airflow import DAG

with DAG(
    dag_id="failure_dag",
    start_date=datetime(2022, 6, 20),
    default_args={
        'on_failure_callback': sn.failure_alert
    },
    schedule_interval='0 6 * * *',
) as dag:
```
