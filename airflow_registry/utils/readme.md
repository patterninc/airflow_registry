**HOW TO SET UP SLACK NOTIFICATIONS FOR YOUR REPOSITORY**
1. Follow instructions in top level readme.md to import this repository
2. Create a slack app https://api.slack.com/docs/slack-button#register_your_slack_app
3. (Optional) create a channel in slack where you want alerts/notifications to appear
4. Create a new webhook and select a channel. (one app can have many webhooks)
5. To use the webhook in airflow, you can either add it to Parameter Store if you have a custom secrets backend set up, or simply add the connection in Airflow.
connection id: conn_id_here
connection type: http
host: https://hooks.slack.com/services/
Password: /T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX
6. Use the connection id in your code 


**HOW TO ADD FAILURE_ALERT TO A DAG**
1. At the top of your dag, put 'from airflow_registroy.utils import slack_notifications as sn
2. in your DAG() parameters in default_args(), add 'on_failure_callback': sn.failure_alert
3. Follow the instructions above to create a webhook pointed at a slack channel, and have your connection id = 'failure_alert'
