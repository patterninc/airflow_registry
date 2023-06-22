from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def base_failure_alert(context, conn):
    ti = context.get('task_instance')
    _task = ti.task_id
    _emoji = ':warning:' if "sensor" in _task else ':alarm:'
    _message = 'Possible data delay' if "sensor" in _task else 'Unexpected error'
    slack_msg = """
    {emoji} {task} failed in {dag} {emoji}
    *Execution Time*: {exec_date}
    {message}, investigate at {log_url}
    """.format(
        dag=ti.dag_id,
        task=_task,
        emoji=_emoji,
        message=_message,
        exec_date=context.get('execution_date'),
        log_url=ti.log_url)

    failed_alert = SlackWebhookOperator(
        task_id='slack_failure_alert',
        http_conn_id=conn,
        message=slack_msg)
    return failed_alert.execute(context=context)

def failure_alert(context):
    return base_failure_alert(context, 'slack_failure_alert')

def content_failure_alert(context):
    return base_failure_alert(context, 'slack_dev_data_quality_airflow_alerts')
