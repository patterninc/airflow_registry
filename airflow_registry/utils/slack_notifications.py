from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


def base_failure_alert(context, conn):
    ti = context.get('task_instance')
    _task = ti.task_id
    _emoji = ':alarm:'
    _message = 'Unexpected error <@U037VV8LWFP>'
    if "alice" in _task:
        _emoji = ':taco:'
        _message = 'Alice currently only running once a day, you can probably ignore this'
    elif "sensor" in _task:
        _emoji = ':warning:'
        _message = 'Possible data delay <@U037VV8LWFP>'
    slack_msg = """
    {emoji} {task} failed in *{dag}* {emoji}
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
        slack_webhook_conn_id=conn,
        message=slack_msg)
    return failed_alert.execute(context=context)


def failure_alert(context):
    return base_failure_alert(context, 'slack_failure_alert')


def content_failure_alert(context):
    return base_failure_alert(context, 'slack_dev_data_quality_airflow_alerts')


def custom_failure_alert(context, conn, xcom_key):
    ti = context.get('task_instance')
    _task = ti.task_id
    message = ti.xcom_pull(task_ids=_task, key=xcom_key)
    _emoji = ':x:'
    slack_msg = """
    {emoji} Task Failed {emoji}
    *Task:* {task}
    *Dag:* {dag}
    *Execution Time:* {exec_date}
    {message}
    <{log_url}|*Logs*>
    """.format(
        dag=ti.dag_id,
        task=_task,
        emoji=_emoji,
        message=message,
        exec_date=context.get('execution_date'),
        log_url=ti.log_url)

    failed_alert = SlackWebhookOperator(
        task_id='slack_failure_alert',
        slack_webhook_conn_id=conn,
        message=slack_msg)
    return failed_alert.execute(context=context)
