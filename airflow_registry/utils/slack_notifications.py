import os
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator


def base_failure_alert(context, conn):
    ti = context.get("task_instance")
    environment: str | None = os.getenv("ENVIRONMENT")
    show_env = os.getenv("LOG_ENV_ON_SLACK_ALERTS") is not None

    _task = ti.task_id
    _environment = (
        f"Environment: *{environment.upper()}* - " if show_env and environment else "\b"
    )
    _emoji = ":alarm:"
    _message = "Unexpected error"
    if "alice" in _task:
        _emoji = ":taco:"
        _message = (
            "Alice currently only running once a day, you can probably ignore this"
        )
    elif "sensor" in _task:
        _emoji = ":warning:"
        _message = "Possible data delay"
    slack_msg = """
    {emoji} {environment} {task} failed in *{dag}* {emoji}
    *Execution Time*: {exec_date}
    {message}, investigate at {log_url}
    """.format(
        dag=ti.dag_id,
        task=_task,
        emoji=_emoji,
        message=_message,
        environment=_environment,
        exec_date=context.get("execution_date"),
        log_url=ti.log_url,
    )

    failed_alert = SlackWebhookOperator(
        task_id="slack_failure_alert", slack_webhook_conn_id=conn, message=slack_msg
    )
    return failed_alert.execute(context=context)


def failure_alert(context):
    return base_failure_alert(context, "slack_failure_alert")


def content_failure_alert(context):
    return base_failure_alert(context, "slack_dev_data_quality_airflow_alerts")


def custom_failure_alert(context, conn, xcom_key):
    ti = context.get("task_instance")
    _task = ti.task_id
    message = ti.xcom_pull(task_ids=_task, key=xcom_key)
    _emoji = ":x:"
    slack_msg = """
    {emoji} Task Failed {emoji}
    *Task:* {task}
    *Dag:* {dag}
    *Execution Time:* {exec_date}
    <{log_url}|*Logs*>
    {message}
    """.format(
        dag=ti.dag_id,
        task=_task,
        emoji=_emoji,
        message=message,
        exec_date=context.get("execution_date"),
        log_url=ti.log_url,
    )

    failed_alert = SlackWebhookOperator(
        task_id="slack_failure_alert", slack_webhook_conn_id=conn, message=slack_msg
    )
    return failed_alert.execute(context=context)


def dqc_failure_alert(context, channel, conn, xcom_key):
    ti = context.get("task_instance")
    _task = ti.task_id
    message = ti.xcom_pull(task_ids=_task, key=xcom_key)
    _emoji = ":x:"
    slack_msg = """
    {emoji} Checks Failed {emoji}
    *Execution Time:* {exec_date}
    <{log_url}|*Logs*>
    {message}
    """.format(
        emoji=_emoji,
        message=message,
        exec_date=context.get("execution_date"),
        log_url=ti.log_url,
    )

    failed_alert = SlackAPIPostOperator(
        task_id="slack_message",
        slack_conn_id=conn,
        channel=channel,
        text=slack_msg,
        username="",
    )
    return failed_alert.execute(context=context)
