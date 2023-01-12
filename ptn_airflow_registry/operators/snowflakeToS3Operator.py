from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import os
import logging

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SnowflakeToS3Operator(BaseOperator):
    """
    Uses a Snowflake stage to send a table's files to S3
    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SnowflakeToS3Operator`
    :param schema: reference to a specific schema in snowflake database
    :param table: reference to a specific table in snowflake database
    :param stage: reference to a specific snowflake stage
    :param file_format: reference to a specific snowflake file format
    :param max_file_size: max file size to be exported to s3
    :param single: single parameter (true or false)
    :param s3_prefix: reference to the s3 bucket prefix
    :param snowflake_conn_id: reference to a specific postgres database
    :param aws_conn_id: reference to a specific S3 connection (this implementation uses env variables)
    """

    template_fields: Sequence[str] = (
        "sf_schema",
        "sf_table",
        "snowflake_conn_id"

    )
    #template_ext: Sequence[str] = (".sql")
    #template_fields_renderers = {"sql": "sql"}

    ui_color = "#99e699"

    def __init__(
        self,
        *,
        sf_schema: str,
        sf_table: str,
        sf_db: str,
        s3_prefix: str,
        sf_stage: str,
        sf_file_format: str,
        max_file_size: int = 5368709120, 
        single: bool = False,
        header: bool = True,
        snowflake_conn_id: str = "snowflake_default",
        aws_conn_id: str = "aws_default",
        autocommit: bool = False,

        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sf_schema = sf_schema
        self.sf_table = sf_table
        self.sf_db = sf_db
        self.s3_prefix = s3_prefix
        self.sf_stage = sf_stage
        self.sf_file_format = sf_file_format
        self.max_file_size = max_file_size
        self.single = single
        self.header = header
        self.snowflake_conn_id = snowflake_conn_id
        self.aws_conn_id = aws_conn_id
        self.autocommit = autocommit

        # if self.method not in AVAILABLE_METHODS:
        #     raise AirflowException(f"Method not found! Available methods: {AVAILABLE_METHODS}")

    def _form_sql_copy_string(self) -> str:
        sql = f"COPY INTO @{self.sf_db}.{self.sf_schema}.{self.sf_stage}/{self.s3_prefix}\
                FROM  {self.sf_db}.{self.sf_schema}.{self.sf_table}\
                FILE_FORMAT = (FORMAT_NAME = '{self.sf_db}.{self.sf_schema}.{self.sf_file_format}')\
                SINGLE = {self.single}\
                MAX_FILE_SIZE = {self.max_file_size}\
                HEADER = {self.header};"
        return sql

    def _form_sql_rm_string(self) -> str:
        sql = f"REMOVE @{self.sf_db}.{self.sf_schema}.{self.sf_stage}/{self.s3_prefix};"
        return sql




    def execute(self, context: Context):
        
        #instantiate hooks
        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)
        try:
            snowflake_hook.run(self._form_sql_rm_string())
            snowflake_hook.run(self._form_sql_copy_string())
        except Exception as e:
            raise AirflowException(f"Issue removing or loading data to s3 using stage: {e}")

        return "Snowflake Data loaded successfully into S3"







