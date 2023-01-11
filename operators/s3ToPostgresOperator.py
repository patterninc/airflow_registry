from __future__ import annotations

from typing import TYPE_CHECKING, Iterable, Sequence

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from plugins.hooks.postgresCustomHook import PostgresCustomHook
from plugins.hooks.s3CustomHook import S3CustomHook

import os
import logging

if TYPE_CHECKING:
    from airflow.utils.context import Context


class S3ToPostgresOperator(BaseOperator):
    """
    Uses the aws_s3 extension to copy files from s3 to Postgres
    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:S3ToPostgresOperator`
    :param schema: reference to a specific schema in postgres database
    :param table: reference to a specific table in postgres database
    :param s3_bucket: reference to a specific S3 bucket
    :param s3_prefix: reference to the s3 bucket prefix
    :param s3_key: reference to a specific S3 key
    :param postgres_conn_id: reference to a specific postgres database
    :param aws_conn_id: reference to a specific S3 connection (this implementation uses env variables)
    :param verify: Whether or not to verify SSL certificates for S3 connection.
        By default SSL certificates are verified.
        You can provide the following values:
        - ``False``: do not validate SSL certificates. SSL will still be used
                 (unless use_ssl is False), but SSL certificates will not be
                 verified.
        - ``path/to/cert/bundle.pem``: A filename of the CA cert bundle to uses.
                 You can specify this argument if you want to use a different
                 CA cert bundle than the one used by botocore.
    :param column_list: list of column names to load
    :param copy_options: reference to a list of COPY options
    :param indices_path: path to json index file
    """

    template_fields: Sequence[str] = ("table","table_schema_file")
    template_ext: Sequence[str] = (".sql")
    #template_fields_renderers = {"sql": "sql"}

    ui_color = "#99e699"

    def __init__(
        self,
        *,
        table: str,
        s3_bucket: str,
        s3_prefix: str,
        postgres_conn_id: str = "postgres_default",
        aws_conn_id: str = "aws_default",
        column_list: str | None = None,
        autocommit: bool = False,
        num_threads: int = 5,
        indices_file: str | None=None,
        table_schema_file: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.postgres_conn_id = postgres_conn_id
        self.aws_conn_id = aws_conn_id
        self.column_list = column_list
        self.autocommit = autocommit
        self.num_threads = num_threads
        self.indices_file = indices_file
        self.table_schema_file = table_schema_file

        # if self.method not in AVAILABLE_METHODS:
        #     raise AirflowException(f"Method not found! Available methods: {AVAILABLE_METHODS}")


    def execute(self, context: Context):
        #instantiate hooks
        postgres_hook = PostgresCustomHook(postgres_conn_id=self.postgres_conn_id)
        s3_hook = S3CustomHook(aws_conn_id=self.aws_conn_id)

        postgres_hook.drop_and_create_table(self.table_schema_file)

        key_array = s3_hook.copy_object(self.s3_bucket, self.s3_prefix) # update metadata and get list of keys
    
        postgres_hook.import_s3_files(key_array, self.table, self.s3_bucket, self.num_threads, self.column_list) #import files with aws_s3 pg extension

        if self.indices_file:
            postgres_hook.apply_indexes(self.indices_file,self.table) # Create indexes on table


        return "S3 Data loaded successfully into Postgres"






