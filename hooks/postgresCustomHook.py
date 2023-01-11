from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowFailException, AirflowException
from multiprocessing.pool import ThreadPool as Pool
import logging
import os
import concurrent.futures
import json
import sqlparse
import psycopg2




class PostgresCustomHook(PostgresHook):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def _build_import_query_array(self, key_array: object, table: str, s3_bucket: str, column_list: str) -> str:
        sql_array = []
        logging.info(f'{column_list}')
        for key in key_array:
            sql_array.append( 
                f"SELECT aws_s3.table_import_from_s3('{table}',\
                '{column_list}',\
                'CSV HEADER DELIMITER ''|'' QUOTE ''\"'' NULL ''NULL''',\
                '{s3_bucket}',\
                '{key}',\
                '{os.getenv('AWS_DEFAULT_REGION')}',\
                '{os.getenv('AWS_ACCESS_KEY_ID')}',\
                '{os.getenv('AWS_SECRET_ACCESS_KEY')}');")

        return sql_array
    
    def _build_index_defs(self, index_file: str, table: str):
        index_array = []
        with open(f'/usr/local/airflow/include/json/{index_file}', 'r') as f:
            index_obj = json.load(f)
        for key, value in index_obj.items():
            index_array.append(f'create index if not exists index_{table}_{key} on {table} using btree ({value})')
        return index_array


    def drop_and_create_table(self, table_schema_file):
        try:
            self.run(table_schema_file)
        except Exception as e:
            raise AirflowException(f"Error opening sql file: {e}")


    def import_s3_files(self, key_array, table, s3_bucket, num_threads, column_list):

        sql_array = self._build_import_query_array(key_array, table, s3_bucket, column_list)
        
        def execute_query(sql):
            success = False
            max_retries = 3
            retries = 0
            while not success and retries < max_retries:
                try:
                    self.run(sql)
                    success = True
                    return f"SQL successfully executed -> {sql}"
                except psycopg2.OperationalError as e:
                    logging.error(f"The sql statement {sql} has failed {retries+1} times.")
                    retries += 1
                except Exception as e:
                    raise AirflowException(f"Error: {e}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Use the submit() method to submit a list of requests to the pool
            results = [executor.submit(execute_query, sql) for sql in sql_array]
            
            # Print results
            for result in concurrent.futures.as_completed(results):
                print(f"{result.result()}")


    def apply_indexes(self, indices_path, table):
        index_array = self._build_index_defs(indices_path, table)
        logging.info(f'Index Array: {index_array}')
        parallel_config = "SET max_parallel_maintenance_workers = 4; SET maintenance_work_mem = '4GB';"
        for index in index_array:
            try:
                self.run(parallel_config + " " + index)
                logging.info(f'Building index with statement: {parallel_config + index}')
            except Exception as e:
                raise AirflowException(f"Error creating index: {e}")
        self.run(f'analyze {table}') #analyze after indexes are created

        
    def execute_custom_query(self, sql):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        return rows

