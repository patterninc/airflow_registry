# helper function to run data quality checks and avoid rewriting code for all checks :
from soda.sampler.sampler import Sampler
from soda.sampler.sample_context import SampleContext
import json

class CustomSampler(Sampler):
    failed_rows = []
    def store_sample(self, sample_context: SampleContext):
      rows = sample_context.sample.get_rows()
      table_name = sample_context.partition.table.table_name
      scan_name = sample_context.scan._scan_definition_name
      check_name = sample_context.check_name
      for row in rows:
        self.failed_rows.append({
          "scan_name": scan_name,
          "table_name": table_name,
          "check_name": check_name,
          "row": row
        })

def check(scan_name, checks_subpath=None, config_file=None, data_source='snowflake', project_root='/usr/local/airflow/include/soda', snowflake_table=None, context=None):
  from soda.scan import Scan
  from snowflake.connector import connect
  import datetime

  """
  Run Soda Scan to perform data quality checks.

  Args:
    scan_name: The name of the scan.
    checks_subpath: The subpath of the checks directory.
    data_source: The data source name.
    project_root: The project root directory.
    config_file: Variable containing Soda configuration string.
    snowflake_table: The name of the Snowflake table to store the results.

  Returns:
    int: The result of the scan execution.
    
  Raises:
    ValueError: If the Soda Scan fails.
  """
  print('Running Soda Scan ...')

  checks_path = f'{project_root}'

  if checks_subpath:
    checks_path += f'/{checks_subpath}'

  scan = Scan()
  scan.set_verbose()
  scan.sampler = CustomSampler()
  scan.add_configuration_yaml_str(config_file)
  scan.set_data_source_name(data_source)
  scan.add_sodacl_yaml_files(f'{project_root}/')

  scan_name += datetime.datetime.now().strftime("_%Y%m%d%H%M%S")
  scan.set_scan_definition_name(scan_name)

  exit_code = scan.execute()
  data = scan.get_scan_results()
  config = json.loads(config_file)
  slack_message = ''

  if snowflake_table:
    # Extract the connection details
    user = config[f'data_source {data_source}']['username']
    password = config[f'data_source {data_source}']['password']
    account = config[f'data_source {data_source}']['account']
    warehouse = config[f'data_source {data_source}']['warehouse']
    role = config[f'data_source {data_source}']['role']

    with connect(user=user, password=password, account=account, role=role, warehouse=warehouse) as conn:

      scan_start = datetime.datetime.fromisoformat(data.get('scanStartTimestamp'))

      for check in data.get('checks'):
          name = str(check.get('name'))
          definition = check.get('definition').replace("'", "''").strip() 
          table_name = check.get('table')
          outcome = check.get('outcome')
          diagnostics = json.dumps(check.get('diagnostics'))
          fail_objects = [row['row'][-1] for row in scan.sampler.failed_rows if row['check_name'] == name] # Fail objects need to be the last column in the row
          file_path = check.get('location').get('filePath')
          load_timestamp = datetime.datetime.now().replace(tzinfo=None)
          sql = f"INSERT INTO {snowflake_table} (NAME, DEFINITION, TABLE_NAME, OUTCOME, DIAGNOSTICS, SCAN_START, SCAN_NAME, FILE_PATH, FAIL_MESSAGES, LOAD_TIMESTAMP) SELECT '{name}', '{definition}', '{table_name}', '{outcome}', PARSE_JSON('{diagnostics}'), TO_TIMESTAMP_NTZ('{scan_start}'), '{scan_name}', '{file_path}', {fail_objects}, TO_TIMESTAMP_NTZ('{load_timestamp}');"
          try:
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
          except Exception as e:
            slack_message = f"An error occurred while executing the SQL command: {e}"
            context['ti'].xcom_push(key='slack_fail_message', value=slack_message)
          finally:
            cursor.close()
  
  if exit_code != 0:
    slack_message += f'*Scan Name:* {scan_name}\n'
    if scan.has_error_logs():
      summary = scan.get_logs_text().split('Oops!')[1].split('Summary:')[0].strip()
      slack_message += f'*Results:* {summary}\n\n*Scan Error Logs* \n\n {scan.get_error_logs_text()}\n\n\n'
    if scan.has_check_fails():
      slack_message += f'*Failed Data Quality Checks:*\n\n'
      for failed_check in scan.get_checks_fail():
        slack_message += f"Table Name: {failed_check.partition.table.table_name}\n Check Name: {failed_check.name}\n Failed Rows: {failed_check.get_log_diagnostic_dict()['value']} \n\n"
    context['ti'].xcom_push(key='slack_fail_message', value=slack_message)
    raise ValueError("Soda Scan failed.")
  