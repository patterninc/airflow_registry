# helper function to run data quality checks and avoid rewriting code for all checks :
def check(scan_name, checks_subpath=None, config_file=None, data_source='snowflake', project_root='/usr/local/airflow/include/soda'):
  from soda.scan import Scan
  from snowflake.connector import connect
  import json
  import datetime
  """
  Run Soda Scan to perform data quality checks.

  Args:
    scan_name: The name of the scan.
    checks_subpath: The subpath of the checks directory.
    data_source: The data source name.
    project_root: The project root directory.
    config_file: Variable containing Soda configuration string.

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
  scan.add_configuration_yaml_str(config_file)
  scan.set_data_source_name(data_source)
  scan.add_sodacl_yaml_files(f'{project_root}/')
  scan.set_scan_definition_name(scan_name)

  result = scan.execute()
  print(scan.get_logs_text())
  data = scan.get_scan_results()
  print(data)

  config = json.loads(config_file)

  # Extract the connection details
  user = config[f'data_source {data_source}']['username']
  password = config[f'data_source {data_source}']['password']
  account = config[f'data_source {data_source}']['account']
  warehouse = config[f'data_source {data_source}']['warehouse']

  with connect(user=user, password=password, account=account, warehouse=warehouse) as conn:

    scan_start = datetime.datetime.fromisoformat(data.get('scanStartTimestamp'))

    for check in data.get('checks'):
        name = str(check.get('name'))
        definition = check.get('definition').replace("'", "''").strip() 
        table_name = check.get('table').split('.')[-1] 
        outcome = check.get('outcome')
        diagnostics = json.dumps(check.get('diagnostics'))

        file_path = check.get('location').get('filePath')
        load_timestamp = datetime.datetime.now().replace(tzinfo=None)
        db = check.get('table').split('.')[0]
        schema = check.get('table').split('.')[1]
        sql = f"INSERT INTO {db}.{schema}.data_quality_tests (NAME, DEFINITION, TABLE_NAME, OUTCOME, DIAGNOSTICS, SCAN_START, FILE_PATH, LOAD_TIMESTAMP) SELECT '{name}', '{definition}', '{table_name}', '{outcome}', PARSE_JSON('{diagnostics}'), TO_TIMESTAMP_NTZ('{scan_start}'), '{file_path}', TO_TIMESTAMP_NTZ('{load_timestamp}');"
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
