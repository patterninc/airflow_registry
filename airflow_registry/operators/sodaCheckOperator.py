from airflow.models import BaseOperator
import importlib
import os

class SodaCheckOperator(BaseOperator):
    def __init__(self, scan_name, config_file, check_function, data_source, project_root, checks_subpath, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.scan_name = scan_name
        self.config_file = config_file
        self.check_function = check_function
        self.data_source = data_source
        self.project_root = project_root
        self.checks_subpath = checks_subpath

    def execute(self, context):
        return self.check_function(scan_name=self.scan_name, config_file=self.config_file, data_source=self.data_source, project_root=self.project_root, checks_subpath=self.checks_subpath)
