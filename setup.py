#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
      name='airflow_registry',
      version='0.1.0',
      description='Custom operators, sensors, and hooks to be shared across org',
      author='Pattern',
      license='MIT License',
      python_requires='>=3.7.0',      
      packages=find_packages(),
      package_dir={'': '.'},
      install_requires=['apache-airflow-providers-slack>=7.1.0','soda-core-snowflake']
)

