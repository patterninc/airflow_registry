#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
      name='airflow_registry',
      version='0.1.0',
      description='Custom operators, sensors, and hooks to be shared across org',
      author='Pattern',
      license='',
      python_requires='>=3.7.0',
      packages=['airflow_registry','airflow_registry.hooks','airflow_registry.sensors','airflow_registry.operators','airflow_registry.utils'],
)