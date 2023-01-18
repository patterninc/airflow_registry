# AIRFLOW_REGISTRY
Pattern's personal airflow registry for custom operators, sensors, and hooks


How To Use In Your DAGS:

Typically, you can use the following statement to install a private github repo as a dependency in your requirements.txt file for your astro project:

git+https://${GITHUB_TOKEN}@github.com/patterninc/airflow_registry.git@main #Pattern's airflow registry package

However, the way the astro images are set up, you cannot read in environment variables to the requirements.txt file. So, there are a few ways to approach it:

1. You can build a custom image with a mounted github ssh token. This is the way Astronomer recommends and has documentation for: https://docs.astronomer.io/astro/develop-project?tab=github#install-python-packages-from-private-sources

2. You can manually install the package when testing locally (put token in requirements.txt file before running `astro dev start` or manually install using pip inside webserver docker container) and then when you're ready to deploy to staging, use github actions to insert the token in the requirements.txt file after the repo is checked out. You can find an example of this here: https://github.com/patterninc/insights-airflow/blob/dev/.github/workflows/dev-ci-cd.yml. 

Whatever you do, DO NOT commit your github token. 


Once the package is installed, an import statement would look like this:

from airflow_registry.operators.s3ToPostgresOperator import S3ToPostgresOperator
