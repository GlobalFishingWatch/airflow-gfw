# airflow-gfw

It is a package providing an [Airflow](https://airflow.apache.org/) extension for GFW pipeline.

##Description

The `airflow-gfw` module provides the Airflow components properly build for GFW purposes.
Extends the Operators such as [BigQueryOperator](https://github.com/apache/airflow/blob/1.10.2/airflow/contrib/operators/bigquery_operator.py), [DataflowOperator](https://github.com/apache/airflow/blob/1.10.2/airflow/contrib/operators/dataflow_operator.py), [PythonOperator](https://airflow.apache.org/howto/operator.html#pythonoperator), etc.
It defines the environment, adjusts the default arguments and handles a configuration dictionary and a factory of DagRun to use it in different pipelines.

###Usage

Includes in your project as
```
pip install https://codeload.github.com/GlobalFishingWatch/airflow-gfw/tar.gz/<version>
```

You can find the registry of changes in the `CHANGES.md` file.

Every time you want to make a change in this repo, please run the test or generate the proper ones.

To Run unit tests use the following
  `docker-compose run tests`
