from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.models import Variable

from airflow_ext.gfw import config as config_tools
from airflow_ext.gfw.operators.helper.flexible_operator import FlexibleOperator
from airflow_ext.gfw.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor

from datetime import timedelta

import re


class DagFactory(object):
    def __init__(self, pipeline, schedule_interval='@daily', extra_default_args={}, extra_config={}, base_config={}):
        self.pipeline = pipeline

        loaded_config = config_tools.load_config(pipeline)
        self.config = base_config.copy()
        self.config.update(loaded_config)
        self.config.update(extra_config)

        self.default_args = config_tools.default_args(self.config)
        self.default_args.update(extra_default_args)

        self.schedule_interval = schedule_interval

        self.flexible_operator = Variable.get('FLEXIBLE_OPERATOR')

    def build_docker_task(self, params):
        """Creates a new airflow task which executes a docker container

        This method returns a new airflow operator instance which is configured
        to execute a docker container. It works similar to instantiating a
        `KubernetesPodOperator` and supports the same arguments, but internally
        decides on how to best run the container based on the environment. For
        example, when running inside Google Cloud Composer, or inside a
        `pipe-airflow` instance running in a kubernetes cluster, this method
        returns a `KubernetesPodOperator`, but when running in a local airflow
        instance it returns a `BashOperator` set up to run the docker container
        locally.

        This behavior is controlled by the `FLEXIBLE_OPERATOR` airflow
        variable, which may be either `bash` or `kubernetes`.
        """
        FlexibleOperator(params).build_operator(self.flexible_operator)

    def source_sensor_date_nodash(self):
        if self.schedule_interval == '@daily':
            return '{{ ds_nodash }}'
        elif self.schedule_interval == '@monthly':
            return '{last_day_of_month_nodash}'.format(**self.config)
        elif self.schedule_interval == '@yearly':
            return '{last_day_of_year_nodash}'.format(**self.config)
        else:
            raise ValueError('Unsupported schedule interval {}'.format(
                self.schedule_interval))

    def source_date_range(self):
        if self.schedule_interval == '@daily':
            return '{{ ds }}', '{{ ds }}'
        elif self.schedule_interval == '@monthly':
            return self.config['first_day_of_month'], self.config['last_day_of_month']
        elif self.schedule_interval == '@yearly':
            return self.config['first_day_of_year'], self.config['last_day_of_year']
        else:
            raise ValueError('Unsupported schedule interval {}'.format(
                self.schedule_interval))

    def format_date_sharded_table(self, table, date=None):
        return "{}{}".format(table, date or '')

    def format_bigquery_table(self, project, dataset, table, date=None):
        return "{}:{}.{}".format(project, dataset, self.format_date_sharded_table(table, date))

    def source_tables(self):
        tables = self.config.get(
            'source_tables') or self.config.get('source_table')
        assert tables
        return tables.split(',')

    def source_table_parts(self, date=None):
        tables = self.source_tables()

        for table in tables:
            yield dict(
                project=self.config['project_id'],
                dataset=self.config['source_dataset'],
                table=table,
                date=date
            )

    def source_table_paths(self, date=None):
        return [self.format_bigquery_table(**parts) for parts in self.source_table_parts(date=date)]

    def source_table_date_paths(self):
        return [self.format_bigquery_table(**parts)
                for parts in self.source_table_parts(date=self.source_sensor_date_nodash())]

    def table_sensor(self, dag, task_id, project, dataset, table, date=None):
        return BigQueryTableSensor(
            dag=dag,
            task_id=task_id,
            project_id=project,
            dataset_id=dataset,
            table_id=self.format_date_sharded_table(table, date),
            mode='reschedule',      # the sensor task frees the worker slot when the criteria is not yet met
                                    # and it's rescheduled at a later time.
            poke_interval=10 * 60,  # check every 10 minutes.
            timeout=60 * 60 * 24    # timeout of 24 hours.
        )

    def source_table_sensors(self, dag):
        return [
            self.table_sensor(dag=dag, task_id='source_exists_{}'.format(
                parts['table']), **parts)
            for parts in self.source_table_parts(date=self.source_sensor_date_nodash())
        ]

    def gcs_sensor(self, dag, bucket, prefix, date):
        """
        Returns the GoogleCloudStoragePreixSensor customized for the parameters.

        :param dag: The DAG which will be associated with the sensor.
        :type dag: DAG from Airflow.
        :param bucket: The bucket of GCS where to sensor.
        :type bucket: str
        :param prefix: The prefix after the bucket id of GCS where to sensor.
        :type prefix: str
        :param date: The date that defines the folder in GCS to be checked.
        :type date: str
        """
        return GoogleCloudStoragePrefixSensor(
            dag=dag,
            task_id='source_exists_{}'.format(bucket),
            bucket=bucket,
            prefix='{}/{}'.format(prefix, date),
            mode='reschedule',               # the sensor task frees the worker slot when the criteria is not yet met
                                             # and it's rescheduled at a later time.
            poke_interval=10 * 60,           # check every 10 minutes.
            timeout=60 * 60 * 24,            # timeout of 24 hours.
            retry_exponential_backoff=False, # disable progressive longer waits
            retries=0                        # no retries and lets fail the task
        )

    def source_gcs_path(self, date=None):
        """
        Returns a generator with bucket, prefix and date if the paths matched the GCS protocol, None in other way.

        :param date: The date that defines the folder in GCS to be checked.
        :type date: str
        """
        gcs_paths = self.config.get('source_gcs_paths') or self.config.get('source_gcs_path')
        assert gcs_paths
        paths = gcs_paths.split(',')
        gcs=None

        for path in paths:
            if (path.strip().startswith('gs://')):
                gcs = yield dict(
                    bucket=re.search('(?<=gs://)[^/]*', path).group(0),
                    prefix=re.search('(?<=gs://)[^/]*/(.*)', path).group(1),
                    date=self.source_date_range()[1] if not date else date
                )
        gcs

    def source_gcs_sensors(self, dag, date=None):
        """
        Returns an array of GCS sensors operators related with the DAG and an specific date if it is defined.

        Iterates over the dict generated of GCS PATHS to define the operator to check its existance.

        :param dag: The DAG which will be associated with the sensor.
        :type dag: DAG from Airflow.
        :param date: The date that defines the folder in GCS to be checked.
        :type date: str
        """
        return [ self.gcs_sensor(dag=dag, **parts) for parts in self.source_gcs_path(date) ]

    def build(self, dag_id):
        raise NotImplementedError
