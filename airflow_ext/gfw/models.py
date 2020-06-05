from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
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
        return FlexibleOperator(params).build_operator(self.flexible_operator)

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

    def source_table_parts(self, tables, date=None):
        assert tables

        for table in tables.split(','):
            yield dict(
                project=self.config['project_id'],
                dataset=self.config['source_dataset'],
                table=table,
                date=date
            )

    def source_table_paths(self, date=None):
        return [
            self.format_bigquery_table(**parts) for parts in 
            self.source_table_parts(self.config.get('source_tables') or self.config.get('source_table'), date=date)
        ]

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
            for parts in
            self.source_table_parts(
                self.config.get('source_tables') or
                self.config.get('source_table'),
                date=self.source_sensor_date_nodash()
            )
        ]

    def gcs_sensor(self, dag, bucket, prefix, date, retries, timeout):
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
        :param retries: The amount of retries before fails.
        :type retries: int
        :param timeout: The timeout to sense the GCS.
        :type timeout: int
        """
        return GoogleCloudStoragePrefixSensor(
            dag=dag,
            task_id='source_exists_{}'.format(bucket),
            bucket=bucket,
            prefix='{}/{}'.format(prefix, date),
            mode='reschedule',                                # the sensor task frees the worker slot
                                                              # when the criteria is not yet met
                                                              # and it's rescheduled at a later time.
            poke_interval=10 * 60,                            # check every 10 minutes.
            timeout=60 * 60 * 24 if not timeout else timeout, # timeout of 24 hours.
            retry_exponential_backoff=False,                  # disable progressive longer waits
            retries=0 if not retries else retries             # amount of retries and lets fail the task
        )

    def source_gcs_path(self, **sensor_args):
        """
        Returns a generator with bucket, prefix and date if the paths matched the GCS protocol, None in other way.

        :param sensor_args: A dict that defines the sensor paramters in GCS to be checked.
        :type sensor_args: dict
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
                    date=self.source_date_range()[1] if not sensor_args.get('date') else sensor_args.get('date'),
                    retries=sensor_args.get('retries'),
                    timeout=sensor_args.get('timeout')
                )
        gcs

    def source_gcs_sensors(self, dag, **sensor_args):
        """
        Returns an array of GCS sensors operators related with the DAG and an specific date if it is defined.

        Iterates over the dict generated of GCS PATHS to define the operator to check its existance.

        :param dag: The DAG which will be associated with the sensor.
        :type dag: DAG from Airflow.
        :param sensor_args: A dict that defines the sensor paramters in GCS to be checked.
        :type sensor_args: dict
        """
        return [ self.gcs_sensor(dag=dag, **parts) for parts in self.source_gcs_path(**sensor_args) ]

    def table_check(self, task_id, project, dataset, table, date, **retries_config=None):
        """
        Returns a BigQueryCheckOperator that checks the existance of a partitioned table for a specific date.
        Having a retry mechanism of 3 days checking by 30 minutes.
        :param task_id: The task identification.
        :type task_id: str
        :param project: The project where the dataset and table belongs.
        :type project: str
        :param dataset: The dataset that has the table.
        :type dataset: str
        :param table: The partitioned table.
        :type table: str
        :param date: The date of the partition to be checked.
        :type date: str
        :param retries_config: The retries configuration to adapt to your
        needs. You can customize parameters as retries, execution_timeout,
        retry_delay and max_retry_delay.
        :type retries_config: dict
        """
        retries = retries_config.get('retries')
        execution_timeout = retries_config.get('execution_timeout')
        retry_delay = retry_delay_config.get('retry_delay')
        max_retry_delay = max_retry_delay_config.get('max_retry_delay')
        return BigQueryCheckOperator(
            task_id=task_id,
            project_id=project,
            dataset_id=dataset,
            sql='SELECT COUNT(*) FROM [{}.{}${}]'.format(dataset_id, table_id, date),
            retries=2*24*3 if not retries else retries,                                          # Retries 3 days with 30 minutes.
            execution_timeout=timedelta(days=3) if not execution_timeout else execution_timeout, # TimeOut of 3 days.
            retry_delay=timedelta(minutes=30) if not retry_dely else retry_delay,                # Delay in retries 30 minutes.
            max_retry_delay=timedelta(minutes=30) if not max_retry_delay else max_retry_delay,   # Max Delay in retries 30 minutes
            on_failure_callback=config_tools.failure_callback_gfw
        )

    def tables_checker(self, **tables_check_args):
        """
        Returns an array of BigQueryCheckOperators of partitioned tables to validate existance.
        :param tables_check_args: The dict that has the details to configure the check operator.
        :type tables_check_args: dict
        """
        return [
            self.table_check(task_id='check_existance_{}'.format(parts['table']), **parts)
            for parts in
            self.source_table_parts(
                self.config.get('check_tables') or
                self.config.get('check_table'),
                date=self.source_sensor_date_nodash()
            )
        ]

    def build(self, dag_id):
        raise NotImplementedError
