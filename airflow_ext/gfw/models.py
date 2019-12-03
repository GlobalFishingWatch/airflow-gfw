from airflow.contrib.sensors.bigquery_sensor import BigQueryTableSensor
from airflow_ext.gfw import config as config_tools
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
        return GoogleCloudStoragePrefixSensor(
            dag=dag,
            task_id='source_exists_{}'.format(bucket),
            bucket=bucket,
            prefix='{}/{}'.format(prefix, date),
            mode='reschedule',      # the sensor task frees the worker slot when the criteria is not yet met
                                    # and it's rescheduled at a later time.
            poke_interval=10 * 60,  # check every 10 minutes.
            timeout=60 * 60 * 24    # timeout of 24 hours.
        )

    def source_path(self, date=None):
        gcs_paths = self.config.get('source_paths') or self.config.get('source_path')
        assert gcs_paths
        paths = gcs_paths.split(',')
        gcs=None
        tables=None

        for path in paths:
            if (path.strip().startswith('gs://')):
                gcs = yield dict(
                    bucket=re.search('(?<=gs://)[^/]*', path).group(0),
                    prefix=re.search('(?<=gs://)[^/]*/(.*)', path).group(1),
                    date=self.source_date_range()[1] if not date else date
                )
            else:
                tables = yield dict(
                    project=self.config['project_id'],
                    dataset=self.config['source_dataset'],
                    table=path,
                    date=self.source_sensor_date_nodash()
                )
        [gcs, tables]

    def source_sensors(self, dag, date=None):
        return [
            self.gcs_sensor(dag=dag, **parts) if ('bucket' in parts and 'prefix' in parts)
            else self.table_sensor(
                    dag=dag,
                    task_id='source_exists_{}'.format( parts['table']),
                    **parts
            )
            for parts in self.source_path(date)
        ]

    def build(self, dag_id):
        raise NotImplementedError
