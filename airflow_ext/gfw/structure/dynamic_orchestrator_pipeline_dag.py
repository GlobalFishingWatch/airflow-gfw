import posixpath as pp
import imp
from datetime import datetime, timedelta

from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow_ext.gfw.models import DagFactory


def get_dag_path(pipeline, module=None):
    if module is None:
        module = pipeline
    config = Variable.get(pipeline, deserialize_json=True)
    return pp.join(config['dag_install_path'], '{}_dag.py'.format(module))


pipe_segment = imp.load_source('pipe_segment', get_dag_path('pipe_segment'))
pipe_measures = imp.load_source('pipe_measures', get_dag_path('pipe_measures'))
pipe_anchorages = imp.load_source('pipe_anchorages', get_dag_path('pipe_anchorages'))
pipe_encounters = imp.load_source('pipe_encounters', get_dag_path('pipe_encounters'))
pipe_features = imp.load_source('pipe_features', get_dag_path('pipe_features'))
pipe_events = imp.load_source('pipe_events', get_dag_path('pipe_events'))

ORCHESTRATOR_KEY='country_orchestrator'

class CountryPipelineDagFactory(DagFactory):
    def __init__(self, **kwargs):
        super(CountryPipelineDagFactory, self).__init__(**kwargs)

    def isEnabled(self):
        'Validates the configuration'
        config=self.config
        orchestrator=config[ORCHESTRATOR_KEY]
        try:
            assert orchestrator is not None
            assert len(orchestrator)>0
            self.orchestrator=Variable.get(ORCHESTRATOR_KEY, deserialize_json=True)
            return True
        except:
            logging.warn("To enable the Orchestrator, the Airflow Variable {ORCHESTRATOR_KEY} must exist and has a valid JSON type")
            return False

    def source_tables(self, country_config):
        tables = country_config.get('normalized_table_name')
        assert tables
        return tables.split(',')

    def source_table_parts(self, country_config, date=None):
        tables = self.source_tables(country_config)

        for table in tables:
            yield dict(
                project=self.config['project_id'],
                dataset=country_config['dataset'],
                table=table,
                date=date
            )

    def source_table_paths(self, country_config, date=None):
        return [self.format_bigquery_table(**parts) for parts in self.source_table_parts(country_config, date=date)]

    def build(self, dag_id):
        #Check if there is any configuration
        if not self.isEnabled(self):
            return False

        logging.info("Orchestrator: Were found {} country configurations.".format(len(self.orchestrator)))

        config=self.config
        default_args = self.default_args
        subdag_default_args = dict(
            start_date=default_args['start_date'],
            end_date=default_args['end_date']
        )

        dags=[]
        for country in self.orchestrator:
            name=country["name"]
            pipeline=country["pipeline"]
            dataset=country["dataset"]
            assert name
            assert pipeline
            assert dataset

            dag_id=("{}"+dag_id).format(pipeline)
            logging.info("Starts orchestrating country <{}> pipeline {}.".format(name))

            subdag_config = dict(
                pipeline_dataset=dataset,
                source_dataset=dataset,
                events_dataset=country["events_dataset"] if country["events_dataset"] else config['events_dataset'],
                dataflow_runner='{dataflow_runner}'.format(**config),
                temp_shards_per_day="3",
            )
            country['source_paths'] = ','.join(self.source_table_paths(country))
            country['source_dates'] = ','.join(self.source_date_range())


            def table_partition_check(dataset_id, table_id, date):
                return BigQueryCheckOperator(
                    task_id='table_partition_check',
                    dataset_id=dataset_id,
                    sql='SELECT COUNT(*) FROM [{}.{}${}]'.format(dataset_id, table_id, date),
                    retries=24*2*7,       # retry twice per hour for a week
                    retry_delay=timedelta(minutes=30),
                    retry_exponential_backoff=False
                )

            def callback_subdag_clear(context):
                """Clears a subdag's tasks on retry."""
                dag_id = "{}.{}".format(
                    context['dag'].dag_id,
                    context['ti'].task_id
                )
                execution_date = context['execution_date']
                sdag = DagBag().get_dag(dag_id)
                sdag.clear(
                    start_date=execution_date,
                    end_date=execution_date,
                    only_failed=False,
                    only_running=False,
                    confirm_prompt=False,
                    include_subdags=False)

            with DAG(dag_id, schedule_interval=self.schedule_interval, default_args=self.default_args) as dag:
                source_exists=table_partition_check(
                    dataset,
                    '{normalized_table_name}'.format(**country),
                    '{ds_nodash}'.format(**config))

                segment = SubDagOperator(
                    subdag=pipe_segment.build_dag(
                        dag_id='{}.segment'.format(dag_id),
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=dict(
                            pipeline_dataset=country['dataset'],
                            source_dataset=country['dataset'],
                            normalized_tables='{normalized_table_name}'.format(**country),
                            dataflow_runner='{dataflow_runner}'.format(**config),
                            temp_shards_per_day="3",
                        )
                    ),
                    trigger_rule=TriggerRule.ONE_SUCCESS,
                    depends_on_past=True,
                    task_id='segment',
                    on_retry_callback=callback_subdag_clear
                )

                measures = SubDagOperator(
                    subdag=pipe_measures.PipeMeasuresDagFactory(
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id='{}.measures'.format(dag_id)),
                    task_id='measures',
                    on_retry_callback=callback_subdag_clear
                )

                port_events = SubDagOperator(
                    subdag=pipe_anchorages.build_port_events_dag(
                        dag_id='{}.port_events'.format(dag_id),
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ),
                    task_id='port_events',
                    on_retry_callback=callback_subdag_clear
                )

                port_visits = SubDagOperator(
                    subdag=pipe_anchorages.build_port_visits_dag(
                        dag_id='{}.port_visits'.format(dag_id),
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ),
                    task_id='port_visits',
                    on_retry_callback=callback_subdag_clear
                )

                encounters = SubDagOperator(
                    subdag=pipe_encounters.build_dag(
                        dag_id='{}.encounters'.format(dag_id),
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ),
                    task_id='encounters',
                    on_retry_callback=callback_subdag_clear
                )

                features = SubDagOperator(
                    subdag=pipe_features.PipeFeaturesDagFactory(
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id='{}.features'.format(dag_id)),
                    depends_on_past=True,
                    task_id='features',
                    on_retry_callback=callback_subdag_clear
                )

                events = SubDagOperator(
                    subdag=pipe_events.CountryPipelineDagFactory(
                        schedule_interval=dag.schedule_interval,
                        extra_default_args=subdag_default_args,
                        extra_config=subdag_config
                    ).build(dag_id='{}.events'.format(dag_id)),
                    depends_on_past=True,
                    task_id='events',
                    on_retry_callback=callback_subdag_clear
                )

                dag >> source_exists >> fetch_normalized

                fetch_normalized >> segment >> measures

                measures >> port_events >> port_visits >> features
                measures >> encounters >> features
                features >> events

                dags.append(dag)

        return dags

pipe_vms_daily_dags = CountryPipelineDagFactory().build(dag_id='_daily')
pipe_vms_monthly_dags = CountryPipelineDagFactory(schedule_interval='@monthly').build(dag_id='_monthly')

