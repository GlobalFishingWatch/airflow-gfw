from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.hooks.gcp_dataflow_hook import DataFlowHook, _Dataflow, _DataflowJob
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator, GoogleCloudBucketHelper
from airflow.models.pool import Pool
from airflow.utils.decorators import apply_defaults

import re


### Fixes the issue on getting the DF job_id
### https://cloud.google.com/dataflow/docs/reference/rest/v1b3/projects.locations.jobs/get
### To know the status of the job after updating to Beam 2.28.0
### https://globalfishingwatch.atlassian.net/browse/PIPELINE-84?focusedCommentId=12131
class _GFWDataflow(_Dataflow):
    @staticmethod
    def _extract_job(line):
        job_id_pattern = re.compile(
            br".*console.cloud.google.com/dataflow.*/jobs/.*/([a-z|0-9|A-Z|\-|\_]+).*")
        matched_job = job_id_pattern.search(line or '')
        if matched_job:
            return matched_job.group(1).decode()


class GFWDataFlowHook(DataFlowHook):
    @GoogleCloudBaseHook._Decorators.provide_gcp_credential_file
    def _start_dataflow(self, variables, name, command_prefix, label_formatter):
        variables = self._set_variables(variables)
        cmd = command_prefix + self._build_cmd(variables, label_formatter)
        job_id = _GFWDataflow(cmd).wait_for_done()
        _DataflowJob(self.get_conn(), variables['project'], name,
                     variables['region'],
                     self.poll_sleep, job_id,
                     self.num_retries).wait_for_done()


class GFWDataFlowPythonOperator(DataFlowPythonOperator):
    def execute(self, context):
        bucket_helper = GoogleCloudBucketHelper(
            self.gcp_conn_id, self.delegate_to)
        self.py_file = bucket_helper.google_cloud_to_local(self.py_file)
        hook = GFWDataFlowHook(gcp_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to,
                            poll_sleep=self.poll_sleep)
        dataflow_options = self.dataflow_default_options.copy()
        dataflow_options.update(self.options)
        camel_to_snake = lambda name: re.sub(
            r'[A-Z]', lambda x: '_' + x.group(0).lower(), name)
        formatted_options = {camel_to_snake(key): dataflow_options[key]
                             for key in dataflow_options}
        hook.start_python_dataflow(
            self.job_name, formatted_options,
            self.py_file, self.py_options)




class DataFlowDirectRunnerHook(GFWDataFlowHook):

    def _start_dataflow(self, task_id, variables, dataflow, name, command_prefix):
        cmd = command_prefix + self._build_cmd(task_id, variables, dataflow)
        _GFWDataflow(cmd).wait_for_done()

    def _build_cmd(self, task_id, variables, dataflow):
        command = [dataflow]
        if variables is not None:
            for attr, value in list(variables.items()):
                command.append("--" + attr + "=" + value)
        return command


class DataFlowDirectRunnerOperator(GFWDataFlowPythonOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DataFlowDirectRunnerOperator, self).__init__(*args, **kwargs)

    @property
    def _is_direct_runner(self):
        runner = self.options.get(
            'runner', self.dataflow_default_options.get('runner'))
        return runner == 'DirectRunner'

    @property
    def _default_pool(self):
        return 'local-cpu' if self._is_direct_runner else 'dataflow'

    @property
    def pool(self):
        return self._pool if self._pool != Pool.DEFAULT_POOL_NAME else self._default_pool

    @pool.setter
    def pool(self, v):
        self._pool = v

    def execute_direct_runner(self, context):
        bucket_helper = GoogleCloudBucketHelper(
            self.gcp_conn_id, self.delegate_to)
        self.py_file = bucket_helper.google_cloud_to_local(self.py_file)
        dataflow_options = self.dataflow_default_options.copy()
        dataflow_options.update(self.options)
        # Convert argument names from lowerCamelCase to snake case.
        def camel_to_snake(name): return re.sub(
            r'[A-Z]', lambda x: '_' + x.group(0).lower(), name)
        formatted_options = {camel_to_snake(key): dataflow_options[key]
                             for key in dataflow_options}
        hook = DataFlowDirectRunnerHook(gcp_conn_id=self.gcp_conn_id,
                                        delegate_to=self.delegate_to)
        hook.start_python_dataflow(
            self.task_id, formatted_options,
            self.py_file, self.py_options)

        pass

    def execute(self, context):
        if self._is_direct_runner:
            return self.execute_direct_runner(context=context)
        else:
            return super(DataFlowDirectRunnerOperator, self).execute(context=context)
