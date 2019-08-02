from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator

import os
import os.path


class FlexibleOperator:
    def __init__(self, parameters):
        assert parameters
        self.operator_parameters = parameters

    def build_operator(self, kind):
        if kind != 'bash' and kind != 'kubernetes':
            raise ValueError('The operators allowed are bash or kubernetes, not <{}>'.format(kind))
        assert self.operator_parameters['task_id']
        assert self.operator_parameters['pool']
        assert self.operator_parameters['arguments']
        assert self.operator_parameters['image']
        task_id = self.operator_parameters['task_id']
        pool = self.operator_parameters['pool']
        arguments = self.operator_parameters['arguments']
        operator = None
        if kind == 'bash':
            assert self.operator_parameters['docker_run']
            commands = '{} {} {}'.format(
                self.operator_parameters['docker_run'],
                self.operator_parameters['image'],
                ' '.join(arguments))
            # Left only the extra parameters that are not the bash_command
            extra_params = { k : self.operator_parameters[k] for k in set(self.operator_parameters) - set(dict.fromkeys({'arguments','image','docker_run','name','dag'})) }
            operator = BashOperator(
                bash_command = commands,
                **extra_params
            )
        else:
            assert self.operator_parameters['name']
            assert self.operator_parameters['dag']
            self.operator_parameters.update(get_logs=True, in_cluster=True)
            # Left only the extra parameters that are not the kubernetes_command
            extra_params = { k : self.operator_parameters[k] for k in set(self.operator_parameters) - set(dict.fromkeys({'image','docker_run','name'})) }
            operator = KubernetesPodOperator(
                namespace = os.getenv('K8_NAMESPACE'),
                image = self.operator_parameters['image'],
                name = self.operator_parameters['name'],
                **extra_params
            )
        return operator
