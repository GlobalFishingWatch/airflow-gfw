from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.bash_operator import BashOperator

import logging
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
        assert self.operator_parameters['arguments']
        assert self.operator_parameters['image']
        task_id = self.operator_parameters['task_id']
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
            logging.debug('Running BashOperator(commands={}, {})'.format(commands, ', '.join(['{0}={1}'.format(k,v) for k,v in list(extra_params.items())])))
            operator = BashOperator(
                bash_command = commands,
                **extra_params
            )
        else:
            assert self.operator_parameters['name']
            assert self.operator_parameters['dag']
            self.operator_parameters.update(get_logs=True, in_cluster=True, reattach_on_restart=False)
            # Left only the extra parameters that are not the kubernetes_command
            extra_params = { k : self.operator_parameters[k] for k in set(self.operator_parameters) - set(dict.fromkeys({'image','docker_run','name'})) }
            logging.debug('Running KubernetesPodOperator(namespace={}, image={}, name={}, {})'.format(os.getenv('K8_NAMESPACE'), self.operator_parameters['image'], self.operator_parameters['name'], ', '.join(['{0}={1}'.format(k,v) for k,v in list(extra_params.items())])))
            operator = KubernetesPodOperator(
                namespace = os.getenv('K8_NAMESPACE'),
                image = self.operator_parameters['image'],
                name = self.operator_parameters['name'],
                **extra_params
            )
        return operator
