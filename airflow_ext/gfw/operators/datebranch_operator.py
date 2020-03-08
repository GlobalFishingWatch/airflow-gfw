from airflow.operators import BranchPythonOperator

def date_branch_operator(dag, task_id,  branches):
    """
    branch on execution date.   Takes a list of tuples containing a date range and a taskid.
    If the execution date is in the range (inclusive), then the corresponding taskid will be returned.
    The first matching range in the list of branches will be used

    example:

    date_branches = [
        (None, datetime(2016, 12, 31), 'nodata'),
        (datetime(2017, 1, 1), datetime(2017, 6, 30), 'historic'),
        (datetime(2017, 7, 1), None, 'daily'),
    ]

    :param dag: dag
    :param task_id: id to assign to the branch operator task
    :param branches: list of tulples containing date range and taskid
    :return:  BranchPythonOperator
    """

    def branch_on_date(**kwargs):
        dt = kwargs['execution_date']
        for d1, d2, task in kwargs['branches']:
            if (d1 is None or dt >= d1) and (d2 is None or dt <= d2):
                return task
        raise ValueError('date_branch_operator: No matching date range found %s' % kwargs)

    op = BranchPythonOperator(
        dag=dag,
        python_callable=branch_on_date,
        task_id=task_id,
        provide_context=True,
        op_kwargs=dict(
            branches=branches
        ),
        templates_dict=dict(
            execution_date='{{ execution_date }}',
        )
    )

    return op
