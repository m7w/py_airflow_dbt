import pytest

from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return DagBag()


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id="etl_pipeline")
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 4


def assert_dag_dict_equal(source, dag):
    assert dag.task_dict.keys() == source.keys()
    for task_id, downstream_list in source.items():
        assert dag.has_task(task_id)
        task = dag.get_task(task_id)
        assert task.downstream_task_ids == set(downstream_list)


def test_dag_structure(dagbag):
    assert_dag_dict_equal(
        {
            "get_last_crash_date_task": ["crash_task"],
            "crash_task": ["vehicle_task"],
            "vehicle_task": ["person_task"],
            "person_task": [],
        },
        dagbag.get_dag(dag_id="etl_pipeline"),
    )
