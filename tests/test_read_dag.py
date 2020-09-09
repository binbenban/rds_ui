from odapui import yaml_processor as yp
from odapui import yaml_reader
from pprint import pprint
import pytest
import copy


def test_read_dag():
    res = yp.read_dag({
        "DAG_NAME": "live_ingest_switch"
    })
    assert len(res) == 2
    assert res[0]["DAG_ID"]["DAG_NAME"] == "live_ingest_switch"
    assert all([x["DAG_ID"] for x in res])
    assert all([x["LOAD_NAME"] for x in res])
    assert all([x["LOAD_ID"] for x in res])
    assert all([x["LOAD_DESC"] for x in res])
    assert all([x["LOAD_EXECUTE_TYPE"] for x in res])
    assert all([x["LOAD_EXECUTE_LOGIC_NAME"] for x in res])
    assert all([x["LOAD_DEPENDENCY_TYPE"] for x in res])
    assert all([x["LOAD_WAREHOUSE_CONFIG_ID"] for x in res])


@pytest.fixture
def save_dag_data():
    res = {
        "new_dag_name": "test_dag_new",
        "new_dag_description": "test_dag this is description",
        "dag_details": [
            {
                "DATA_OBJECT_ID_CDS": {
                    "DATA_OBJECT_NAME": "somedata",
                    "TGT_DB_NAME": "cds_aa",
                },
                "LOAD_NAME": "cdc.fds_aa.somedata",
                "LOAD_DESC": "Execute CDC to maintain for somedata",
                "LOAD_EXECUTE_TYPE": "FDS_CDC",
                "LOAD_EXECUTE_LOGIC_NAME": "execute_cdc_type2",
                "LOAD_WAREHOUSE_CONFIG_NAME": "DEV_BATCH_XS",
            }
        ]
    }
    return res


def test_save_dag_new(save_dag_data):
    rd = yaml_reader.reader_instance()
    dags_count_before = len(rd.dags.entries)
    loads_count_before = len(rd.loads.entries)
    dodos_count_before = len(rd.data_object_data_objects.entries)
    yp.save_dag("NEW_DAG", save_dag_data)
    assert len(rd.dags.entries) == dags_count_before + 1
    assert rd.dags.filter_entries("DAG_NAME", "test_dag_new")
    assert len(rd.loads.entries) == loads_count_before + 1
    assert rd.loads.filter_entries("LOAD_NAME", "cdc.fds_aa.somedata")
    assert len(rd.data_object_data_objects.entries) == dodos_count_before + 1
    dodo = rd.data_object_data_objects.filter_entries(
        "LOAD_ID", 
        {
            "LOAD_NAME": "cdc.fds_aa.somedata"
        }
    )[0]
    assert dodo["LOAD_ID"]["LOAD_NAME"] == "cdc.fds_aa.somedata"
    assert dodo["LOAD_SOURCE_DATA_OBJECT_ID"] == {
        "DATA_OBJECT_NAME": "somedata",
        "TGT_DB_NAME": "cds_aa"
    }
    assert dodo["LOAD_TARGET_DATA_OBJECT_ID"] == {
        "DATA_OBJECT_NAME": "somedata",
        "TGT_DB_NAME": "fds_aa"
    }


def test_save_dag_update(save_dag_data):
    rd = yaml_reader.reader_instance()
    dag_id = {
        "DAG_NAME": "live_ingest_pim_products"
    }
    yp.save_dag(str(dag_id), save_dag_data)

    dags = rd.dags.filter_entries("DAG_NAME", "live_ingest_pim_products") 
    assert len(dags) == 1

    loads = rd.loads.filter_entries("DAG_ID", dag_id)
    assert len(loads) == 1
    assert loads[0]["LOAD_NAME"] == "cdc.fds_aa.somedata"

    dodo = rd.data_object_data_objects.filter_entries(
        "LOAD_ID",
        {
            "LOAD_NAME": "cdc.fds_aa.somedata"
        }
    )
    assert len(dodo) == 1
    assert dodo[0]["LOAD_ID"]["LOAD_NAME"] == "cdc.fds_aa.somedata"
    assert dodo[0]["LOAD_SOURCE_DATA_OBJECT_ID"] == {
        "DATA_OBJECT_NAME": "somedata",
        "TGT_DB_NAME": "cds_aa"
    }
    assert dodo[0]["LOAD_TARGET_DATA_OBJECT_ID"] == {
        "DATA_OBJECT_NAME": "somedata",
        "TGT_DB_NAME": "fds_aa"
    }
    