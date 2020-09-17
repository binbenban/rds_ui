import pytest
from odapui import yaml_processor as yp
from pprint import pprint


def test_read_feed_attr_data_object_attr_1():
    feed_id = {
        "SOURCE_SYSTEM": "aa",
        "FEED_NAME": "browser"
    }
    data_object_id = {
        "DATA_OBJECT_NAME": "browser",
        "TGT_DB_NAME": "cds_aa"
    }
    res = yp.map_feed_attr_data_object_attr(
        feed_id, data_object_id
    )
    assert res
    assert all([x["FEED_ATTRIBUTE_NAME"] for x in res])
    assert all([x["DATA_OBJECT_ATTRIBUTE_NAME"] for x in res])


def test_read_feed_attr_data_object_attr_2():
    feed_id = {
        "SOURCE_SYSTEM": "zzz",
        "FEED_NAME": "browser"
    }
    data_object_id = {
        "DATA_OBJECT_NAME": "browser",
        "TGT_DB_NAME": "cds_aa"
    }
    res = yp.map_feed_attr_data_object_attr(
        feed_id, data_object_id
    )
    assert res
    assert all(["FEED_ATTRIBUTE_NAME" not in x for x in res])
    assert all([x["DATA_OBJECT_ATTRIBUTE_NAME"] for x in res])
