from odapui import yaml_processor as yp


def test_read_feed_attr_data_object_attr_1():
    rd = yaml_reader.Reader.get_instance()
    feed_id = {
        "SOURCE_SYSTEM": "aa",
        "FEED_NAME": "browser"
    }
    data_object_id = {
        "DATA_OBJECT_NAME": "browser",
        "TGT_DB_NAME": "cds_aa"
    }
    res = yp.map_feed_attr_data_object_attr(
        rd, feed_id, data_object_id
    )
    assert res
    assert all([x["FEED_ATTRIBUTE_NAME"] for x in res])
    assert all([x["DATA_OBJECT_ATTRIBUTE_NAME"] for x in res])


def test_read_feed_attr_data_object_attr_2():
    rd = yaml_reader.Reader.get_instance()
    feed_id = {
        "SOURCE_SYSTEM": "zzz",
        "FEED_NAME": "browser"
    }
    data_object_id = {
        "DATA_OBJECT_NAME": "browser",
        "TGT_DB_NAME": "cds_aa"
    }
    res = yp.map_feed_attr_data_object_attr(
        rd, feed_id, data_object_id
    )
    assert res
    assert all(["FEED_ATTRIBUTE_NAME" not in x for x in res])
    assert all([x["DATA_OBJECT_ATTRIBUTE_NAME"] for x in res])
