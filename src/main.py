import flask
from flask import Flask, request
import yaml_reader
import yaml_processor as yp
from schema import Schema, And, Use, Optional, Or


app = Flask(__name__)
rd = yaml_reader.reader_instance()


@app.route("/")
def landing():
    return flask.render_template(
        "feeds.html",
        title="RDS Visualizer",
        feeds=rd.feeds.entries,
        data_objects=rd.data_objects.entries,
    )


@app.route("/read_feed_attributes_by_feed_id/<feed_id>")
def read_feed_attributes_by_feed_id(feed_id):
    """:param feed_id: {feed_name, source_system}"""
    res = rd.feed_attributes.filter_entries(
        "FEED_ID", eval(feed_id), "ATTRIBUTE_NO"
    )
    return flask.jsonify(res)


@app.route("/read_data_object_attributes_by_data_object_id/<data_object_id>")
def read_data_object_attributes_by_data_object_id(data_object_id):
    res = rd.data_object_attributes.filter_entries(
        "DATA_OBJECT_ID", eval(data_object_id), "ATTRIBUTE_NO"
    )
    return flask.jsonify(res)


@app.route("/read_one_feed_attribute/<feed_id>/<attribute_name>")
def read_one_feed_attribute(feed_id, attribute_name):
    app.logger.info(f"feed_id={feed_id}, attr_name={attribute_name}")
    met, _ = rd.feed_attributes.filter_entries_multi(
        [
            ["FEED_ID", eval(feed_id)],
            ["ATTRIBUTE_NAME", attribute_name]
        ], "ATTRIBUTE_NO"
    )
    if met:
        return flask.jsonify(met[0])
    else:
        return {}


@app.route("/read_one_data_object_attribute/<data_object_id>/<attribute_name>")
def read_one_data_object_attribute(data_object_id, attribute_name):
    app.logger.info(f"feed_id={data_object_id}, attr_name={attribute_name}")
    met, _ = rd.data_object_attributes.filter_entries_multi(
        [
            ["DATA_OBJECT_ID", eval(data_object_id)],
            ["ATTRIBUTE_NAME", attribute_name],
        ], "ATTRIBUTE_NO"
    )
    app.logger.info(met)
    if met:
        return flask.jsonify(met[0])
    else:
        return {}


@app.route("/map_feed_attr_data_object_attr/<feed_id>/<data_object_id>")
def map_feed_attr_data_object_attr(feed_id, data_object_id):
    res = yp.map_feed_attr_data_object_attr(
        eval(feed_id), eval(data_object_id)
    )
    return flask.jsonify(res)


@app.route(
    "/read_table_transformation_by_feed_id_data_object_id/<feed_id>/<data_object_id>"
)
def read_table_transformation_by_feed_id_data_object_id(
    feed_id, data_object_id
):
    res = yp.read_table_transformation_by_feed_id_data_object_id(
        eval(feed_id), eval(data_object_id)
    )
    return flask.jsonify(res)


@app.route("/save_feed/<feed_id>", methods=["POST"])
def save_feed(feed_id):
    """
    save a feed to the Table instance
    1. update rd.feeds.entries
    2. update rd.feed_attributes.entries
    parameters:
        feed_id: serialised feed_id or 'NEW_FEED'
    """
    schema = Schema(
        {
            Optional("new_feed_source_system"): str,
            Optional("new_feed_name"): str,
            Optional("new_feed_filetype"): str,
            Optional("new_feed_dbname"): str,
            "feed_attributes": [{
                "ATTRIBUTE_NAME": And(str, len),
                "ATTRIBUTE_NO": Use(int),
                "ATTRIBUTE_TYPE": And(str, len),
                "PRIMARY_KEY_IND": And(str, lambda s: s in ["Y", "N"]),
                "NULLABLE_IND": And(str, lambda s: s in ["Y", "N"]),
                Optional("ATTRIBUTE_LENGTH"): And(Use(int), lambda n: n > 0),
                Optional("ATTRIBUTE_PRECISION"): And(str, len),
                Optional("NESTED_ATTRIBUTE_TYPE"): And(str, len),
                Optional("NESTED_ATTRIBUTE_PATH"): And(str, len),
                Optional("NESTED_LEVEL"): And(Use(int), lambda n: n > 0),
            }]
        }, ignore_extra_keys=True
    )
    schema.validate(request.json)
    app.logger.info("feed attr schema validated")
    yp.save_feed(feed_id, request.json)
    return {"msg": "updated temp_feed.yaml, temp_feed_attribute.yaml"}


@app.route("/save_data_object/<data_object_id>", methods=["POST"])
def save_data_object(data_object_id):
    """
    save a data object to the Table instance
    1. update rd.data_objects.entries
    2. update rd.data_object_attributes.entries
    parameters:
        data_object_id: serialised data_object_id or 'NEW_DATA_OBJECT'
    in request.json
        data_object_attributes [{
            DATA_OBJECT_ATTRIBUTE_ID, ATTRIBUTE_NO, ATTRIBUTE_NAME
            ATTRIBUTE_TYPE, PRIMARY_KEY_IND
        }]
    """
    schema = Schema(
        {
            Optional("new_data_object_dbname"): str,
            Optional("new_data_object_name"): str,
            "data_object_attributes": [{
                "ATTRIBUTE_NAME": And(str, len),
                "ATTRIBUTE_NO": Use(int),
                "ATTRIBUTE_TYPE": And(str, len),
                "PRIMARY_KEY_IND": And(str, lambda s: s in ["Y", "N"]),
            }]
        }, ignore_extra_keys=True
    )
    schema.validate(request.json)
    yp.save_data_object(data_object_id, request.json)
    return {
        "msg":
        "updated temp_data_objects.yaml, temp_data_objects_attributes.yaml"
    }


@app.route("/save_transformation/<feed_id>/<data_object_id>", methods=["POST"])
def save_transformation(feed_id, data_object_id):
    """
    assume feed, feed_attr, data_object, data_object_attr all exist in memory
    in attr mapping entries
    - delete where feed_attr_id and data_object_attr_id match (need flatten)
    add new entries from request.json to attr mapping entries

    parameters:
        feed_id:
            SOURCE_SYSTEM':
            'FEED_NAME':
        data_object_id:
            DATA_OBJECT_NAME
            TGT_DB_NAME
    """
    schema = Schema(
        {
            "attribute_mappings": [{
                "DATA_OBJECT_ATTRIBUTE_ID": {
                    "ATTRIBUTE_NAME": And(str, len),
                    "DATA_OBJECT_ID": {
                        "DATA_OBJECT_NAME": And(str, len),
                        "TGT_DB_NAME": And(str, len)
                    }
                },
                "DATA_OBJECT_ATTRIBUTE_NAME": Use(str, len),
                "DATA_OBJECT_ATTRIBUTE_NO": And(Use(int), lambda n: n > 0),
                "DATA_OBJECT_ATTRIBUTE_TYPE": And(str, len),
                "FEED_ATTRIBUTE_ID": {
                    "ATTRIBUTE_NAME": Use(str, len),
                    "FEED_ID": {
                        "FEED_NAME": Use(str, len),
                        "SOURCE_SYSTEM": Use(str, len)
                    }
                },
                "FEED_ATTRIBUTE_NAME": Use(str, len),
                "FEED_ATTRIBUTE_NO": And(Use(int), lambda n: n > 0),
                "FEED_ATTRIBUTE_TYPE": And(str, len),
                "TRANSFORM_FN": Or(None, And(str, len)),
            }],
            "transform_sql_query": str,
            "src_filter_sql": str
        }, ignore_extra_keys=True
    )
    schema.validate(request.json)
    yp.save_transformation(eval(feed_id), eval(data_object_id), request.json)
    return {
        "msg": "updated feed_attr_data_object_attr.yaml"
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=True)
