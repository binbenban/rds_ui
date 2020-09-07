import flask
from flask import Flask, request
import yaml_reader
import yaml_processor as yp


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


"""
    :param feed_id: {feed_name, source_system}
"""
@app.route("/read_feed_attributes_by_feed_id/<feed_id>")
def read_feed_attributes_by_feed_id(feed_id):
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
    print(f"{eval(feed_id)}; {attribute_name}")
    met, _ = rd.feed_attributes.filter_entries_multi(
        [
            ["FEED_ID", eval(feed_id)],
            ["ATTRIBUTE_NAME", attribute_name]
        ], "ATTRIBUTE_NO"
    )
    return flask.jsonify(met[0])


@app.route("/read_one_data_object_attribute/<data_object_id>/<attribute_name>")
def read_one_data_object_attribute(data_object_id, attribute_name):
    print(f"{eval(data_object_id)}; {attribute_name}")
    met, _ = rd.data_object_attributes.filter_entries_multi(
        [
            ["DATA_OBJECT_ID", eval(data_object_id)],
            ["ATTRIBUTE_NAME", attribute_name],
        ], "ATTRIBUTE_NO"
    )
    return flask.jsonify(met[0])


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
    in request.json
        if NEW_FEED:
            new_feed_sourcesystem, new_feed_name,
            new_feed_filetype, new_feed_dbname
        else:
            feed_attributes [{
                FEED_ATTRIBUTE_ID,ATTRIBUTE_NAME,ATTRIBUTE_NO,ATTRIBUTE_TYPE,PRIMARY_KEY_IND,NULLABLE_IND,
                ATTRIBUTE_LENGTH,ATTRIBUTE_PRECISION,NESTED_ATTRIBUTE_TYPE,NESTED_ATTRIBUTE_PATH,NESTED_LEVEL
            }]
    """
    yp.save_feed(eval(feed_id), request.json)
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
        if NEW_DATA_OBJECT:
            new_data_object_dbname, new_data_object_name
        else:
            data_object_attributes [{
                DATA_OBJECT_ATTRIBUTE_ID, ATTRIBUTE_NO, ATTRIBUTE_NAME
                ATTRIBUTE_TYPE, PRIMARY_KEY_IND
            }]
    """
    yp.save_data_object(eval(data_object_id), request.json)
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
    in request.json
        attribute_mappings:
            DATA_OBJECT_ATTRIBUTE_ID:
                ATTRIBUTE_NAME:
                DATA_OBJECT_ID:
                    DATA_OBJECT_NAME
                    TGT_DB_NAME
            DATA_OBJECT_ATTRIBUTE_NAME
            DATA_OBJECT_ATTRIBUTE_NO
            DATA_OBJECT_ATTRIBUTE_TYPE
            FEED_ATTRIBUTE_ID
                ATTRIBUTE_NAME
                FEED_ID
                    FEED_NAME
                    SOURCE_SYSTEM
            FEED_ATTRIBUTE_NAME
            FEED_ATTRIBUTE_NO
            FEED_ATTRIBUTE_TYPE
            TRANSFORM_FN
        transform_sql_query:
        src_filter_sql:
    """
    yp.save_transformation(eval(feed_id), eval(data_object_id), request.json)
    return {
        "msg": "updated feed_attr_data_object_attr.yaml"
    }


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=True)
