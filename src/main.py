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
    for fa in rd.feed_attributes.filter_entries(
        "FEED_ID", eval(feed_id), "ATTRIBUTE_NO"
    ):
        if fa["ATTRIBUTE_NAME"] == attribute_name:
            return fa
    return {}


@app.route("/read_one_data_object_attribute/<data_object_id>/<attribute_name>")
def read_one_data_object_attribute(data_object_id, attribute_name):
    for doa in rd.data_object_attributes.filter_entries(
        "DATA_OBJECT_ID", eval(data_object_id), "ATTRIBUTE_NO"
    ):
        if doa["ATTRIBUTE_NAME"] == attribute_name:
            return doa
    return {}


@app.route("/read_attributes_by_feed_id_data_object_id/<feed_id>/<data_object_id>")
def read_attributes_by_feed_id_data_object_id(feed_id, data_object_id):
    feed_id_filter, data_object_id_filter = eval(feed_id), eval(data_object_id)
    feed_attrs = rd.feed_attributes.filter_entries(
        "FEED_ID", feed_id_filter, "ATTRIBUTE_NO"
    )
    data_object_attrs = rd.data_object_attributes.filter_entries(
        "DATA_OBJECT_ID", data_object_id_filter, "ATTRIBUTE_NO"
    )
    mapping = rd.feed_attr_data_object_attr.entries

    res = yp.map_feed_data_object_attr(
        feed_id_filter, data_object_id_filter,
        feed_attrs, data_object_attrs, mapping
    )
    return flask.jsonify(res)


@app.route(
    "/read_table_transformation_by_feed_id_data_object_id/<feed_id>/<data_object_id>"
)
def read_table_transformation_by_feed_id_data_object_id(
        feed_id, data_object_id):
    feed_id_filter, data_object_id_filter = eval(feed_id), eval(data_object_id)

    feeds = rd.feeds.filter_entries(
        "FEED_ID", feed_id_filter, None
    )
    if not feeds:
        return {}
    feed = feeds[0]

    feed_data_objects = rd.feed_data_objects.filter_entries(
        "DATA_OBJECT_ID", data_object_id_filter, None
    )
    res = yp.search_feed_data_object(feed, feed_data_objects)
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
            new_feed_sourcesystem, new_feed_name, new_feed_filetype, new_feed_dbname
        else:
            feed_attributes [{
                FEED_ATTRIBUTE_ID,ATTRIBUTE_NAME,ATTRIBUTE_NO,ATTRIBUTE_TYPE,PRIMARY_KEY_IND,NULLABLE_IND,
                ATTRIBUTE_LENGTH,ATTRIBUTE_PRECISION,NESTED_ATTRIBUTE_TYPE,NESTED_ATTRIBUTE_PATH,NESTED_LEVEL
            }]
    """
    data = request.json

    if feed_id == "NEW_FEED":
        new_feed = {
            "SOURCE_SYSTEM": data.get("new_feed_sourcesystem"),
            "FEED_NAME": data.get("new_feed_name"),
            "FEED_FILE_TYPE": data.get("new_feed_filetype"),
            "DB_NAME": data.get("new_feed_dbname"),
        }
        rd.feeds.add_entry(new_feed)

        rd.feed_attributes.add_entries(
            yp.create_feed_attributes(new_feed, data)
        )
    else:
        feed = {
            "SOURCE_SYSTEM": eval(feed_id)["SOURCE_SYSTEM"],
            "FEED_NAME": eval(feed_id)["FEED_NAME"],
        }
        feed_attrs = yp.create_feed_attributes(feed, data)
        rd.feed_attributes.delete_entries("FEED_ID", feed_attrs[0]["FEED_ID"])
        rd.feed_attributes.add_entries(feed_attrs)

    rd.feeds.dump()
    rd.feed_attributes.dump()
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
                DATA_OBJECT_ATTRIBUTE_ID
                ATTRIBUTE_NO
                ATTRIBUTE_NAME
                ATTRIBUTE_TYPE
                PRIMARY_KEY_IND
            }]
    """
    data = request.json

    if data_object_id == "NEW_DATA_OBJECT":
        new_data_object = {
            "DATA_OBJECT_NAME": data.get("new_data_object_name"),
            "TGT_DB_NAME": data.get("new_data_object_dbname"),
        }
        rd.data_objects.add_entry(
            yp.create_data_object(new_data_object, data)
        )
        rd.data_object_attributes.add_entries(
            yp.create_feed_attributes(new_data_object, data)
        )
    else:
        data_object = {
            "DATA_OBJECT_NAME": eval(data_object_id)["DATA_OBJECT_NAME"],
            "TGT_DB_NAME": eval(data_object_id)["TGT_DB_NAME"],
        }
        data_object_attrs = yp.create_data_object_attributes(data_object, data)
        rd.data_object_attributes.delete_entries(
            "DATA_OBJECT_ID", data_object_attrs[0]["DATA_OBJECT_ID"]
        )
        rd.data_object_attributes.add_entries(data_object_attrs)

    rd.data_objects.dump()
    rd.data_object_attributes.dump()
    return {
        "msg": "updated temp_data_objects.yaml, temp_data_objects_attributes.yaml"
    }


@app.route("/save_transformation/<feed_id>/<data_object_id>", methods=["POST"])
def save_transformation(feed_id, data_object_id):
    """
    1. update rd.feed_attr_data_object_attr.entries
    2. update rd.feed_data_object.entries
    parameters:
        data_object_id: serialised data_object_id or 'NEW_DATA_OBJECT'
    in request.json
        if NEW_DATA_OBJECT:
            new_data_object_dbname, new_data_object_name
        else:
            data_object_attributes [{
                DATA_OBJECT_ATTRIBUTE_ID
                ATTRIBUTE_NO
                ATTRIBUTE_NAME
                ATTRIBUTE_TYPE
                PRIMARY_KEY_IND
            }]
    """
    pass


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=True)
