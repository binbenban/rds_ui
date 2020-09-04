import flask
from flask import Flask, request
import yaml_reader
import yaml_processor as yp


app = Flask(__name__)
rd = yaml_reader.Reader()


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
    res = rd.read_feed_attributes_by_feed_id(eval(feed_id))
    return flask.jsonify(res)


@app.route("/read_data_object_attributes_by_data_object_id/<data_object_id>")
def read_data_object_attributes_by_data_object_id(data_object_id):
    res = rd.read_data_object_attributes_by_data_object_id(eval(data_object_id))
    return flask.jsonify(res)


@app.route("/read_one_feed_attribute/<feed_id>/<attribute_name>")
def read_one_feed_attribute(feed_id, attribute_name):
    for fa in rd.read_feed_attributes_by_feed_id(eval(feed_id)):
        if fa["ATTRIBUTE_NAME"] == attribute_name:
            return fa
    return {}


@app.route("/read_one_data_object_attribute/<data_object_id>/<attribute_name>")
def read_one_data_object_attribute(data_object_id, attribute_name):
    for doa in rd.read_data_object_attributes_by_data_object_id(eval(data_object_id)):
        if doa["ATTRIBUTE_NAME"] == attribute_name:
            return doa
    return {}


@app.route("/read_attributes_by_feed_id_data_object_id/<feed_id>/<data_object_id>")
def read_attributes_by_feed_id_data_object_id(feed_id, data_object_id):
    res = []
    feed_attributes = rd.read_feed_attributes_by_feed_id(eval(feed_id))
    data_object_attributes = rd.read_data_object_attributes_by_data_object_id(
        eval(data_object_id)
    )
    mapping = rd.feed_attr_data_object_attr.entries
    feed_id_filter, data_object_id_filter = eval(feed_id), eval(data_object_id)
    print(feed_id_filter)
    print(data_object_id_filter)

    for m in mapping:
        # look for feed_attribute
        if (
            m["FEED_ATTRIBUTE_ID"]["SOURCE_SYSTEM"] == feed_id_filter["SOURCE_SYSTEM"]
            and m["FEED_ATTRIBUTE_ID"]["FEED_NAME"] == feed_id_filter["FEED_NAME"]
            and m["DATA_OBJECT_ATTRIBUTE_ID"]["DATA_OBJECT_NAME"]
            == data_object_id_filter["DATA_OBJECT_NAME"]
            and m["DATA_OBJECT_ATTRIBUTE_ID"]["TGT_DB_NAME"]
            == data_object_id_filter["TGT_DB_NAME"]
        ):
            fa, doa = None, None
            for ix, fa_ix in enumerate(feed_attributes):
                if fa_ix and fa_ix["FEED_ATTRIBUTE_ID"] == m["FEED_ATTRIBUTE_ID"]:
                    fa = fa_ix
                    break
            if not fa:
                continue
            for jx, doa_jx in enumerate(data_object_attributes):
                if (
                    doa_jx
                    and doa_jx["DATA_OBJECT_ATTRIBUTE_ID"]
                    == m["DATA_OBJECT_ATTRIBUTE_ID"]
                ):
                    doa = doa_jx
                    break
            if not doa:
                continue

            do = {}
            do["FEED_ATTRIBUTE_ID"] = fa["FEED_ATTRIBUTE_ID"]
            do["FEED_ATTRIBUTE_NAME"] = fa["ATTRIBUTE_NAME"]
            do["FEED_ATTRIBUTE_TYPE"] = fa["ATTRIBUTE_TYPE"]
            do["FEED_ATTRIBUTE_NO"] = fa["ATTRIBUTE_NO"]
            do["DATA_OBJECT_ATTRIBUTE_ID"] = doa["DATA_OBJECT_ATTRIBUTE_ID"]
            do["DATA_OBJECT_ATTRIBUTE_NO"] = doa["ATTRIBUTE_NO"]
            do["DATA_OBJECT_ATTRIBUTE_NAME"] = doa["ATTRIBUTE_NAME"]
            do["DATA_OBJECT_ATTRIBUTE_TYPE"] = doa["ATTRIBUTE_TYPE"]
            do["TRANSFORM_FN"] = m.get("TRANSFORM_FN", "")
            res.append(do)
            feed_attributes[ix] = None
            data_object_attributes[jx] = None

    # add rest
    for fa in feed_attributes:
        if not fa:
            continue
        do = {}
        do["FEED_ATTRIBUTE_ID"] = fa.get("FEED_ATTRIBUTE_ID")
        do["FEED_ATTRIBUTE_NAME"] = fa.get("ATTRIBUTE_NAME")
        do["FEED_ATTRIBUTE_TYPE"] = fa.get("ATTRIBUTE_TYPE")
        do["FEED_ATTRIBUTE_NO"] = fa.get("ATTRIBUTE_NO")
        res.append(do)

    for doa in data_object_attributes:
        if not doa:
            continue
        do = {}
        do["DATA_OBJECT_ATTRIBUTE_ID"] = doa["DATA_OBJECT_ATTRIBUTE_ID"]
        do["DATA_OBJECT_ATTRIBUTE_NO"] = doa["ATTRIBUTE_NO"]
        do["DATA_OBJECT_ATTRIBUTE_NAME"] = doa["ATTRIBUTE_NAME"]
        do["DATA_OBJECT_ATTRIBUTE_TYPE"] = doa["ATTRIBUTE_TYPE"]
        do["TRANSFORM_FN"] = m.get("TRANSFORM_FN")
        res.append(do)

    return flask.jsonify(res)


@app.route(
    "/read_table_transformation_by_feed_id_data_object_id/<feed_id>/<data_object_id>"
)
def read_table_transformation_by_feed_id_data_object_id(feed_id, data_object_id):
    feed_id_filter, data_object_id_filter = eval(feed_id), eval(data_object_id)

    # find the corresponding feed_id details
    feed = None
    for fd in rd.feeds.entries:
        if fd["FEED_ID"] == feed_id_filter:
            feed = fd
            feed["NEW_FEED_ID"] = {
                "FEED_NAME": feed["FEED_NAME"],
                "DB_NAME": feed["DB_NAME"],
            }
            break

    if not feed:
        return {}

    for r in rd.feed_data_objects.entries:
        if (
            feed["NEW_FEED_ID"] == r["FEED_ID"]
            and data_object_id_filter == r["DATA_OBJECT_ID"]
        ):
            do = {}
            do["SRC_FILTER_SQL"] = r.get("SRC_FILTER_SQL")
            do["TRANSFORM_SQL_QUERY"] = r.get("TRANSFORM_SQL_QUERY")
            return flask.jsonify(do)
    return {}


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
        rd.feeds.add_entry(
            yp.create_feed(new_feed, data)
        )
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
