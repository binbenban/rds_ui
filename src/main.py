import flask
from flask import Flask, request
# import psycopg2

import util

app = Flask(__name__)


FEEDS = None
DATA_OBJECTS = None
FEED_ATTRIBUTES = None
DATA_OBJECT_ATTRIBUTES = None
FEED_ATTR_DATA_OBJECT_ATTR = None
FEED_DATA_OBJECTS = None


def _read_all_feeds():
    global FEEDS
    app.logger.info("reading all feeds from yaml")
    if not FEEDS:
        app.logger.info("...no cache. reading from yaml")
        FEEDS = _read_from_yaml("feed", ["SOURCE_SYSTEM", "FEED_NAME"])
    return FEEDS


def _read_all_data_objects():
    global DATA_OBJECTS
    app.logger.info("reading all data objects from yaml")
    if not DATA_OBJECTS:
        app.logger.info("...no cache. reading from yaml")
        DATA_OBJECTS = _read_from_yaml("data_object", ["DATA_OBJECT_NAME", "TGT_DB_NAME"])
    return DATA_OBJECTS


def _read_all_feed_attributes():
    global FEED_ATTRIBUTES
    app.logger.info("reading all feed attributes from yaml")
    if not FEED_ATTRIBUTES:
        app.logger.info("...no cache. reading from yaml")
        FEED_ATTRIBUTES = _read_from_yaml("feed_attribute", ["FEED_ID", "ATTRIBUTE_NAME"])
    return FEED_ATTRIBUTES


def _read_all_data_object_attributes():
    global DATA_OBJECT_ATTRIBUTES
    app.logger.info("reading all data object attributes from yaml")
    if not DATA_OBJECT_ATTRIBUTES:
        app.logger.info("...no cache. reading from yaml")
        DATA_OBJECT_ATTRIBUTES = _read_from_yaml("data_object_attribute", ["DATA_OBJECT_ID", "ATTRIBUTE_NAME"])
    return DATA_OBJECT_ATTRIBUTES


def _read_all_feed_attr_data_object_attr():
    global FEED_ATTR_DATA_OBJECT_ATTR
    app.logger.info("reading all feed attr data object attr from yaml")
    if not FEED_ATTR_DATA_OBJECT_ATTR:
        app.logger.info("...no cache. reading from yaml")
        FEED_ATTR_DATA_OBJECT_ATTR = _read_from_yaml("feed_attr_data_object_attr", ["FEED_ATTRIBUTE_ID", "DATA_OBJECT_ATTRIBUTE_ID"])
    return FEED_ATTR_DATA_OBJECT_ATTR


def _read_all_feed_data_objects():
    global FEED_DATA_OBJECTS
    app.logger.info("reading all feed data objects")
    if not FEED_DATA_OBJECTS:
        app.logger.info("...no cache. reading from yaml")
        FEED_DATA_OBJECTS = _read_from_yaml("feed_data_object", ["FEED_ID", "DATA_OBJECT_ID"])
    return FEED_DATA_OBJECTS


@app.route("/")
def landing():
    return flask.render_template(
        "feeds.html",
        title="RDS Visualizer",
        feeds=_read_all_feeds(),
        data_objects=_read_all_data_objects()
    )


def _read_from_yaml(
    file_name: str, key_attributes: list
):
    res = []
    key_name = f"{file_name}_ID".upper()

    from_yaml = util.read_metadata_yaml(file_name)
    for r in from_yaml['data_object']['data_records']:
        row = r['row']
        record = {}
        record[key_name] = {}
        for k in key_attributes:
            if isinstance(row[k], dict):
                for key, val in row[k].items():
                    record[key_name][key] = val
            else:
                record[key_name][k] = row[k]

        for k, v in row.items():
            record[k] = v
        res.append(record)
    return res


def _read_feed_attributes_by_feed_id(feed_id: str):
    res = []
    for x in _read_all_feed_attributes():
        if x["FEED_ID"] == eval(feed_id):
            res.append(x)
    res.sort(key=lambda x: x["ATTRIBUTE_NO"])
    return res


@app.route("/read_feed_attributes_by_feed_id/<feed_id>")
def read_feed_attributes_by_feed_id(feed_id):
    res = _read_feed_attributes_by_feed_id(feed_id)
    return flask.jsonify(res)
    

def _read_data_object_attributes_by_data_object_id(data_object_id: str):
    res = []
    for x in _read_all_data_object_attributes():
        if x["DATA_OBJECT_ID"] == eval(data_object_id):
            res.append(x)
    res.sort(key=lambda x: x["ATTRIBUTE_NO"])
    return res


@app.route("/read_data_object_attributes_by_data_object_id/<data_object_id>")
def read_data_object_attributes_by_data_object_id(data_object_id):
    res = _read_data_object_attributes_by_data_object_id(data_object_id)
    return flask.jsonify(res)


@app.route("/read_one_feed_attribute/<feed_id>/<attribute_name>")
def read_one_feed_attribute(feed_id, attribute_name):
    for fa in _read_feed_attributes_by_feed_id(feed_id):
        if fa["ATTRIBUTE_NAME"] == attribute_name:
            return fa
    return {}


@app.route("/read_one_data_object_attribute/<data_object_id>/<attribute_name>")
def read_one_data_object_attribute(data_object_id, attribute_name):
    for doa in _read_data_object_attributes_by_data_object_id(data_object_id):
        if doa["ATTRIBUTE_NAME"] == attribute_name:
            return doa
    return {}



@app.route("/read_attributes_by_feed_id_data_object_id/<feed_id>/<data_object_id>")
def read_attributes_by_feed_id_data_object_id(feed_id, data_object_id):
    res = []
    feed_attributes = _read_feed_attributes_by_feed_id(feed_id)
    data_object_attributes = _read_data_object_attributes_by_data_object_id(data_object_id)
    mapping = _read_all_feed_attr_data_object_attr()
    feed_id_filter, data_object_id_filter = eval(feed_id), eval(data_object_id)

    for m in mapping:
        # look for feed_attribute
        if (
            m["FEED_ATTRIBUTE_ID"]["SOURCE_SYSTEM"] == feed_id_filter["SOURCE_SYSTEM"] and
            m["FEED_ATTRIBUTE_ID"]["FEED_NAME"] == feed_id_filter["FEED_NAME"] and
            m["DATA_OBJECT_ATTRIBUTE_ID"]["DATA_OBJECT_NAME"] == data_object_id_filter["DATA_OBJECT_NAME"] and
            m["DATA_OBJECT_ATTRIBUTE_ID"]["TGT_DB_NAME"] == data_object_id_filter["TGT_DB_NAME"]
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
                    doa_jx and
                    doa_jx["DATA_OBJECT_ATTRIBUTE_ID"] == m["DATA_OBJECT_ATTRIBUTE_ID"]
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
        do["FEED_ATTRIBUTE_ID"] = fa["FEED_ATTRIBUTE_ID"]
        do["FEED_ATTRIBUTE_NAME"] = fa["ATTRIBUTE_NAME"]
        do["FEED_ATTRIBUTE_TYPE"] = fa["ATTRIBUTE_TYPE"]
        do["FEED_ATTRIBUTE_NO"] = fa["ATTRIBUTE_NO"]
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

    with db_connect() as conn:
        curs = conn.cursor()
        stmt = f"""
            select transform_sql_query, src_filter_sql
            from odap.feed_data_object
            where feed_id = {feed_id} and data_object_id = {data_object_id}
            limit 1
        """
        app.logger.info(stmt)
        curs.execute(stmt)
        do = {}
        for res in curs.fetchall():
            do["transform_sql_query"] = res[0]
            do["src_filter_sql"] = res[1]
            break
        return flask.jsonify(do)


@app.route("/save_transformation/<feed_id>/<data_object_id>", methods=["POST"])
def save_transformation(feed_id, data_object_id):
    stmts = []
    data = request.json
    attribute_mappings = data.get("attribute_mappings")
    src_filter_sql = data.get("src_filter_sql")
    transform_sql_query = data.get("transform_sql_query")
    print(request.json)

    stmts.append(
        f"""
    insert into odap.feed_data_object (feed_id,data_object_id,transform_sql_query,src_filter_sql)
    values ({feed_id}, {data_object_id}, '{None if not transform_sql_query else transform_sql_query}','{None if not src_filter_sql else src_filter_sql}');
    """
    )

    for row in attribute_mappings:
        if not (row["feed_attribute_id"] and row["doa_id"]):
            stmts.append(
                f"""
                -- ignoring {row} as it's not valid
            """
            )
        else:
            transform_fn = row.get("transform_fn")
            if transform_fn is None:
                transform_fn = ""
            transform_fn = transform_fn.strip()
            if transform_fn == "" or transform_fn == row.get("feed_attribute_name"):
                transform_fn = None
            else:
                transform_fn = transform_fn.replace("'", "''")
            stmts.append(
                f"""
                insert into odap.feed_attr_data_object_attr (
                    feed_attribute_id,
                    data_object_attribute_id,
                    transform_fn
                )
                select
                    '{row.get("feed_attribute_id")}',
                    '{row.get("doa_id")}',
                    '{transform_fn}'
                ;
            """
            )
    combined_stmt = "\n".join(stmts).replace("'None'", "null")
    app.logger.info(combined_stmt)
    return flask.jsonify(combined_stmt)


@app.route("/save_feed/<feed_id>", methods=["POST"])
def save_feed(feed_id):
    NEW_FEED = "NEW_FEED"
    data = request.json

    stmts = []
    if feed_id == NEW_FEED:
        # create feed; then create attributes
        stmts.append(
            f"""
            insert into odap.feed(
                feed_id, source_system, feed_name, feed_file_type, db_name
            )
            select
                max(feed_id)+1,
                '{data.get("new_feed_sourcesystem")}',
                '{data.get("new_feed_name")}',
                '{data.get("new_feed_filetype")}',
                '{data.get("new_feed_dbname")}'
            from odap.feed;
        """
        )

    feed_condition = ""
    if feed_id == NEW_FEED:  # find feed_id by feed attributes
        feed_condition = f"""
            and feed_name='{data.get("new_feed_name")}' 
            and source_system='{data.get("new_feed_sourcesystem")}'
        """
    else:  # use feed_id parameter
        feed_condition = f"and feed_id='{feed_id}'"

    for row in data["feed_attributes"]:
        if (
            not row.get("feed_attribute_no")
            or not row.get("feed_attribute_name")
            or not row.get("feed_attribute_type")
        ):
            stmts.append(
                f"""
                -- ignoring {row} as it's not valid
            """
            )
        else:
            stmts.append(
                f"""
                insert into odap.feed_attribute(
                    feed_attribute_id,
                    feed_id,
                    attribute_name,
                    attribute_desc,
                    attribute_no,
                    attribute_type,
                    primary_key_ind,
                    nullable_ind,
                    attribute_length,
                    attribute_precision,
                    nested_attribute_type,
                    nested_attribute_path,
                    nested_level
                )
                select
                    max(feed_attribute_id)+1,
                    f.feed_id,
                    '{row["feed_attribute_name"]}',
                    null,
                    {row.get("feed_attribute_no")},
                    '{row.get("feed_attribute_type")}',
                    '{row.get("primary_key_ind")}',
                    '{row.get("nullable_ind")}',
                    '{row.get("attribute_length")}',
                    '{"None" if not row.get("attribute_precision") else row.get("attribute_precision")}',
                    '{row.get("nested_attribute_type")}',
                    '{row.get("nested_attribute_path")}',
                    '{row.get("nested_level")}'
                from odap.feed_attribute fa
                cross join (
                    select feed_id from odap.feed f
                    where 1=1 {feed_condition}
                    limit 1
                ) f
                group by f.feed_id
                ;
            """
            )

    combined_stmt = "\n".join(stmts).replace("'None'", "null")
    app.logger.info(combined_stmt)
    return flask.jsonify(combined_stmt)


@app.route("/save_data_object/<data_object_id>", methods=["POST"])
def save_data_object(data_object_id):
    NEW_DATA_OBJECT = "NEW_DATA_OBJECT"
    data = request.json
    stmts = []

    if data_object_id == NEW_DATA_OBJECT:
        # create data object; then create attributes
        stmts.append(
            f"""
            insert into odap.data_object(
                data_object_id, data_object_name, tgt_db_name
            )
            select
                max(data_object_id)+1,
                '{data.get("new_data_object_name")}',
                '{data.get("new_data_object_dbname")}'
            from odap.data_object;
        """
        )

    data_object_condition = ""
    if (
        data_object_id == NEW_DATA_OBJECT
    ):  # find data_object_id by data_object attributes
        data_object_condition = f"""
            and data_object_name='{data.get("new_data_object_name")}' 
        """
    else:  # use data_object_id from parameter
        data_object_condition = f"and data_object_id='{data_object_id}'"

    for row in data["data_object_attributes"]:
        if (
            not row.get("attribute_no")
            or not row.get("attribute_name")
            or not row.get("attribute_type")
        ):
            stmts.append(
                f"""
                -- ignoring {row} as it's not valid
            """
            )
        else:
            stmts.append(
                f"""
                insert into odap.data_object_attribute(
                    data_object_attribute_id,
                    data_object_id,
                    attribute_no,
                    attribute_name,
                    attribute_type,
                    primary_key_ind
                )
                select
                    max(data_object_attribute_id)+1,
                    da.data_object_id,
                    '{row.get("attribute_no")}',
                    '{row.get("attribute_name")}',
                    '{row.get("attribute_type")}',
                    '{row.get("primary_key_ind")}'
                from odap.data_object_attribute
                cross join (
                    select data_object_id from odap.data_object
                    where 1=1 {data_object_condition}
                    limit 1
                ) da
                group by da.data_object_id
                ;
            """
            )
    combined_stmt = "\n".join(stmts).replace("'None'", "null")
    app.logger.info(combined_stmt)
    return flask.jsonify(combined_stmt)


def db_connect():
    connection = psycopg2.connect(
        user=util.get_config_param("db")["user"],
        password=util.get_config_param("db")["password"],
        host="localhost",
        port=5432,
        database="postgres",
    )
    return connection


if __name__ == "__main__":
    _read_all_feeds()
    _read_all_data_objects()
    _read_all_feed_attributes()
    _read_all_data_object_attributes()
    _read_all_feed_attr_data_object_attr()
    _read_all_feed_data_objects()
    app.run(host="0.0.0.0", port=3000, debug=True)
