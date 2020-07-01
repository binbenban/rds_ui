import flask
from flask import Flask, request
import psycopg2

import util

app = Flask(__name__)


@app.route("/")
def landing():
    feeds = _read_all_feeds()
    data_objects = _read_all_data_objects()
    return flask.render_template(
        "feeds.html", title="RDS Visualizer",
        feeds=feeds,
        data_objects=data_objects
    )


def _read_all_feeds():
    feeds = []
    with db_connect() as conn:
        curs = conn.cursor()
        stmt = """
            select f.feed_id, f.source_system, f.feed_name, f.db_name
            from odap.feed f
        """
        app.logger.info(stmt)
        curs.execute(stmt)
        for res in curs.fetchall():
            feed = {}
            feed['feed_id'] = res[0]
            feed['source_system'] = res[1]
            feed['feed_name'] = res[2]
            feed['db_name'] = res[3]
            feeds.append(feed)
    return feeds


def _read_all_data_objects():
    data_objects = []
    with db_connect() as conn:
        curs = conn.cursor()
        stmt = """
            select da.data_object_id, da.data_object_name, da.tgt_db_name
            from odap.data_object da
        """
        app.logger.info(stmt)
        curs.execute(stmt)
        for res in curs.fetchall():
            data_object = {}
            data_object['data_object_id'] = res[0]
            data_object['data_object_name'] = res[1]
            data_object['tgt_db_name'] = res[2]
            data_objects.append(data_object)
    return data_objects


@app.route("/read_one_feed/<feed_id>")
def read_one_feed(feed_id):
    res = []
    with db_connect() as conn:
        curs = conn.cursor()
        stmt = f"""
            select feed_attribute_id, attribute_name, attribute_type, attribute_no, primary_key_ind, nullable_ind, attribute_length, attribute_precision,
            nested_attribute_type, nested_attribute_path, nested_level
            from odap.feed_attribute
            where feed_id = {feed_id}
            order by attribute_no;
        """
        app.logger.info(stmt)
        curs.execute(stmt)
        for fa in curs.fetchall():
            res.append({
                "feed_attribute_id": fa[0],
                "feed_attribute_name": fa[1],
                "feed_attribute_type": fa[2],
                "feed_attribute_no": fa[3],
                "primary_key_ind": fa[4],
                "nullable_ind": fa[5],
                "attribute_length": fa[6],
                "attribute_precision": fa[7],
                "nested_attribute_type": fa[8],
                "nested_attribute_path": fa[9],
                "nested_level": fa[10],
            })
        return flask.jsonify(res)


@app.route("/read_one_data_object/<data_object_id>")
def read_one_data_object(data_object_id):
    res = []
    with db_connect() as conn:
        curs = conn.cursor()
        stmt = f"""
            select data_object_attribute_id, attribute_name, attribute_type, attribute_no, primary_key_ind
            from odap.data_object_attribute
            where data_object_id = {data_object_id}
            order by attribute_no;
        """
        app.logger.info(stmt)
        curs.execute(stmt)
        for doa in curs.fetchall():
            res.append({
                "attribute_id": doa[0],
                "attribute_name": doa[1],
                "attribute_type": doa[2],
                "attribute_no": doa[3],
                "primary_key_ind": doa[4],
            })
        return flask.jsonify(res)


@app.route("/read_one_attribute_mapping/<feed_attribute_id>/<data_object_attribute_id>")
def read_one_attribute_mapping(feed_attribute_id, data_object_attribute_id):
    res = {
        "feed_attribute_name": "",
        "feed_attribute_type": "",
        "data_object_attribute_name": "",
        "data_object_attribute_type": "",
        "primary_key_ind": "",
        "transform_fn": "",
    }

    with db_connect() as conn:
        if feed_attribute_id.isdigit():
            curs = conn.cursor()
            stmt = f"""
                select attribute_name, attribute_type
                from odap.feed_attribute
                where feed_attribute_id = {feed_attribute_id}
                limit 1;
            """
            app.logger.info(stmt)
            curs.execute(stmt)
            for fa in curs.fetchall():
                res["feed_attribute_name"] = fa[0]
                res["feed_attribute_type"] = fa[1]

        if data_object_attribute_id.isdigit():
            stmt = f"""
                select attribute_name, attribute_type, primary_key_ind
                from odap.data_object_attribute
                where data_object_attribute_id = {data_object_attribute_id}
                limit 1;
            """
            app.logger.info(stmt)
            curs.execute(stmt)
            for doa in curs.fetchall():
                res["data_object_attribute_name"] = doa[0]
                res["data_object_attribute_type"] = doa[1]
                res["primary_key_ind"] = doa[2]
        
        if feed_attribute_id.isdigit() and data_object_attribute_id.isdigit():
            stmt = f"""
                select transform_fn
                from odap.feed_attr_data_object_attr
                where feed_attribute_id={feed_attribute_id}
                and data_object_attribute_id={data_object_attribute_id}
                limit 1;
            """
            app.logger.info(stmt)
            curs.execute(stmt)
            for tr in curs.fetchall():
                res["transform_fn"] = tr[0]
        
        return flask.jsonify(res)


@app.route("/read_attributes_by_feed_id_data_object_id/<feed_id>/<data_object_id>")
def read_attributes_by_feed_id_data_object_id(feed_id, data_object_id):
    attr = []
    with db_connect() as conn:
        curs = conn.cursor()
        stmt = f"""
            with mapped as (
                select
                    fa.feed_attribute_id,
                    fa.attribute_name,
                    fa.attribute_type,
                    fa.attribute_no,
                    doa.data_object_attribute_id,
                    doa.attribute_no,
                    doa.attribute_name,
                    doa.attribute_type,
                    fada.transform_fn
                from
                    odap.feed fd
                join odap.feed_data_object fdo on
                    fd.feed_id = fdo.feed_id
                join odap.data_object dob on
                    fdo.data_object_id = dob.data_object_id
                join odap.feed_attribute fa on
                    fd.feed_id = fa.feed_id
                join odap.feed_attr_data_object_attr fada on
                    fa.feed_attribute_id = fada.feed_attribute_id
                join odap.data_object_attribute doa on
                    fada.data_object_attribute_id = doa.data_object_attribute_id 
                    and fdo.data_object_id = doa.data_object_id 
                where fd.feed_id = {feed_id} and dob.data_object_id  = {data_object_id}
                order by
                    doa.attribute_no
            )
            ,feed_attr as (
                select feed_attribute_id, attribute_name, attribute_type, attribute_no, 0, 0, '', '', ''
                from odap.feed_attribute 
                where feed_id = {feed_id}
            )
            ,data_obj_attr as (
                select 0, '', '', 0, data_object_attribute_id, attribute_no, attribute_name, attribute_type, ''
                from odap.data_object_attribute
                where data_object_id = {data_object_id}
            )
            select * from mapped
            union all
            select * from feed_attr 
            where feed_attribute_id not in (select feed_attribute_id from mapped)
            union all
            select * from data_obj_attr
            where data_object_attribute_id not in (select data_object_attribute_id from mapped);
        """
        app.logger.info(stmt)
        curs.execute(stmt)
        for res in curs.fetchall():
            do = {}
            do["feed_attribute_id"] = res[0]
            do["feed_attribute_name"] = res[1]
            do["feed_attribute_type"] = res[2]
            do["feed_attribute_no"] = res[3]
            do["doa_id"] = res[4]
            do["doa_no"] = res[5]
            do["doa_name"] = res[6]
            do["doa_type"] = res[7]
            do["transform_fn"] = res[8]
            attr.append(do)
        return flask.jsonify(attr)


@app.route("/save_attributes", methods=["POST"])
def save_attributes():
    stmts = []
    for row in request.json:
        if not (row["feed_attribute_id"] and row["doa_id"]):
            stmts.append(f"""
                -- ignoring {row} as it's not valid
            """)
        else:
            transform_fn = row.get("transform_fn")
            if transform_fn is None:
                transform_fn = ""
            transform_fn = transform_fn.strip()
            if transform_fn=="" or transform_fn==row.get("feed_attribute_name"):
                transform_fn = None
            else:
                transform_fn = transform_fn.replace("'", "''")
            stmts.append(f"""
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
            """)
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
        stmts.append(f"""
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
        """)

    feed_condition = ""
    if feed_id == NEW_FEED:  # find feed_id by feed attributes
        feed_condition = f"""
            and feed_name='{data.get("new_feed_name")}' 
            and source_system='{data.get("new_feed_sourcesystem")}'
        """
    else:  # use feed_id parameter
        feed_condition = f"and feed_id='{feed_id}'"

    for row in data["feed_attributes"]:
        if not row.get("feed_attribute_no") or not row.get("feed_attribute_name") or not row.get("feed_attribute_type"):
            stmts.append(f"""
                -- ignoring {row} as it's not valid
            """)
        else:
            stmts.append(f"""
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
                    '{row.get("attribute_precision")}',
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
            """)

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
        stmts.append(f"""
            insert into odap.data_object(
                data_object_id, data_object_name, tgt_db_name
            )
            select
                max(data_object_id)+1,
                '{data.get("new_data_object_name")}',
                '{data.get("new_data_object_dbname")}'
            from odap.data_object;
        """)

    data_object_condition = ""
    if data_object_id == NEW_DATA_OBJECT:  # find data_object_id by data_object attributes
        data_object_condition = f"""
            and data_object_name='{data.get("new_data_object_name")}' 
        """
    else:  # use data_object_id from parameter
        data_object_condition = f"and data_object_id='{data_object_id}'"

    for row in data["data_object_attributes"]:
        if not row.get("attribute_no") or not row.get("attribute_name") or not row.get("attribute_type"):
            stmts.append(f"""
                -- ignoring {row} as it's not valid
            """)
        else:
            stmts.append(f"""
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
            """)
    combined_stmt = "\n".join(stmts).replace("'None'", "null")
    app.logger.info(combined_stmt)
    return flask.jsonify(combined_stmt)

def db_connect():
    connection = psycopg2.connect(
        user=util.get_config_param("db")["user"],
        password=util.get_config_param("db")["password"],
        host="localhost",
        port=5432,
        database="postgres"
    )
    return connection

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, debug=True)
