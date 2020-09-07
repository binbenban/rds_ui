import util
import yaml_reader
from ruamel.yaml.comments import CommentedMap as ordereddict


rd = yaml_reader.reader_instance()


def map_feed_attr_data_object_attr(feed_id, data_object_id):
    feed_attrs = rd.feed_attributes.filter_entries(
        "FEED_ID", feed_id, "ATTRIBUTE_NO"
    )
    data_object_attrs = rd.data_object_attributes.filter_entries(
        "DATA_OBJECT_ID", data_object_id, "ATTRIBUTE_NO"
    )
    mapping = rd.feed_attr_data_object_attr.entries
    res = []

    # for each mapping, find details in feed_attrs & data_object_attrs
    for m in mapping:
        if (m["ZZ_FEED_ID"] != feed_id
                or m["ZZ_DATA_OBJECT_ID"] != data_object_id):
            continue

        fa, doa = None, None
        for ix, fa_ix in enumerate(feed_attrs):
            if not fa_ix:
                continue
            if fa_ix["ZZ_FEED_ATTRIBUTE_ID_FLAT"] == m["FEED_ATTRIBUTE_ID"]:
                fa = fa_ix
                break
        if not fa:
            continue
        for jx, doa_jx in enumerate(data_object_attrs):
            if not doa_jx:
                continue
            if doa_jx["ZZ_DATA_OBJECT_ATTRIBUTE_ID_FLAT"] == \
                    m["DATA_OBJECT_ATTRIBUTE_ID"]:
                doa = doa_jx
                break
        if not doa:
            continue
        do = {}
        util.copy_keys(
            {
                "ZZ_FEED_ATTRIBUTE_ID": "FEED_ATTRIBUTE_ID",
                "ATTRIBUTE_NAME": "FEED_ATTRIBUTE_NAME",
                "ATTRIBUTE_TYPE": "FEED_ATTRIBUTE_TYPE",
                "ATTRIBUTE_NO": "FEED_ATTRIBUTE_NO",
            }, fa, do)
        util.copy_keys(
            {
                "ZZ_DATA_OBJECT_ATTRIBUTE_ID": "DATA_OBJECT_ATTRIBUTE_ID",
                "ATTRIBUTE_NO": "DATA_OBJECT_ATTRIBUTE_NO",
                "ATTRIBUTE_NAME": "DATA_OBJECT_ATTRIBUTE_NAME",
                "ATTRIBUTE_TYPE": "DATA_OBJECT_ATTRIBUTE_TYPE",
            }, doa, do)
        util.copy_keys(
            {
                "TRANSFORM_FN": "TRANSFORM_FN",
            }, m, do)

        res.append(do)
        # set to None so won't copy again later
        feed_attrs[ix] = None
        data_object_attrs[jx] = None

    # add non-mapped feed_attrs & data_object_attrs
    for fa in feed_attrs:
        if not fa:
            continue
        do = {}
        util.copy_keys(
            {
                "ZZ_FEED_ATTRIBUTE_ID": "FEED_ATTRIBUTE_ID",
                "ATTRIBUTE_NAME": "FEED_ATTRIBUTE_NAME",
                "ATTRIBUTE_TYPE": "FEED_ATTRIBUTE_TYPE",
                "ATTRIBUTE_NO": "FEED_ATTRIBUTE_NO",
            }, fa, do)
        res.append(do)

    for doa in data_object_attrs:
        if not doa:
            continue
        do = {}
        util.copy_keys(
            {
                "DATA_OBJECT_ATTRIBUTE_ID": "ZZ_DATA_OBJECT_ATTRIBUTE_ID",
                "ATTRIBUTE_NO": "DATA_OBJECT_ATTRIBUTE_NO",
                "ATTRIBUTE_NAME": "DATA_OBJECT_ATTRIBUTE_NAME",
                "ATTRIBUTE_TYPE": "DATA_OBJECT_ATTRIBUTE_TYPE",
            }, doa, do)
        res.append(do)
    return res


def read_table_transformation_by_feed_id_data_object_id(
    feed_id, data_object_id
):
    feeds = rd.feeds.filter_entries("ZZ_FEED_ID", feed_id)
    if not feeds:
        return {}

    feed_data_objects, _ = rd.feed_data_objects.filter_entries_multi(
        [
            ["DATA_OBJECT_ID", data_object_id],
            ["FEED_ID", feeds[0]["ZZ_FEED_ID2"]]
        ]
    )
    if not feed_data_objects:
        return {}

    res = {}
    util.copy_keys(
        {
            "SRC_FILTER_SQL": "SRC_FILTER_SQL",
            "TRANSFORM_SQL_QUERY": "TRANSFORM_SQL_QUERY",
        }, feed_data_objects[0], res
    )
    return res


def save_feed(feed_id, data):
    if feed_id == "NEW_FEED":
        new_feed = {
            "SOURCE_SYSTEM": data.get("new_feed_sourcesystem"),
            "FEED_NAME": data.get("new_feed_name"),
            "FEED_FILE_TYPE": data.get("new_feed_filetype"),
            "DB_NAME": data.get("new_feed_dbname"),
        }
        rd.feeds.add_entry(new_feed)

        rd.feed_attributes.add_entries(
            create_feed_attributes(new_feed, data)
        )
    else:
        feed = {
            "SOURCE_SYSTEM": feed_id["SOURCE_SYSTEM"],
            "FEED_NAME": feed_id["FEED_NAME"],
        }
        feed_attrs = create_feed_attributes(feed, data)
        rd.feed_attributes.delete_entries(
            [
                ["FEED_ID", feed],
            ]
        )
        rd.feed_attributes.add_entries(feed_attrs)

    rd.feeds.dump()
    rd.feed_attributes.dump()


def create_feed_attributes(values: dict, data):
    field_map = {
        "ATTRIBUTE_NAME": ["ATTRIBUTE_NAME", str],
        "ATTRIBUTE_NO": ["ATTRIBUTE_NO", int],
        "ATTRIBUTE_TYPE": ["ATTRIBUTE_TYPE", str],
        "PRIMARY_KEY_IND": ["PRIMARY_KEY_IND", str],
        "NULLABLE_IND": ["NULLABLE_IND", str],
        "ATTRIBUTE_LENGTH": ["ATTRIBUTE_LENGTH", int],
        "NESTED_ATTRIBUTE_TYPE": ["NESTED_ATTRIBUTE_TYPE", str],
        "NESTED_ATTRIBUTE_PATH": ["NESTED_ATTRIBUTE_PATH", str],
    }
    feed_attrs = []

    for row in data["feed_attributes"]:
        new_feed_attribute = ordereddict()
        new_feed_attribute["FEED_ID"] = util.create_yaml_map(
            **util.build_dict_value_from_keys(
                values, ["SOURCE_SYSTEM", "FEED_NAME"]
            )
        )
        for incoming, existing in field_map.items():
            if row.get(incoming):
                new_feed_attribute[existing[0]] = existing[1](row[incoming])
        feed_attrs.append(new_feed_attribute)
    return feed_attrs


def save_data_object(data_object_id, data):
    if data_object_id == "NEW_DATA_OBJECT":
        new_data_object = {
            "DATA_OBJECT_NAME": data.get("new_data_object_name"),
            "TGT_DB_NAME": data.get("new_data_object_dbname"),
        }
        rd.data_objects.add_entry(new_data_object)
        rd.data_object_attributes.add_entries(
            create_data_object_attributes(new_data_object, data)
        )
    else:
        data_object = {
            "DATA_OBJECT_NAME": data_object_id["DATA_OBJECT_NAME"],
            "TGT_DB_NAME": data_object_id["TGT_DB_NAME"],
        }
        data_object_attrs = create_data_object_attributes(data_object, data)
        rd.data_object_attributes.delete_entries(
            [
                ["DATA_OBJECT_ID", data_object]
            ]
        )
        rd.data_object_attributes.add_entries(data_object_attrs)

    rd.data_objects.dump()
    rd.data_object_attributes.dump()


def create_data_object_attributes(values: dict, data):
    field_map = {
        "ATTRIBUTE_NAME": ["ATTRIBUTE_NAME", str],
        "ATTRIBUTE_NO": ["ATTRIBUTE_NO", int],
        "ATTRIBUTE_TYPE": ["ATTRIBUTE_TYPE", str],
        "PRIMARY_KEY_IND": ["PRIMARY_KEY_IND", str],
    }
    data_object_attrs = []

    for row in data["data_object_attributes"]:
        new_data_object_attr = ordereddict()
        new_data_object_attr["DATA_OBJECT_ID"] = util.create_yaml_map(
            **util.build_dict_value_from_keys(
                values, ["DATA_OBJECT_NAME", "TGT_DB_NAME"]
            )
        )
        for incoming, existing in field_map.items():
            if row.get(incoming):
                new_data_object_attr[existing[0]] = existing[1](row[incoming])

        data_object_attrs.append(new_data_object_attr)
    return data_object_attrs


def save_transformation(feed_id, data_object_id, data):
    # feed_attr_data_object_attr
    rd.feed_attr_data_object_attr.delete_entries(
        [
            ["ZZ_FEED_ID", feed_id],
            ["ZZ_DATA_OBJECT_ID", data_object_id],
        ]
    )
    rd.feed_attr_data_object_attr.add_entries(
        create_feed_attr_data_object_attrs(data)
    )
    rd.feed_attr_data_object_attr.dump()

    # feed_data_object
    rd.feed_data_objects.delete_entries(
        [
            ["FEED_ID", feed_id],
            ["DATA_OBJECT_ID", data_object_id]
        ]
    )

    rd.feed_data_objects.add_entries(
        create_feed_data_object(feed_id, data_object_id, data)
    )
    rd.feed_data_objects.dump()


def create_feed_data_object(feed_id, data_object_id, data):

    field_map = {
        "src_filter_sql": ["SRC_FILTER_SQL", "str"],
        "transform_sql_query": ["TRANSFORM_SQL_QUERY", "str"],
    }

    feed_data_obj = ordereddict()
    complete_feed, _ = rd.feeds.filter_entries_multi(
        [
            ["SOURCE_SYSTEM", feed_id["SOURCE_SYSTEM"]],
            ["FEED_NAME", feed_id["FEED_NAME"]],
        ]
    )
    complete_feed = complete_feed[0]
    feed_data_obj["FEED_ID"] = util.create_yaml_map(
        **util.build_dict_value_from_keys(
            complete_feed, ["FEED_NAME", "DB_NAME"]
        )
    )
    feed_data_obj["DATA_OBJECT_ID"] = util.create_yaml_map(
        **util.build_dict_value_from_keys(
            data_object_id, ["DATA_OBJECT_NAME", "TGT_DB_NAME"]
        )
    )
    for incoming, existing in field_map.items():
        if data.get(incoming):
            feed_data_obj[existing[0]] = existing[1](data[incoming])

    return feed_data_obj


def create_feed_attr_data_object_attrs(data):
    res = []
    for row in data["attribute_mappings"]:
        new_attr = ordereddict()
        new_attr["FEED_ATTRIBUTE_ID"] = util.create_yaml_map(
            **util.flatten_dict(row["FEED_ATTRIBUTE_ID"])
        )
        new_attr["DATA_OBJECT_ATTRIBUTE_ID"] = util.create_yaml_map(
            **util.flatten_dict(row["DATA_OBJECT_ATTRIBUTE_ID"])
        )
        new_attr["TRANSFORM_FN"] = row.get("TRANSFORM_FN")
        res.append(new_attr)
    return res
