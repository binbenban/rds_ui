from ruamel.yaml.comments import CommentedMap as ordereddict
import util


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


def create_data_object(values: dict, data):
    new_data_object = values.copy()
    new_data_object["DATA_OBJECT_ID"] = util.build_dict_value_from_keys(
        values, ["DATA_OBJECT_NAME", "TGT_DB_NAME"]
    )
    return new_data_object


def create_data_object_attributes(values: dict, data):
    field_map = {
        "DATA_OBJECT_ATTRIBUTE_ID": ["DATA_OBJECT_ATTRIBUTE_ID", str],
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


def map_feed_data_object_attr(
        feed_id, data_object_id, feed_attrs, data_object_attrs, mapping):
    res = []

    # for each mapping, find details in feed_attrs & data_object_attrs
    for m in mapping:
        m_feed_id = util.build_dict_value_from_keys(
            m["FEED_ATTRIBUTE_ID"], ["SOURCE_SYSTEM", "FEED_NAME"]
        )
        m_data_object_id = util.build_dict_value_from_keys(
            m["DATA_OBJECT_ATTRIBUTE_ID"], ["DATA_OBJECT_NAME", "TGT_DB_NAME"]
        )

        if m_feed_id != feed_id or m_data_object_id != data_object_id:
            continue

        fa, doa = None, None
        for ix, fa_ix in enumerate(feed_attrs):
            if not fa_ix:
                continue
            if util.flatten_dict(fa_ix["FEED_ATTRIBUTE_ID"]) == \
                    m["FEED_ATTRIBUTE_ID"]:
                fa = fa_ix
                break
        if not fa:
            continue
        for jx, doa_jx in enumerate(data_object_attrs):
            if not doa_jx:
                continue
            if util.flatten_dict(doa_jx["DATA_OBJECT_ATTRIBUTE_ID"]) == \
                    m["DATA_OBJECT_ATTRIBUTE_ID"]:
                doa = doa_jx
                break
        if not doa:
            continue

        # do["DATA_OBJECT_ATTRIBUTE_ID"] = doa["DATA_OBJECT_ATTRIBUTE_ID"]
        # do["DATA_OBJECT_ATTRIBUTE_NO"] = doa["ATTRIBUTE_NO"]
        # do["DATA_OBJECT_ATTRIBUTE_NAME"] = doa["ATTRIBUTE_NAME"]
        # do["DATA_OBJECT_ATTRIBUTE_TYPE"] = doa["ATTRIBUTE_TYPE"]
        # do["TRANSFORM_FN"] = m.get("TRANSFORM_FN")

        do = {}
        util.copy_keys(
            {
                "FEED_ATTRIBUTE_ID": "FEED_ATTRIBUTE_ID",
                "ATTRIBUTE_NAME": "FEED_ATTRIBUTE_NAME",
                "ATTRIBUTE_TYPE": "FEED_ATTRIBUTE_TYPE",
                "ATTRIBUTE_NO": "FEED_ATTRIBUTE_NO",
            }, fa, do)
        util.copy_keys(
            {
                "DATA_OBJECT_ATTRIBUTE_ID": "DATA_OBJECT_ATTRIBUTE_ID",
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
                "FEED_ATTRIBUTE_ID": "FEED_ATTRIBUTE_ID",
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
                "DATA_OBJECT_ATTRIBUTE_ID": "DATA_OBJECT_ATTRIBUTE_ID",
                "ATTRIBUTE_NO": "DATA_OBJECT_ATTRIBUTE_NO",
                "ATTRIBUTE_NAME": "DATA_OBJECT_ATTRIBUTE_NAME",
                "ATTRIBUTE_TYPE": "DATA_OBJECT_ATTRIBUTE_TYPE",
            }, doa, do)
        res.append(do)

    return res


def search_feed_data_object(feed, feed_data_objects):
    feed_id_temp = util.build_dict_value_from_keys(
        feed, ["FEED_NAME", "DB_NAME"]
    )

    for r in feed_data_objects:
        if feed_id_temp == r["FEED_ID"]:
            do = {}
            util.copy_keys(
                {
                    "SRC_FILTER_SQL": "SRC_FILTER_SQL",
                    "TRANSFORM_SQL_QUERY": "TRANSFORM_SQL_QUERY",
                }, r, do
            )
            return do
    return {}
