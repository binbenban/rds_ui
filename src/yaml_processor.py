from ruamel.yaml.comments import CommentedMap as ordereddict
import util
import typing


def _build_dict_value_from_keys(items: dict, keys: typing.List[str]) -> dict:
    return {k: items[k] for k in keys}


def create_feed(values: dict, data):
    new_feed = values.copy()
    new_feed["FEED_ID"] = _build_dict_value_from_keys(
        values, ["SOURCE_SYSTEM", "FEED_NAME"]
    )
    return new_feed


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
            **_build_dict_value_from_keys(
                values, ["SOURCE_SYSTEM", "FEED_NAME"]
            )
        )
        for incoming, existing in field_map.items():
            if row.get(incoming):
                new_feed_attribute[existing[0]] = existing[1](row[incoming])
        new_feed_attribute["FEED_ATTRIBUTE_ID"] = (
            _build_dict_value_from_keys(
                {**values, **new_feed_attribute},
                ["SOURCE_SYSTEM", "FEED_NAME", "ATTRIBUTE_NAME"],
            )
        )
        feed_attrs.append(new_feed_attribute)
    return feed_attrs


def create_data_object(values: dict, data):
    new_data_object = values.copy()
    new_data_object["DATA_OBJECT_ID"] = _build_dict_value_from_keys(
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
            **_build_dict_value_from_keys(
                values, ["DATA_OBJECT_NAME", "TGT_DB_NAME"]
            )
        )
        for incoming, existing in field_map.items():
            if row.get(incoming):
                new_data_object_attr[existing[0]] = existing[1](row[incoming])
        
        new_data_object_attr["DATA_OBJECT_ATTRIBUTE_ID"] = (
            _build_dict_value_from_keys(
                {**values, **new_data_object_attr},
                ["DATA_OBJECT_NAME", "TGT_DB_NAME", "ATTRIBUTE_NAME"],
            )
        )
        data_object_attrs.append(new_data_object_attr)
    return data_object_attrs
