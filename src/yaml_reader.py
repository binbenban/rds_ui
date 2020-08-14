import util
from typing import List


def read_from_yaml(table_name: str, keys: List):
    res = []
    key_name = f"{table_name}_ID".upper()

    from_yaml = util.read_metadata_yaml(table_name)
    for r in from_yaml["data_object"]["data_records"]:
        row = r["row"]
        record = {}
        record[key_name] = {}
        for k in keys:
            if isinstance(row[k], dict):
                for key, val in row[k].items():
                    record[key_name][key] = val
            else:
                record[key_name][k] = row[k]

        for k, v in row.items():
            record[k] = v
        res.append(record)
    return res


class Table:
    def __init__(self, table_name: str, keys: List[str]):
        self.table_name = table_name
        self.keys = keys
        self.entries = self.refresh()

    def refresh(self):
        entries = read_from_yaml(self.table_name, self.keys)
        return entries


class Reader:
    def __init__(self):
        self.feeds = None
        self.data_objects = None
        self.feed_attributes = None
        self.data_object_attributes = None
        self.feed_attr_data_object_attr = None
        self.feed_data_objects = None
        self.refresh()

    def refresh(self, force=True):
        self.feeds = Table("feed", ["SOURCE_SYSTEM", "FEED_NAME"])
        self.data_objects = Table(
            "data_object", ["DATA_OBJECT_NAME", "TGT_DB_NAME"])
        self.feed_attributes = Table(
            "feed_attribute", ["FEED_ID", "ATTRIBUTE_NAME"])
        self.data_object_attributes = Table(
            "data_object_attribute", ["DATA_OBJECT_ID", "ATTRIBUTE_NAME"])
        self.feed_attr_data_object_attr = Table(
            "feed_attr_data_object_attr",
            ["FEED_ATTRIBUTE_ID", "DATA_OBJECT_ATTRIBUTE_ID"])
        self.feed_data_objects = Table(
            "feed_data_object", ["FEED_ID", "DATA_OBJECT_ID"])

    def read_data_object_attributes_by_data_object_id(
        self, data_object_id: str
    ):
        res = []
        for x in self.data_object_attributes.entries:
            if x["DATA_OBJECT_ID"] == data_object_id:
                res.append(x)
        res.sort(key=lambda x: x["ATTRIBUTE_NO"])
        return res

    def read_feed_attributes_by_feed_id(self, feed_id: dict):
        res = []
        for x in self.feed_attributes.entries:
            if x["FEED_ID"] == feed_id:
                res.append(x)
        res.sort(key=lambda x: x["ATTRIBUTE_NO"])
        return res
