import util
from typing import List
from ruamel.yaml import YAML
import logging
import os
import typing
import arrow
import copy


yaml = YAML()
yaml.preserve_quotes = True
yaml.width = 40096
yaml.indent(mapping=4, sequence=6, offset=4)

reader = None


def read_metadata_yaml(table_name: str) -> dict:
    parent_path = util.metadata_path()
    if parent_path[-1] == "/":
        parent_path = parent_path[:-1]
    path = f"{parent_path}/yaml/{table_name}.yaml"

    with open(path) as f:
        feed = yaml.load(f)
    return feed


def reader_instance():
    global reader
    if not reader:
        reader = Reader()
    return reader


class Table:
    def __init__(self, table_name: str, keys: List[str]):
        self.table_name = table_name
        self.key_name = f"ZZ_{table_name}_ID".upper()
        self.keys = keys
        self.from_yaml = None
        self.entries = None
        self.refresh()

    def refresh(self):
        self.from_yaml, self.entries = self.read_from_yaml(
            self.table_name, self.keys
        )
        self.build_columns()

    def read_from_yaml(self, table_name: str, keys: List):
        logging.info("loading yaml... " + table_name)
        data_records = []

        from_yaml = read_metadata_yaml(table_name)
        for r in from_yaml["data_object"]["data_records"]:
            row = r["row"]
            data_records.append(row)
        return from_yaml, data_records

    def build_columns(self):
        # by default build composite key
        for r in self.entries:
            r[self.key_name] = {
                k: r[k]
                for k in self.keys
            }

    def add_entry(self, entry):
        self.entries.append(entry)
        self.build_columns()

    def add_entries(self, entries):
        self.entries.extend(entries)
        self.build_columns()

    def delete_entries(self, conds: typing.List[typing.List]):
        met, not_met = self.filter_entries_multi(conds)
        self.entries = not_met

    def filter_entries(self, filter_by, filter_val, order_by=None):
        met, not_met = self.filter_entries_multi(
            [
                [filter_by, filter_val]
            ],
            order_by
        )
        return met

    def filter_entries_multi(
        self, conds: typing.List[typing.List], order_by=None
    ):
        met = []
        not_met = []
        for x in self.entries:
            keep = True
            for cond in conds:
                if x[cond[0]] != cond[1]:
                    keep = False
                    break
            if keep:
                met.append(x)
            else:
                not_met.append(x)
        if order_by:
            met.sort(key=lambda x: x[order_by])
        return met, not_met

    """
    name: "FEED_ID"
    vals: [["FEED_ATTR_ID", "FEED_NAME"], ["FEED_ATTR_ID", "SOURCE_SYSTEM"]]
    result: x["FEED_ID"] = {
        "FEED_NAME": x["FEED_ATTR_ID"]["FEED_NAME"],
        "SOURCE_SYSTEM": x["FEED_ATTR_ID"]["SOURCE_SYSTEM"],
    }
    """
    def add_element(self, name, vals: typing.List[typing.List[str]]):
        print(f"adding element {name}; {vals}")
        for ix, e in enumerate(self.entries):
            val = {}
            for strs in vals:
                sub_key = strs[-1]
                sub_val = e[strs[0]]
                for str in strs[1:]:
                    sub_val = sub_val[str]
                val[sub_key] = sub_val
            self.entries[ix][name] = copy.deepcopy(val)

    def dump(self):
        res = []
        for e in self.entries:
            temp_e = copy.deepcopy(e)
            # remove ZZ_ keys
            for k in e.keys():
                if "ZZ_" in k:
                    del temp_e[k]
            res.append(temp_e)

        filepath = os.path.join(
            util.metadata_path(),
            f"temp_{self.table_name}.yaml",
        )
        self.from_yaml['data_object']['data_records'] = [
            {"row": e}
            for e in res
        ]
        with open(filepath, 'w') as f:
            yaml.dump(self.from_yaml, f)


class Feeds(Table):
    def build_columns(self):
        Table.build_columns(self)
        self.add_element(
            "ZZ_FEED_ID2",
            [
                ["FEED_NAME"], ["DB_NAME"]
            ]
        )


class FeedAttributes(Table):
    def build_columns(self):
        Table.build_columns(self)
        self.add_element(
            "ZZ_FEED_ATTRIBUTE_ID_FLAT",
            [
                ["FEED_ID", "SOURCE_SYSTEM"],
                ["FEED_ID", "FEED_NAME"],
                ["ATTRIBUTE_NAME"]
            ]
        )


class DataObjectAttributes(Table):
    def build_columns(self):
        Table.build_columns(self)
        self.add_element(
            "ZZ_DATA_OBJECT_ATTRIBUTE_ID_FLAT",
            [
                ["DATA_OBJECT_ID", "DATA_OBJECT_NAME"],
                ["DATA_OBJECT_ID", "TGT_DB_NAME"],
                ["ATTRIBUTE_NAME"]
            ]
        )


class FeedAttrDataObjectAttrs(Table):
    def build_columns(self):
        Table.build_columns(self)
        self.add_element(
            "ZZ_FEED_ID",
            [
                ["FEED_ATTRIBUTE_ID", "FEED_NAME"],
                ["FEED_ATTRIBUTE_ID", "SOURCE_SYSTEM"],
            ]
        )
        self.add_element(
            "ZZ_DATA_OBJECT_ID",
            [
                ["DATA_OBJECT_ATTRIBUTE_ID", "DATA_OBJECT_NAME"],
                ["DATA_OBJECT_ATTRIBUTE_ID", "TGT_DB_NAME"],
            ]
        )


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
        print(f"start refreshing all yamls...{arrow.now()}")

        self.feeds = Feeds("feed", ["SOURCE_SYSTEM", "FEED_NAME"])
        self.data_objects = Table(
            "data_object", ["DATA_OBJECT_NAME", "TGT_DB_NAME"])
        self.feed_attributes = FeedAttributes(
            "feed_attribute", ["FEED_ID", "ATTRIBUTE_NAME"])
        self.data_object_attributes = DataObjectAttributes(
            "data_object_attribute", ["DATA_OBJECT_ID", "ATTRIBUTE_NAME"])
        self.feed_attr_data_object_attr = FeedAttrDataObjectAttrs(
            "feed_attr_data_object_attr",
            ["FEED_ATTRIBUTE_ID", "DATA_OBJECT_ATTRIBUTE_ID"])
        self.feed_data_objects = Table(
            "feed_data_object", ["FEED_ID", "DATA_OBJECT_ID"])

        print(f"finished refreshing all yamls...{arrow.now()}")
