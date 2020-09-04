import util
from typing import List
from ruamel.yaml import YAML
import logging
import os
import arrow
import copy


yaml = YAML()
yaml.preserve_quotes = True
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
        self.key_name = f"{table_name}_ID".upper()
        self.keys = keys
        self.from_yaml = None
        self.entries = None
        self.refresh()

    def refresh(self):
        self.from_yaml, self.entries = self.read_from_yaml(
            self.table_name, self.keys
        )
        self.build_composite_key()

    def read_from_yaml(self, table_name: str, keys: List):
        logging.info("loading yaml... " + table_name)
        data_records = []

        from_yaml = read_metadata_yaml(table_name)
        for r in from_yaml["data_object"]["data_records"]:
            row = r["row"]
            record = {}
            # record[self.key_name] = {}
            # for k in keys:
            #     if isinstance(row[k], dict):
            #         for key, val in row[k].items():
            #             record[self.key_name][key] = val
            #     else:
            #         record[self.key_name][k] = row[k]
            for k, v in row.items():
                record[k] = v

            data_records.append(record)
        return from_yaml, data_records

    def build_composite_key(self):
        for r in self.entries:
            r[self.key_name] = {
                k: r[k]
                for k in self.keys
            }

    def add_entry(self, entry):
        self.entries.append(entry)
        self.build_composite_key()

    def add_entries(self, entries):
        self.entries.extend(entries)
        self.build_composite_key()

    def delete_entries(self, compare_field: str, compare_value: dict):
        res = []
        for x in self.entries:
            if x[compare_field] == compare_value:
                continue
            res.append(x)
        self.entries = res

    def filter_entries(self, filter_by, filter_val, order_by):
        res = []
        for x in self.entries:
            if x[filter_by] == filter_val:
                res.append(x)
        res.sort(key=lambda x: x[order_by])
        return res

    def dump(self):
        filepath = os.path.join(
            util.metadata_path(),
            f"temp_{self.table_name}.yaml",
        )
        entries_no_id = copy.deepcopy(self.entries)
        id_col = f"{self.table_name.upper()}_ID"
        for e in entries_no_id:
            if id_col in e:
                del e[id_col]
        self.from_yaml['data_object']['data_records'] = [
            {"row": e}
            for e in entries_no_id
        ]
        with open(filepath, 'w') as f:
            yaml.dump(self.from_yaml, f)


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
        print(f"finished refreshing all yamls...{arrow.now()}")


