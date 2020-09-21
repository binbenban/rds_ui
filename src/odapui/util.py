import oyaml as yaml
import os
from ruamel.yaml.comments import CommentedMap as ordereddict
from ruamel.yaml.scalarstring import SingleQuotedScalarString as sq
from ruamel.yaml.scalarstring import DoubleQuotedScalarString as dq
import typing


config = None


def get_project_path():
    return os.path.abspath(os.path.dirname(__file__))


def read_config_file():
    global config
    config_file = os.path.join(
        get_project_path(), "config.yaml"
    )
    with open(config_file) as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


def get_config_param(parameter):
    global config
    if not config:
        read_config_file()
    return config[parameter]


def metadata_path() -> str:
    res = get_config_param("metadata_path")
    if not res:
        raise ValueError("Cannot find metadata_path in config")
    return res


def format_entry(
    e: any, no_quote_fields: list, double_quote_fields: list
) -> any:
    # max 2 layers
    if isinstance(e, str):
        return sq(e)
    res = ordereddict()
    for k1, v1 in e.items():
        if isinstance(v1, ordereddict):
            res[k1] = v1
        elif isinstance(v1, dict):
            # break up v1
            temp1 = ordereddict()
            for k2, v2 in v1.items():
                if k2 in no_quote_fields:
                    temp1[sq(k2)] = v2
                elif k2 in double_quote_fields:
                    temp1[sq(k2)] = dq(v2)
                else:
                    temp1[sq(k2)] = sq(v2)
            temp1.fa.set_flow_style()  # one line
            if isinstance(v1, ordereddict):
                temp1.ca.comment = v1.ca.comment
            res[k1] = temp1
        elif v1:
            if k1 in no_quote_fields:
                res[k1] = v1
            elif k1 in double_quote_fields:
                res[k1] = dq(v1)
            else:
                res[k1] = sq(v1)
    res.ca.comment = e.ca.comment  # preserve comments
    return res


def build_dict_value_from_keys(items: dict, keys: typing.List[str]) -> dict:
    return {k: items[k] for k in keys}


def copy_keys(keys: dict, source_dict, target_dict):
    for ks, kt in keys.items():
        target_dict[kt] = source_dict.get(ks)
