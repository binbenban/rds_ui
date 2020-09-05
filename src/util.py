import oyaml as yaml
import os
import ruamel.yaml
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


def quote_yaml_element(s: any) -> any:
    if isinstance(s, str) and not s.isnumeric():
        return ruamel.yaml.scalarstring.SingleQuotedScalarString(s)
    else:
        return s


def create_yaml_map(**m):
    ret = ruamel.yaml.comments.CommentedMap(m)
    ret.fa.set_flow_style()
    return ret


def build_dict_value_from_keys(items: dict, keys: typing.List[str]) -> dict:
    return {k: items[k] for k in keys}


def flatten_dict(d):
    res = {}
    for k, v in d.items():
        if isinstance(v, dict):
            for ks, vs, in v.items():
                res[ks] = vs
        else:
            res[k] = v
    return res


def copy_keys(keys: dict, source_dict, target_dict):
    for ks, kt in keys.items():
        target_dict[kt] = source_dict.get(ks)


