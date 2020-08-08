import yaml
import os

config = None


def read_config_file():
    global config
    root_dir = os.path.abspath(os.path.dirname(__file__))
    config_file = os.path.join(root_dir, 'config.yaml')
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


def read_metadata_yaml(table_name: str) -> dict:
    parent_path = metadata_path()
    if parent_path[-1] == "/":
        parent_path = parent_path[:-1]
    path = f"{parent_path}/yaml/{table_name}.yaml"

    with open(path) as f:
        feed = yaml.load(f, Loader=yaml.FullLoader)
    return feed
