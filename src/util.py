import ruamel.yaml as yaml
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
    print(config)
    return config[parameter]
