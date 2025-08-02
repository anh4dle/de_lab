import os
import yaml


class ConfigLoader:
    def __init__(self, config_path):
        self.config_path = config_path

    def get_yaml_config_dict(self) -> dict:
        if self.config_path is None:
            raise ValueError(
                "LOCAL_CONFIG_PATH is not set in the environment.")
        with open(self.config_path, 'r') as f:
            raw_yaml = f.read()

        # Load the yaml with env
        filled_yaml = raw_yaml.format(**os.environ)
        config = yaml.safe_load(filled_yaml)

        return config
