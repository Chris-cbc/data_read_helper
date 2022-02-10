import yaml


def load_config_file(file_path):
    with open(file_path) as f:
        return yaml.safe_load(f)
