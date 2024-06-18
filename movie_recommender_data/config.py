import os
from typing import Any

import yaml


def get_dev_db_params(
    config_file: str = "config.yaml", config_secret: str = None
) -> dict[str, Any]:
    with open(config_file, "r") as fp_config:
        config = yaml.safe_load(fp_config)
    db_params_dict = config["database"]["dev"]

    if config_secret is not None:
        with open(config_secret, "r") as fp_secret_config:
            config_secret_dict = yaml.safe_load(fp_secret_config)
        password = config_secret_dict["database"]["dev"]["password"]
    else:
        password = os.environ["DATABASE_PASSWORD"]
    db_params_dict["password"] = password

    return db_params_dict


def get_dev_25M_db_params(
    config_file: str = "config.yaml", config_secret: str = None
) -> dict[str, Any]:
    with open(config_file, "r") as fp_config:
        config = yaml.safe_load(fp_config)
    db_params_dict = config["database"]["dev_25M"]

    if config_secret is not None:
        with open(config_secret, "r") as fp_secret_config:
            config_secret_dict = yaml.safe_load(fp_secret_config)
        password = config_secret_dict["database"]["dev"]["password"]
    else:
        password = os.environ["DATABASE_PASSWORD"]
    db_params_dict["password"] = password

    return db_params_dict
