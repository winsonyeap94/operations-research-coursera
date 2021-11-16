"""
Dataset connector script includes PandasFileConnector, DatabaseFileConnector, APIFileConnector, CloudFileConnector, YAMLFileConnector
"""

import os
import re
import yaml

from ..common import loguru_logger


class YAMLFileConnector:

    _logger = loguru_logger

    env_pattern = re.compile(r".*?\${(.*?)}.*?")

    @classmethod
    def load(cls, filepath, **kwargs):
        """
        Read a YAML file and return the list of dictionaries.

        Args:
            filepath ([str]): [filepath]
        Returns:
            dict_file ([dict]): [list of dictionaries]
        """

        with open(filepath, 'r') as file:
            try:
                yaml.add_implicit_resolver("!pathex", cls.env_pattern, None, yaml.SafeLoader)
                yaml.add_constructor("!pathex", cls.env_constructor, yaml.SafeLoader)
                data_dict = yaml.safe_load(file, **kwargs)
                cls._logger.info(f"[YAMLFileConnector] YAML file ({filepath}) loaded successfully.")

                return data_dict
            except Exception as error:
                return error

    @classmethod
    def save(cls, data_dict, filepath, **kwargs):
        """
        Save out dictionary file as yaml file format.

        Args:
            data_dict ([dict]): [list of dictionaries to be saved out as yaml file]
            filepath ([str]): [filepath]
        """

        try:
            with open(filepath, 'w') as file:
                yaml.dump(data_dict, file, sort_keys=False, **kwargs)
            cls._logger.info(f"[YAMLFileConnector] List of dictionaries saved as YAML file successfully to {filepath}.")

        except Exception as error:
            return error

    @classmethod
    def env_constructor(cls, loader, node):
        value = loader.construct_scalar(node)
        for group in cls.env_pattern.findall(value):
            value = value.replace(f"${{{group}}}", os.environ.get(group))
        return value


if __name__ == "__main__":

    yaml_file = YAMLFileConnector.load('data/testing.yaml')
    YAMLFileConnector.save(yaml_file, 'data/testing_sub.yaml')
    print(yaml_file)
