import os
from .. import YAMLFileConnector


def test_yaml():

    # Test saving of yaml files
    sample_data = {"name": "John", "age": 31, "city": "New York"}
    YAMLFileConnector.save(sample_data, 'tmp.yml')

    # Test loading of yaml files
    try:
        yaml_file = YAMLFileConnector.load('tmp.yml')
        assert yaml_file, "YAML file was not loaded successfully."

    finally:
        # Clearing temporary files
        os.remove('tmp.yml')


