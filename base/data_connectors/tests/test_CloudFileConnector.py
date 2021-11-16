import os
import shutil
import pytest
import pandas as pd
from pathlib import Path
from decouple import config
from pandas.util.testing import assert_frame_equal
from ..CloudFileConnector import AzureBlobStorage

BLOB_SECRETS = {
    "account_url": "https://ptazsg5dspmlstrg01.blob.core.windows.net",
    "account_name": "ptazsg5dspmlstrg01",
    "key": os.getenv("BLOB_KEY") or config("BLOB_KEY"),  # Please request for password from DS-Frameworks team
}


@pytest.fixture
def sample_dataframe():
    d = {'col1': [1.2, 2.3], 'col2': [3.1, 4.6], 'col3': ['a', 'b']}
    return pd.DataFrame(data=d)


@pytest.mark.first
def test_save_blob(sample_dataframe):

    # Writing a dataset locally for testing purposes
    tmp_folder = "./tmp/"
    Path(tmp_folder).mkdir(parents=True, exist_ok=True)
    tmp_local_file_dir = Path(tmp_folder, "sample_dataframe.csv")
    sample_dataframe.to_csv(tmp_local_file_dir, index=False)

    # Establishing connection to AzureBlobStorage
    azure_connector = AzureBlobStorage(account_url=BLOB_SECRETS['account_url'],
                                       key=BLOB_SECRETS['key'])

    # Pushing local file to Azure blob
    azure_connector.save(local_file_to_upload=tmp_local_file_dir,
                         container_name='tests',
                         blob_name='sample_dataframe.csv')

    # Deleting local file
    shutil.rmtree(tmp_folder)

    # Check if local file is pushed successfully to Azure blob
    assert azure_connector.check_exists(container_name='tests', blob_name='sample_dataframe.csv'), \
        "File was not pushed successfully to Azure Blob"


@pytest.mark.order2
def test_read_blob(sample_dataframe):

    # Establishing connection to AzureBlobStorage
    azure_connector = AzureBlobStorage(account_url=BLOB_SECRETS['account_url'],
                                       key=BLOB_SECRETS['key'])

    # Loading data from AzureBlobStorage
    blob_df = azure_connector.load(container_name='tests',
                                   blob_name='sample_dataframe.csv')

    # Checking if data is the same
    assert_frame_equal(blob_df, sample_dataframe)


@pytest.mark.order2
def test_download_blob(sample_dataframe):

    # Establishing connection to AzureBlobStorage
    azure_connector = AzureBlobStorage(account_url=BLOB_SECRETS['account_url'],
                                       key=BLOB_SECRETS['key'])

    # Downloading blob file locally
    tmp_folder = "./tmp/"
    Path(tmp_folder).mkdir(parents=True, exist_ok=True)
    azure_connector.download(local_filepath_to_download=Path(tmp_folder),
                             container_name='tests',
                             blob_name='sample_dataframe.csv')

    # Check if file is downloaded properly
    assert 'sample_dataframe.csv' in os.listdir(tmp_folder), \
        f"File sample_dataframe.csv was not downloaded to {tmp_folder}"

    # If file is downloaded, check if data matches
    blob_df = pd.read_csv(Path(tmp_folder, "sample_dataframe.csv"))

    # Both tables should match
    assert_frame_equal(blob_df, sample_dataframe)

    # Deleting local file
    shutil.rmtree(tmp_folder)


@pytest.mark.order3
def test_delete_blob():

    # Establishing connection to AzureBlobStorage
    azure_connector = AzureBlobStorage(account_url=BLOB_SECRETS['account_url'],
                                       key=BLOB_SECRETS['key'])

    # Delete blob
    azure_connector.delete(container_name='tests',
                           blob_name='sample_dataframe.csv')

    # Check if blob still exists
    assert not azure_connector.check_exists(container_name='tests', blob_name='sample_dataframe.csv'), \
        f"File was not deleted from blob successfully, file still exists"
