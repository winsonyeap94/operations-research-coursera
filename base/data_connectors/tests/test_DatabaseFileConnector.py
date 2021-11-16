import os
import pytest
import numpy as np
import pandas as pd
from decouple import config
from pandas.util.testing import assert_frame_equal
from ..DatabaseFileConnector import DatabaseConnector

DB_SECRETS = {
    "db_type": "postgresql",
    "host": "ptsg-5dsppsql01.postgres.database.azure.com",
    "port": 5432,
    "database": "pymlframework_db",
    "username": "ds_framework@ptsg-5dsppsql01",
    "password": os.getenv("DB_PASSWORD") or config("DB_PASSWORD")  # Please request for password from DS-Frameworks team
}


@pytest.fixture
def sample_dataframe():
    d = {'col1': [1.2, 2.3], 'col2': [3.1, 4.6], 'col3': ['a', 'b']}
    return pd.DataFrame(data=d)


@pytest.mark.first
def test_write_read_table(sample_dataframe):

    # Writing data to SQL
    db_connector = DatabaseConnector(**DB_SECRETS)
    db_connector.save(sample_dataframe, "pytest_db", if_exists='replace')

    # Checking if SQL table exists
    sql_df = db_connector.load("pytest_db")
    sql_df = sql_df[sample_dataframe.columns]

    # Both tables should match
    assert_frame_equal(sql_df, sample_dataframe, check_dtype=False)

    # ID should start with 1, 2
    assert sql_df['id'].equals(pd.Series([1, 2])), "`id` column is not saved correctly"


@pytest.mark.order2
def test_update_table():

    db_connector = DatabaseConnector(**DB_SECRETS)

    # Randomising data
    sample_dataframe = db_connector.load("pytest_db")
    sample_dataframe['col1'] = np.random.rand(sample_dataframe.shape[0])
    sample_dataframe['col2'] = np.random.rand(sample_dataframe.shape[0])
    sample_dataframe['col3'] = ['c', 'd']

    # Updating data in SQL table
    db_connector.update_table(sample_dataframe, "pytest_db", update_based_on='id')

    # Checking if SQL table is updated properly
    sql_df = db_connector.load("pytest_db")
    sql_df = sql_df[sample_dataframe.columns]

    # Both tables should match
    assert_frame_equal(sql_df, sample_dataframe, check_dtype=False)

    # ID should start with 1, 2
    assert sql_df['id'].equals(pd.Series([1, 2])), "`id` column is not saved correctly"


@pytest.mark.order3
def test_execute_statement():

    db_connector = DatabaseConnector(**DB_SECRETS)

    # Get latest id from SQL table
    latest_id = db_connector.execute_statement("""
    SELECT MAX("id") FROM pytest_db
    """)

    assert latest_id is not None, f"execute_statement did not return expected results, got --> {latest_id}"


@pytest.mark.last
def test_delete_table():

    db_connector = DatabaseConnector(**DB_SECRETS)
    db_connector.execute_statement("DROP TABLE pytest_db", expect_output=False)

    # Check if table still exists
    db_table_names = db_connector._create_engine().table_names()
    assert 'pytest_db' not in db_table_names, f"Table not deleted successfully, tables found: {db_table_names}"
