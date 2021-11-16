import os
import pytest
import numpy as np
import pandas as pd
from ..PandasFileConnector import PandasFileConnector


@pytest.fixture
def sample_dataframe():
    d = {'col1': [1, 2], 'col2': [3, 4], 'col3': ['a', 'b']}
    return pd.DataFrame(data=d)


def test_csv(sample_dataframe):

    PandasFileConnector.save(sample_dataframe, "tmp.csv", index=False)

    try:
        data_df = PandasFileConnector.load("tmp.csv")
        assert data_df.equals(sample_dataframe), "CSV was not loaded successfully."

    finally:
        os.remove("tmp.csv")


def test_excel(sample_dataframe):

    PandasFileConnector.save(sample_dataframe, "tmp.xlsx", index=False)

    try:
        data_df = PandasFileConnector.load("tmp.xlsx")
        assert data_df.equals(sample_dataframe), "Excel was not loaded successfully."

    finally:
        os.remove("tmp.xlsx")


def test_json(sample_dataframe):

    PandasFileConnector.save(sample_dataframe, "tmp.json")

    try:
        data_df = PandasFileConnector.load("tmp.json")
        data_df = pd.DataFrame(data_df)
        assert data_df.equals(sample_dataframe), "JSON was not loaded successfully."

    finally:
        os.remove("tmp.json")


def test_text(sample_dataframe):

    PandasFileConnector.save(sample_dataframe, "tmp.txt", index=False)

    try:
        data_df = PandasFileConnector.load("tmp.txt")
        assert data_df.equals(sample_dataframe), "TXT was not loaded successfully."

    finally:
        os.remove("tmp.txt")


def test_feather():

    sample_dataframe = pd.DataFrame(np.random.randn(100000, 20))
    sample_dataframe.columns = ['X' + str(x) for x in sample_dataframe.columns.values]
    PandasFileConnector.save(sample_dataframe, "tmp.feather")

    try:
        data_df = PandasFileConnector.load("tmp.feather")
        assert data_df.equals(sample_dataframe), "Feather File was not loaded successfully."

    finally:
        os.remove("tmp.feather")


def test_parquet(sample_dataframe):

    PandasFileConnector.save(sample_dataframe, "tmp.parquet")

    try:
        data_df = PandasFileConnector.load("tmp.parquet")
        assert data_df.equals(sample_dataframe), "Parquet File was not loaded successfully."

    finally:
        os.remove("tmp.parquet")


def test_pickle(sample_dataframe):

    PandasFileConnector.save(sample_dataframe, "tmp.pkl")

    try:
        data_df = PandasFileConnector.load("tmp.pkl")
        assert data_df.equals(sample_dataframe), "Pickle File was not loaded successfully."

    finally:
        os.remove("tmp.pkl")


