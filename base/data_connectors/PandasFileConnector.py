"""
PandasFileConnector script includes loading and writing to file formats for csv, excel, feather, json, txt,
pickle, parquet.
"""

import json
import pandas as pd
from pathlib import Path

from ..common import loguru_logger


class PandasFileConnector:

    _logger = loguru_logger

    @classmethod
    def load(cls, filepath, file_type=None, **kwargs):
        """
        Different load methods for respective file format type.

        Args:
            filepath ([str]): [filepath]
            file_type ([str]): [type of files: {'.csv', '.xlsx', '.json', '.txt', '.pkl', '.yaml', '.parquet'}]
            **kwargs ([dict]): [dictionary of extra arguments]

        Returns:
            data_df ([dataframe]): [loaded data]
        """

        try:
            cls._logger.debug(f"[PandasFileConnector] Data loading ({filepath}) initiated...")
            file_type = file_type or cls._check_filetype(filepath)
            file_type = file_type if file_type.startswith('.') else '.' + file_type
            pd_connector = cls._get_connector(file_type)
            data_df = pd_connector.load(filepath=filepath, **kwargs)
            cls._logger.info(f"[PandasFileConnector] Data loaded ({filepath}) successfully.")
            return data_df

        except Exception as error:
            cls._logger.exception(f"[PandasFileConnector] load error: {error}")

    @classmethod
    def save(cls, data_df, filepath, file_type=None, **kwargs):
        """
        Different save methods for respective file format type.

        Args:
            data_df ([dataframe]): [data or table to be saved out]
            filepath ([str]): [file path to save out the dataframe]
            file_type ([str]): [type of files: {'csv', 'xlsx', 'json', 'txt', 'pkl', 'yaml', 'parquet'}]
            **kwargs ([dict]): [dictionary of extra arguments]
        """

        try:
            cls._logger.debug(f"[PandasFileConnector] Data saving ({filepath}) initiated...")
            file_type = file_type or cls._check_filetype(filepath)
            pd_connector = cls._get_connector(file_type)
            pd_connector.save(data_df, filepath, **kwargs)
            cls._logger.info(f"[PandasFileConnector] Data saved ({filepath}) successfully.")

        except Exception as error:
            cls._logger.exception(f"[PandasFileConnector] save error: {error}")

    @staticmethod
    def _connector_list():
        file_connectors = {
            '.csv': CSVFileConnector,
            '.xlsx': ExcelFileConnector,
            '.feather': FeatherFileConnector,
            '.json': JSONFileConnector,
            '.txt': TxtFileConnector,
            '.pkl': PickleFileConnector,
            '.pickle': PickleFileConnector,
            '.parquet': ParquetFileConnector
        }
        return file_connectors

    @classmethod
    def _check_filetype(cls, filepath):
        file_extension = Path(filepath).suffix
        cls._logger.debug(f"[_check_filetype] File extension detected as {file_extension}")
        file_connectors = cls._connector_list()
        assert file_extension in file_connectors.keys(), \
            f"File extension ({file_extension}) not recognised. Only accept .csv, .xlsx, .txt, .json, " \
            f".feather, .pkl, .parquet"
        return file_extension

    @classmethod
    def _get_connector(cls, file_type):
        file_connectors = cls._connector_list()
        assert file_type in file_connectors.keys(), \
            f"File extension ({file_type}) not recognised. Only accept .csv, .xlsx, .txt, .json, .feather, " \
            f".pkl, .parquet"
        return cls._connector_list()[file_type]


class CSVFileConnector:

    @classmethod
    def load(cls, filepath, **kwargs):
        """
        Load csv file as dataframe.

        Args:
            filepath ([str]): [filepath]
            **kwargs ([dict]): [dictionary of extra arguments]
        Returns:
            data_df ([dataframe]): [loaded dataframe]
        """
        data_df = pd.read_csv(filepath, **kwargs)
        return data_df

    @classmethod
    def save(cls, data_df, filepath, **kwargs):
        """
        Save dataframe as csv file.

        Args:
            data_df ([dataframe]): [data to be saved out as csv file]
            filepath ([str]): [filepath]
            **kwargs ([dict]): [dictionary of extra arguments]
        """
        data_df.to_csv(filepath, **kwargs)


class ExcelFileConnector:

    @classmethod
    def load(cls, filepath, **kwargs):
        """
        Load xlsx excel file as dataframe.

        Args:
            filepath ([str]): [filepath]
            **kwargs ([dict]): [dictionary of extra arguments]
        Returns:
            data_df ([dataframe]): [loaded dataframe]
        """
        data_df = pd.read_excel(filepath, **kwargs)
        return data_df

    @classmethod
    def save(cls, data_df, filepath, **kwargs):
        """
        Save dataframe as csv file.

        Args:
            data_df ([dataframe]): [data to be saved out as excel file]
            filepath ([str]): [filepath]
            **kwargs ([dict]): [dictionary of extra arguments]
        """
        data_df.to_excel(filepath, **kwargs)


class FeatherFileConnector:

    @classmethod
    def load(cls, filepath, **kwargs):
        """
        Read a feather file as a dataframe.

        Args:
            filepath ([str]): [filepath]
            **kwargs ([dict]): [dictionary of extra arguments]
        Returns:
            data_df ([dataframe]): [loaded dataframe]
        """
        data_df = pd.read_feather(filepath, **kwargs)
        return data_df

    @classmethod
    def save(cls, data_df, filepath, **kwargs):
        """
        Save out dataframe as feather file format.

        Args:
            data_df ([dataframe]): [data to be saved out as feather file]
            filepath ([str]): [filepath]
            **kwargs ([dict]): [dictionary of extra arguments]
        """
        data_df.to_feather(filepath, **kwargs)


class JSONFileConnector:

    @classmethod
    def load(cls, filepath, **kwargs):
        """
        Load json excel file as dataframe.

        Args:
            filepath ([str]): [filepath]
            **kwargs ([dict]): [dictionary of extra arguments]
        Returns:
            data_df ([dataframe]): [loaded dataframe]
        """
        with open(filepath, mode='r') as fs_file:
            data_df = json.load(fs_file, **kwargs)
            return data_df

    @classmethod
    def save(cls, data_df, filepath, orient='records', **kwargs):
        """
        Save out dataframe as json file format.

        Args:
            data_df ([dataframe]): [data to be saved out as json file]
            filepath ([str]): [filepath]
            orient ([str]): [orient method for pd.DataFrame.to_dict()]
            **kwargs ([dict]): [dictionary of extra arguments]
        """
        if type(data_df) is pd.DataFrame:
            json_data = data_df.to_dict(orient=orient)
        else:
            json_data = data_df
        with open(filepath, 'w') as file:
            json.dump(json_data, file, indent=4, **kwargs)


class TxtFileConnector:

    @classmethod
    def load(cls, filepath, sep=' ', **kwargs):
        """
        Read a text file as a dataframe.

        Args:
            filepath ([str]): [filepath]
            sep ([str]): [text file column separator]
            **kwargs ([dict]): [dictionary of extra arguments]
        Returns:
            data_df ([dataframe]): [loaded dataframe]
        """
        with open(filepath, mode='r') as fs_file:
            data_df = pd.read_csv(fs_file, sep=sep, **kwargs)
            return data_df

    @classmethod
    def save(cls, data_df, filepath, sep=' ', **kwargs):
        """
        Save out dataframe as text file format.

        Args:
            data_df ([dataframe]): [data to be saved out as text file]
            filepath ([str]): [filepath]
            sep ([str]): [text file column separator]
            **kwargs ([dict]): [dictionary of extra arguments]
        """
        data_df.to_csv(filepath, sep=sep, **kwargs)


class PickleFileConnector:

    @classmethod
    def load(cls, filepath, **kwargs):
        """
        Read a pickle file as a dataframe.

        Args:
            filepath ([str]): [filepath]
            **kwargs ([dict]): [dictionary of extra arguments]
        Returns:
            data_df ([dataframe]): [loaded dataframe]
        """
        with open(filepath, 'rb') as file:
            data_df = pd.read_pickle(file, **kwargs)
            return data_df

    @classmethod
    def save(cls, data_df, filepath, **kwargs):
        """
        Save out dataframe as pickle file format, the maximum file size of pickle is about 2GB.

        Args:
            data_df ([dataframe]): [data to be saved out as pickle file]
            filepath ([str]): [filepath]
            **kwargs ([dict]): [dictionary of extra arguments]
        """
        data_df.to_pickle(filepath, **kwargs)


class ParquetFileConnector:

    @classmethod
    def load(cls, filepath, **kwargs):
        """
        Read a parquet file as a dataframe.

        Args:
            filepath ([str]): [filepath]
            **kwargs ([dict]): [dictionary of extra arguments]
        Returns:
            data_df ([dataframe]): [loaded dataframe]
        """
        return pd.read_parquet(filepath, **kwargs)

    @classmethod
    def save(cls, data_df, filepath, **kwargs):
        """
        Save out dataframe as parquet file format.

        Args:
            data_df ([dataframe]): [list of dictionaries to be saved out as parquet file]
            filepath ([str]): [filepath]
        """
        return data_df.to_parquet(filepath, **kwargs)
