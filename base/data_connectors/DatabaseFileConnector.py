"""
DatabaseFileConnector script includes reading and writing to Postgresql, MySQL, MSSQL.
"""

import getpass
import numpy as np
import pandas as pd
from platform import uname
from datetime import datetime
from sqlalchemy import create_engine

from ..common import loguru_logger


class DatabaseConnector:

    DEFAULT_SCHEMAS = {
        'postgresql': 'public',
        'mssql': 'dbo',
        'mysql': None,
    }

    def __init__(self, host: str, port: str, username: str, password: str, database: str,
                 db_type='postgresql', schema_name=None):

        self._logger = loguru_logger
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.db_type = db_type
        if self.db_type not in self.DEFAULT_SCHEMAS.keys():
            self._logger.exception("[DatabaseConnector] Type of connection is not found. Please check for typos.")
        self.schema_name = schema_name or self.DEFAULT_SCHEMAS[db_type]

    def load(self, table_name, sql_query_statement=None, **kwargs):
        """
        Load database table based on SQL query.

        Args:
            table_name (str): Name of the table where data_df will be exported to.
            sql_query_statement (str): SQL Query if any. Defaults to None
        """

        try:
            # Establishing connection
            self._logger.debug("[DatabaseConnector] Establishing connection...")
            engine_conn = self._create_engine()
            self._logger.debug("[DatabaseConnector] SQL connection established...")

            # Loading data
            self._logger.info("[DatabaseConnector] Executing SQL query...")
            if self.schema_name is not None:
                schema_table_name = str(self.schema_name) + "." + str(table_name)
            else:
                schema_table_name = table_name
            sql_query_statement = sql_query_statement or self._query_table(schema_table_name)
            db_loader = pd.read_sql(sql_query_statement, con=engine_conn, **kwargs)
            self._logger.info("[DatabaseConnector] SQL query executed successfully.")
            return db_loader

        except Exception as error:
            self._logger.exception(f"[DatabaseConnector] SQL Query Failed. Error: {error}")

        finally:
            engine_conn.dispose()
            self._logger.debug("[DatabaseConnector] SQL connection disposed.")

    def save(self, data_df, table_name, if_exists='replace', id_column='id', run_id_column='run_id', run_id=None,
             **kwargs):
        """
        Save dataframe to database table.

        Args:
            data_df (pd.DataFrame): Data to be have ID appended to.
            table_name (str): Name of the table where data_df will be exported to.
            if_exists (str): [{'replace', 'append', 'fail'}. Defaults to 'replace']
            id_column (str): (Optional) Column name in table which represents ID column. If set to None, no column
                will be added.
            run_id_column (str): Column name in table 'runs' which represents ID column. If set to None, no column
                will be added.
            run_id (int): id that will be saved in table 'runs'
        """
        try:
            start_time = datetime.now()

            if id_column is not None:
                replace = True if if_exists == 'replace' else False
                data_df = self._prepend_id(data_df, table_name, id_column, replace=replace)

            if run_id is not None:
                data_df[run_id_column] = run_id

            # Establishing connection
            self._logger.debug("[DatabaseConnector] Establishing connection...")
            engine_conn = self._create_engine()
            self._logger.debug("[DatabaseConnector] SQL connection established...")

            self._logger.info(f"[DatabaseConnector] Saving data_df to SQL table '{table_name}'...")
            data_df.to_sql(name=table_name, con=engine_conn, if_exists=if_exists, index=False, **kwargs)
            end_time = datetime.now()
            self._logger.info(f"[DatabaseConnector] data_df saved to SQL table '{table_name}' successfully in "
                              f"{end_time - start_time}.")

        except Exception as error:
            self._logger.exception(f"[DatabaseConnector] SQL Query Failed. Error: {error}")

        finally:
            engine_conn.dispose()
            self._logger.debug("[DatabaseConnector] SQL connection disposed.")

    def update_table(self, data_df, table_name, update_based_on):
        """
        Updates table based on a few approaches:
        1. If there is only a single row in each item/query of unique `update_based_on` column,
            then use SQL UPDATE statement.
        2. If there are multiple rows in each item/query of unique `update_based_on` column,
            then use SQL DELETE statement followed by INSERT.

        Args:
            data_df (pd.DataFrame): Data to be have ID appended to.
            table_name (str): Name of the table where data_df will be exported to.
            update_based_on (str): Column key to update table based on.
        """
        start_time = datetime.now()
        self._logger.debug("[DatabaseConnector] update_table() initiated.")

        for unique_update_identifier in data_df[update_based_on].unique():
            subset_data_df = data_df.loc[data_df[update_based_on] == unique_update_identifier, :].copy()

            # Method 1: SQL UPDATE
            if subset_data_df.shape[0] == 1:
                set_statement = []
                update_columns = data_df.columns[data_df.columns != update_based_on]
                for column_name in update_columns:
                    column_dtype = data_df[column_name].dtype
                    if column_dtype in ['float64', 'int64', 'float32', 'int32']:
                        set_statement += [f""""{column_name}" = {subset_data_df[column_name].values[0]}"""]
                    elif column_dtype in ['datetime64', 'datetime64[ns]']:
                        str_datetime = pd.to_datetime(subset_data_df[column_name].values[0])\
                            .strftime("%Y-%m-%d %H:%M:%S")
                        if self.db_type != 'postgresql':
                            set_statement += [f""""{column_name}" = CAST('{str_datetime}' AS DATETIME)"""]
                        else:
                            set_statement += [
                                f""""{column_name}" = TO_TIMESTAMP('{str_datetime}', 'YYYY-MM-DD HH24:MI:SS')"""
                            ]
                    else:
                        str_statement = subset_data_df[column_name].values[0]
                        str_statement = str_statement.replace("'", "`")
                        set_statement += [f""""{column_name}" = '{str_statement}'"""]

                sql_update_statement = f"""
                UPDATE {table_name}
                SET {', '.join(set_statement)}
                WHERE "{update_based_on}" = {unique_update_identifier}
                """

                self.execute_statement(sql_update_statement, expect_output=False)

            # Method 2: SQL DELETE & INSERT
            else:
                self._logger.warning(
                    "[DatabaseConnector] update_table() `update_based_on` column is not unique. DELETE and INSERT "
                    "method is done instead."
                )
                subset_data_df = self._prepend_id(subset_data_df, table_name, replace=False)
                delete_statement = f"DELETE FROM {table_name} WHERE {update_based_on} = {unique_update_identifier}"
                self.execute_statement(delete_statement, expect_output=False)
                self._logger.debug(f"[DatabaseConnector] Delete complete. Inserting updated data...")
                con = self._create_engine()
                subset_data_df.to_sql(table_name, con=con, if_exists='append', index=False, method='multi',
                                      chunksize=int(2100 / subset_data_df.shape[1]) - 1)

        end_time = datetime.now()
        self._logger.debug(f"[DatabaseConnector] update_table() completed in {end_time - start_time}.")

    def _create_engine(self):
        """Establishes connection with database."""
        sql_connectors = {
            'postgresql': 'postgresql',
            'mysql': 'mysql+pymysql',
            'mssql': 'mssql+pyodbc'
        }
        sql_connector = sql_connectors[self.db_type]
        if self.db_type == 'mssql':
            engine_conn = create_engine(
                f"{sql_connector}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
                f"?driver=SQL+Server"
            )
        else:
            engine_conn = create_engine(
                f"{sql_connector}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
            )
        return engine_conn

    def execute_statement(self, sql_statement, expect_output=True):
        """
        Executes SQL statement

        Args:
            sql_statement (str): SQL query statement to be executed
            expect_output (bool): Boolean to indicate if a result is expected from the SQL statement.
        """
        self._logger.debug("[DatabaseConnector] execute_statement() initiated.")
        engine = self._create_engine()
        with engine.connect() as con:
            self._logger.debug(f"[DatabaseConnector] Running {sql_statement}...")
            if expect_output:
                rs = con.execute(sql_statement).fetchall()
            else:
                con.execute(sql_statement)
                rs = None
        self._logger.debug("[DatabaseConnector] execute_statement() completed.")
        return rs

    @staticmethod
    def _query_table(table_name):
        """
        Default sql query

        Args:
            table_name (str): Name of database table
        """
        return 'select * from {} '.format(table_name)

    def get_latest_id(self, table_name, id_column='id'):
        """
        Detects and returns the latest ID from the database table. Returns 0 if table is not found.

        Args:
            table_name (str): Table name in DB to get ID.
            id_column (str): Column name in table which represents ID column.
        """
        self._logger.debug("[DatabaseConnector] get_latest_id() initiated.")

        if table_name not in self._create_engine().table_names():
            latest_id = 0
        elif self.execute_statement(f"SELECT COUNT(*) FROM {table_name}")[0][0] == 0:
            latest_id = 0
        else:
            latest_id = int(self.execute_statement(f'SELECT MAX("{id_column}") FROM {table_name}')[0][0])

        self._logger.debug("[DatabaseConnector] get_latest_id() completed.")
        return latest_id

    def _prepend_id(self, data_df, table_name, id_column='id', replace=False):
        """
        Prepends ID column (as part of auto-increment)

        Args:
            data_df (pd.DataFrame): Data to be have ID appended to.
            table_name (str): Name of the table where data_df will be exported to.
            id_column (str): Column name in table which represents ID column.
            replace (bool): Boolean to indicate whether the table in database is to be replaced. Default is False.
        """
        self._logger.debug("[DatabaseConnector] _prepend_id() initiated.")

        if table_name not in self._create_engine().table_names():
            data_df[id_column] = np.arange(1, data_df.shape[0] + 1, 1)
            return data_df

        id_no = np.arange(1, data_df.shape[0] + 1, 1)
        if not replace:
            id_no = id_no + self.get_latest_id(table_name, id_column)

        data_df[id_column] = id_no
        data_df = data_df[[id_column] + [x for x in data_df.columns if x != id_column]]

        self._logger.debug("[DatabaseConnector] _prepend_id() completed.")
        return data_df

    def log_run(self, task, start_time=None, end_time=None, run_id=None, run_table_name='runs'):
        """
        Log to runs table in database, intended for tracking function calls.

        Args:
            task (str): Task that is being performed, e.g., 'predict', 'train', 'download', 'optimise'
            start_time (datetime): datetime object representing the script start time.
            end_time (Optional, datetime): datetime object representing the script completion time.
            run_id (Optional, run_id): The ID of the run to be updated.
            run_table_name (Optional, str): Table name which stores all runs. Defaults to 'runs'.
        """

        runs_df = pd.DataFrame({
            'id': run_id,
            'server_name': uname()[1],
            'user_name': getpass.getuser(),
            'start_time': start_time,
            'end_time': end_time,
            'task': task,
            'run_time_minutes': 0.0
        }, index=[0])

        # If run_id is not provided, treat as a new run.
        if run_id is None:
            self.save(data_df=runs_df, table_name=run_table_name, if_exists='append')
            run_id = self.get_latest_id(run_table_name)

        # If run_id is provided, then update existing ID in runs table
        else:
            runs_df['id'] = run_id
            runs_df['end_time'] = end_time
            runs_df['run_time_minutes'] = (end_time - start_time) / pd.Timedelta(minutes=1)
            self.update_table(runs_df, run_table_name, update_based_on='id')

        return run_id


if __name__ == '__main__':

    pass