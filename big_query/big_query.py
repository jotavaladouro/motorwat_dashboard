"""
Lib to manage raw and aggr table in bigquery dataset.
RAW table : Car info
AGGR  Aggregate info for a time window with IMH and travel time for all posible routes
Options:
    -f filename .- Load a csv file.
You can create dataset, tables, fill the table and get date.
Based on  pandas_gbq
"""

import pandas_gbq as pd_gbq
import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions
import datetime
import argparse
import window
import unittest
import time
from env import PROJECT_ID


TABLE_RAW = 'raw'
TABLE_AGGR = 'aggr'
DEFAULT_DATASET = "toll_data"
TEST_DATASET = "test_bigquery"
TABLE_RAW_SCHEMA = ("N_Message:INTEGER," 
                    "N_Station:INTEGER,"
                    "N_Lane:INTEGER,"
                    "D_Date:STRING,"
                    "T_Time:STRING,"
                    "Sz_Key:STRING,"
                    "N_Source:INTEGER,"
                    "N_Destination:INTEGER,"
                    "N_Payment:INTEGER,"
                    "N_Obu_Entry_Ok:INTEGER,"
                    "N_Obu_Payment:INTEGER,"
                    "N_Obu_Entry_Station:INTEGER,"
                    "D_Obu_Entry_Date:STRING,"
                    "T_Obu_Entry_Time:STRING,"
                    "N_Obu_Entry_Lane:INTEGER,"
                    "Travel_Time_Second:INTEGER")
TABLE_AGGR_SCHEMA = ("Source:INTEGER,"
                     "Destination:INTEGER,"
                     "Date:STRING,"
                     "Time:STRING," 
                     "AHT:INTEGER,"
                     "Travel_Time:INTEGER")


def write_df(df, table_id, dataset=DEFAULT_DATASET):
    """
    Write a dataframe to a table in a dataset.
    :param df:  dataframe to write
    :param table_id:  big query table id
    :param dataset: bigquery dataset.
    :return:
    """
    pd_gbq.to_gbq(df,
                  dataset + '.' + table_id,
                  PROJECT_ID,
                  if_exists='append')


def write_df_raw(df):
    """
    Write a dataframe to raw table in default dataset
    :param df:
    :return:
    """
    write_df(df, TABLE_RAW)


def write_df_aggr(df):
    """
    Write a dataframe to raw table in default dataset
    :param df:
    :return:
    """
    write_df(df, TABLE_AGGR)


def read_df_from_raw(date,  dataset=DEFAULT_DATASET):
    """
    Read data from the raw table for a date and a dataset
    :param date: Get data from this date
    :param dataset:
    :return: Dataframe with data
    """
    sql = ("select * from " + dataset + ".raw "
           "where D_Date=\"" + date + "\" order "
           "by D_Date,T_Time")
    return pd_gbq.read_gbq(sql, PROJECT_ID, dialect='standard')


def read_df_from_aggr(date,  dataset=DEFAULT_DATASET):
    """
    Read data from the aggr table for a date and a dataset
    :param date: Get data from this date
    :param dataset:
    :return: Dataframe with data
    """
    sql = ("select * from " + dataset + ".aggr"
           " where Date=\"" + date + "\" order "
           "by Date,Time")
    return pd_gbq.read_gbq(sql, PROJECT_ID, dialect='standard')


def delete_day_table(date,
                     client_bq,
                     table,
                     date_column,
                     dataset=DEFAULT_DATASET):
    """
    Deleta day info from a table
    :param date: String format YYYY-MM-DD
    :param client_bq:
    :param table:
    :param date_column: column with data info in the fable
    :param dataset:
    :return:
    """
    sql = "delete FROM " + dataset + "." + table + " where " \
          " " + date_column + " = \"" + date + "\""
    query_result = client_bq.query(sql)
    while query_result.running():
        time.sleep(0.1)


def delete_day(date, client_bq, dataset=DEFAULT_DATASET):
    """
    Delete day info from raw and aggr table
    :param date:
    :param client_bq:
    :param dataset:
    :return:
    """
    delete_day_table(date, client_bq, "raw", "D_Date", dataset=dataset)
    delete_day_table(date, client_bq, "aggr", "Date", dataset=dataset)


def get_client_bigquery():
    """
    :return: A big query client
    """
    return bigquery.Client(project=PROJECT_ID)


def create_dataset(client, dataset_id):
    """ Create a dataset en EU if it do not exist."""
    dataset_ref = client.dataset(dataset_id)
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = 'EU'
    try:
        client.create_dataset(dataset)
    except exceptions.Conflict:
        pass


def get_schema_from_list(schema_list):
    """
    Create a bigquery schema from a list
    :param schema_list: Columns definitions separate by  "," character,
        A column definition is NAME: TYPE
    :return: bigquery schema
    """
    schema = []
    columns_def = schema_list.split(',')
    for column_def in columns_def:
        column_name, column_type = column_def.split(':')
        schema.append(bigquery.SchemaField(column_name,
                                           column_type,
                                           mode='NULLABLE'))
    return schema


def get_columns_from_list(schema_list):
    """
    Create a column name list
    :param schema_list: Columns definitions separate by  "," character,
        A column definition is NAME: TYPE
    :return: column name list
    """
    columns_name = []
    columns_def = schema_list.split(',')
    for column_def in columns_def:
        column_name, column_type = column_def.split(':')
        columns_name.append(column_name)
    return columns_name


def create_table(dataset_id, table_id, schema_list, client_bq):
    """
    Create a table if it does not exist, form a schema
    :param dataset_id:
    :param table_id:
    :param schema_list: Columns definitions separate by  "," character,
        A column definition is NAME: TYPE
    :param client_bq:
    :return:
    """
    dataset_ref = client_bq.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    schema = get_schema_from_list(schema_list)
    table = bigquery.Table(table_ref, schema=schema)
    try:
        client_bq.create_table(table)
    except exceptions.Conflict:
        pass


def delete_table(dataset_id, table_id, client_bq):
    """ Deleta a table"""
    table_ref = client_bq.dataset(dataset_id).table(table_id)
    try:
        client_bq.delete_table(table_ref)
    except exceptions.NotFound:
        pass


def add_travel_time(df):
    """
    Add travel time to a dataframe.
    If car has a obu transponder ["N_Obu_Pago"] != 0
    and entry info in obu is ok row["N_Obu_Validez_In"]==0
        "Travel_Time_Second" = toll time - entry time
    other case:
        "Travel_Time_Second" = 0
    :param df:  Dataframe with car transit info.
    :return: dataframe with travel time added "Travel_Time_Second"
    """

    def get_time(row):
        """ Add time trave to a dataframe row"""
        seconds_travel = 0
        try:

            timestamp = datetime.datetime.strptime(row['D_Date'] + " @ " +
                                                   row['T_Time'],
                                                   "%Y-%m-%d @ %H:%M:%S")
            timestamp_sg = (timestamp - datetime.datetime(1970, 1, 1)).total_seconds()

            if row["N_Obu_Payment"] != 0 and row["N_Obu_Entry_Ok"] == 0:
                # If we have a valid obu with a valid entry info
                timestamp_in = datetime.datetime.strptime(row['D_Obu_Entry_Date'] +
                                                          " @ " +
                                                          row["T_Obu_Entry_Time"],
                                                          "%Y-%m-%d @ %H:%M:%S")
                timestamp_in_sg = (timestamp_in - datetime.datetime(1970, 1, 1)).total_seconds()
                seconds_travel = timestamp_sg - timestamp_in_sg
        except Exception as e:
            print(e)
            pass
        return int(seconds_travel)
    df["Travel_Time_Second"] = df.apply(get_time, axis=1)
    return df


def add_raw_columns_names(df):
    """
        Add column names to a dataframe acording to TABLE_RAW_SCHEMA
    :param df:
    :return:
    """
    df.columns = get_columns_from_list(TABLE_RAW_SCHEMA)
    return df


def load_file_csv_to_raw(file_load):
    """ Load file to raw table and aggr table"""
    df = pd.read_csv(file_load,
                     names=get_columns_from_list(TABLE_RAW_SCHEMA))
    df[["Sz_Key"]] = df[["Sz_Key"]].astype(object)
    client = get_client_bigquery()
    create_table(DEFAULT_DATASET, TABLE_RAW, TABLE_RAW_SCHEMA, client)
    create_table(DEFAULT_DATASET, TABLE_AGGR, TABLE_AGGR_SCHEMA, client)
    add_travel_time(df)
    write_df(df, TABLE_RAW)
    # Windonize dataframe
    window.window_add_dataframe(df)
    # Get first window info, dataframe format
    df_aggr_total = window.window_get_window_ready(0)
    if df_aggr_total is not None:
        # if we have info for first window, get info for next window
        df_aggr = window.window_get_window_ready(0)
        while df_aggr is not None:
            # While we get info for a time window, add to the total info dataframe
            df_aggr_total = df_aggr_total.append(df_aggr)
            # Get info for the next dataframe
            df_aggr = window.window_get_window_ready(0)
        write_df(df_aggr_total, TABLE_AGGR)


class TestBigQuery(unittest.TestCase):

    def setUp(self):
        """ Create a dataset and clean tables for test
        Called before test
        """
        self.client = get_client_bigquery()
        create_dataset(self.client, TEST_DATASET)
        delete_table(TEST_DATASET, TABLE_RAW, self.client)
        create_table(TEST_DATASET, TABLE_RAW, TABLE_RAW_SCHEMA, self.client)
        delete_table(TEST_DATASET, TABLE_AGGR, self.client)
        create_table(TEST_DATASET, TABLE_AGGR, TABLE_AGGR_SCHEMA, self.client)

    def test_remove_operation(self):
        date_remove = "2018-01-01"
        self.test_raw_operations()
        self.test_aggr_opperation()
        df_read = read_df_from_aggr(date_remove, dataset=TEST_DATASET)
        assert df_read.shape[0] > 0
        df_read = read_df_from_raw(date_remove, dataset=TEST_DATASET)
        assert df_read.shape[0] > 0
        delete_day(date_remove, self.client, dataset=TEST_DATASET)
        df_read = read_df_from_aggr(date_remove, dataset=TEST_DATASET)
        assert df_read.shape[0] == 0
        df_read = read_df_from_raw(date_remove, dataset=TEST_DATASET)
        assert df_read.shape[0] == 0

    def test_travel_time(self):
        def create_df_test_raw_2(columns_name):
            """ For test use"""
            lst = [[0, 6, 2, "2018-01-01", "01:00:00", "55555", 6, 7, 8, 0, 0, 8, "2018-01-01", "00:01:00", 13, 14],
                   [1, 6, 2, "2018-01-01", "01:00:01", "55555", 6, 7, 8, 1, 5, 8, "2018-01-01", "00:02:00", 13, 16],
                   [1, 6, 2, "2018-01-01", "01:00:02", "55555", 6, 7, 8, 0, 5, 8, "2018-01-01", "00:05:00", 13, 16],
                   ]
            return pd.DataFrame(lst, columns=columns_name), [0.0, 0.0, 3302.0]

        df_test, list_travel_time = create_df_test_raw_2(get_columns_from_list(TABLE_RAW_SCHEMA))
        df_test.pop("Travel_Time_Second")
        add_travel_time(df_test)
        assert list(df_test["Travel_Time_Second"]) == list_travel_time

    def test_raw_operations(self):
        def create_df_test_raw(columns_name):
            """ For test use"""
            lst = [[0, 6, 2, "2018-01-01", "00:00:00", "55555", 6, 7, 8, 9, 10, 8, "2018-01-02", "01:00:00", 13, 14],
                   [1, 6, 2, "2018-01-01", "00:00:00", "55555", 6, 7, 8, 9, 10, 8, "2018-01-02", "01:00:00", 13, 16]
                   ]
            return pd.DataFrame(lst, columns=columns_name)
        df_test = create_df_test_raw(get_columns_from_list(TABLE_RAW_SCHEMA))
        write_df(df_test,
                 TABLE_RAW,
                 dataset=TEST_DATASET)
        df_read = read_df_from_raw("2018-01-01", dataset=TEST_DATASET)
        assert df_read.equals(df_test)

    def test_aggr_opperation(self):
        def create_df_test_aggr(columns_name):
            """ For test use"""
            lst = [[6, 2, "2018-01-01", "00:00:00", 1, 2],
                   [6, 2, "2018-01-01", "00:00:00", 6, 7]
                   ]
            return pd.DataFrame(lst,
                                columns=columns_name)
        df_test = create_df_test_aggr(get_columns_from_list(TABLE_AGGR_SCHEMA))
        write_df(df_test,
                 TABLE_AGGR,
                 dataset=TEST_DATASET)
        df_read = read_df_from_aggr("2018-01-01", dataset=TEST_DATASET)
        assert df_read.equals(df_test)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-f',
                        '--file_to_raw',
                        dest='input',
                        help='Input file to process.')
    known_args, other_args = parser.parse_known_args()

    if known_args.input is not None:
        load_file_csv_to_raw(known_args.input)
