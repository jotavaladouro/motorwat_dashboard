"""
Load data to big query from mysql in batch mode (all data from a date)
or streaming (load data from a date, wait, and load new data).
* loader.py Date .- Load data from mysql to big query from a day
* loader.py Date online .- Load in streamind mode.
* loader.py Date --until_yestarday .- Load all data from
                                            date until yestarday

Based in biq_query.py and window.py
Fill raw table with car info
Fill aggr table with info aggregate by minites

"""

from string import Template
import argparse
import datetime
import numpy as np
import big_query
import window
from time import sleep
from datetime import date, timedelta
import mysql

from typing import Optional


INPUT_DIR_LOCAL = "./data/"


class DeltaTemplate(Template):
    delimiter = "%"


def strfdelta(tdelta, fmt):
    """
    :param tdelta:datetime timedelta
    :param fmt: Format
    :return: String from tdelte in fmt format
    """
    d = {"D": tdelta.days}
    hours, rem = divmod(tdelta.seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    d["H"] = '{:02d}'.format(hours)
    d["M"] = '{:02d}'.format(minutes)
    d["S"] = '{:02d}'.format(seconds)
    t = DeltaTemplate(fmt)
    return t.substitute(**d)


# Get a string from a datetime.timedelta
def lambda_timedelta(x): return strfdelta(x, '%H:%M:%S')


# Get a string from a datetime.date
def lambda_date2string(x): return x.strftime('%Y-%m-%d') if type(x) == datetime.date else x


# Replace None with 0000-00-00
def lambda_data_none(x):
    if x is None:
        return "0000-00-00"
    else:
        return x


def process_daraframe(dataframe_result):
    """
    Process time and dates from a dataframe
    :param dataframe_result: dataframe
    :return: dataframe processed
    """
    if dataframe_result is not None:
        #T_Hora_C : Transit time
        dataframe_result[4] = dataframe_result[4]. \
            apply(lambda_timedelta)
        # D_Obu_Data : Obu entry date
        dataframe_result[12] = dataframe_result[12]. \
            apply(lambda_data_none)
        # T_Obu_Time : Obu entry time
        dataframe_result[13] = dataframe_result[13]. \
            apply(lambda_timedelta)
        ##D_Data_C : Transit date
        dataframe_result[3] = dataframe_result[3]. \
            apply(lambda_date2string)
        # D_Obu_Data : Obu entry date
        dataframe_result[12] = dataframe_result[12]. \
            apply(lambda_date2string)
    return dataframe_result



def print_lines(dataframe_load):
    """
    Print rows from a dataframe
    :param dataframe_load:
    :return:
    """
    csv = dataframe_load.to_csv(
                            header=None,
                            index=False).strip('\n').split('\n')
    for line in csv:
        print(line)


def parse_parameter():
    """

    :return: args_return:
        day
        only_print
        online
        until_yestarday
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("day", help="day string")
    parser.add_argument('--only_print', help='Print data, not send',
                        action='store_true', default=False)
    parser.add_argument('--online', help='Online send',
                        action='store_true', default=False)
    parser.add_argument('--until_yesterday', help="load fron date to yestarday",
                        action='store_true', default=False)
    args_return = parser.parse_args()
    return args_return


def load_df(df, window_remain=5):
    """
    Load a dataframe to raw data
    Load a dataframe to in memory windows  time slider
    Store the last window_remain time windows for future data.
    Load the rest of the time windows to  bigquery.
    :param df: dataframe
    :param window_remain: number of win
    :return:
    """

    df["Travel_Time_Second"] = 0
    # Prepare data and load to rwa table
    df = big_query.add_raw_columns_names(df)
    df = big_query.add_travel_time(df)
    big_query.write_df_raw(df)
    # Add data to window time slider
    window.window_add_dataframe(df)
    # Get windows to load to big query
    df_aggr_total = window.window_get_window_ready(window_remain)
    if df_aggr_total is not None:
        df_aggr = window.window_get_window_ready(window_remain)
        while df_aggr is not None:
            df_aggr_total = df_aggr_total.append(df_aggr)
            df_aggr = window.window_get_window_ready(window_remain)
        big_query.write_df_aggr(df_aggr_total)


def load_day(date_load, online, lmysql, lbigquery):
    """
    Get day data from mysql and load to bigquery.
    Before this delete data in big query from that day
    :param date_load:  day
    :param online: if True, never finish, load data, sleep and load new data
    :param lmysql: Mysql conection
    :param lbigquery:  BigQuery connection
    :return:
    """
    print("Load day, delete " + str(date_load) + " online " + str(online))
    big_query.delete_day(date_load, lbigquery)
    print("Load day " + str(date_load) + " online " + str(online))
    end = False
    last_index = 0  # Last index read from mysql database
    while not end:
        # Get data from mysql
        data = mysql.get_data(lmysql, date_load, last_index)
        data = process_daraframe(data)
        if data is not None:
            # Last column is database index.
            # Store for new sql querys and remove from dataframes
            print("Loading " + str(data.shape))
            last_index = data[15].max()
            data = data.drop(columns=15)
            if args.only_print:
                print_lines(data)
            else:
                if args.online:
                    # If online, we store the last five window for future data incoming
                    load_df(data,  window_remain=5)
                else:
                    # If not online, not future data incomming, not store.
                    load_df(data,  window_remain=0)
            if not args.online:
                end = True
            else:
                sleep(60)


def get_days(init_date, until_yesterday):
    """
    If until_yesterday we return a list with days from init date
        until yesterday
    Otherway return a list with init_date
    :param init_date: First day in the list
    :param until_yesterday:
    :return: list string days
    """
    if until_yesterday:
        start = datetime.datetime.strptime(init_date, "%Y-%m-%d").date()
        yesterday = date.today() - timedelta(1)
        lst_days = [(start + datetime.timedelta(days=x)).
                    strftime("%Y-%m-%d") for x in range(0, (yesterday - start).days + 1)]
    else:
        lst_days = [init_date]
    return lst_days


if __name__ == "__main__":
    np.seterr(all='raise')
    args = parse_parameter()
    # Get mysql and big_query client connections
    mysql_conn = mysql.connect_mysql()
    client = big_query.get_client_bigquery()
    print(args)
    # Init window
    window.window_init()
    try:
        for day in get_days(args.day, args.until_yesterday):
            load_day(day, args.online, mysql_conn, client)
    except KeyboardInterrupt:
        pass
    mysql.close_mysql(mysql_conn)

