"""
Add car data, group data by date and time window (window size is 1 minute).
In a time window, group by route
Calculate IMH and Median travel time for all route
Use:
    window_init .- Init data
    window_add_dataframe .- Add dataframes to data
    window_get_window_ready .- Get the oldest window data and remove
"""

import big_query as bq
import pandas as pd
import unittest

# Store data :Dictionari : keys date, time (hour and minute)
# Value : List of car data: Columns: N_Orixen_X,N_Destino_X,N_Mensaxe_C,Travel_Time_Second
window_data = {}


def window_init():
    """ Init data"""
    global window_data
    window_data = {}


def window_add_dataframe(df):
    """ Add data from a dataframe do window_data

    """
    for date in df.D_Date.unique():
        window_add_dataframe_date(date, df.loc[df["D_Date"] == date])


def window_add_dataframe_date(date, df):
    """
    Add data from a a dataframe.
    Dataframe has data only from a day
    Remove seconds from T_Hora_C, where time are store
    :param date: day
    :param df:data
    :return:
    """
    def filter_time(row):
        return row["T_Time"][:5]
    df["T_Time"] = df.apply(filter_time, axis=1)
    for time in df.T_Time.unique():
        window_add_dataframe_time(date,
                                  time,
                                  df.loc[df["T_Time"] == time])


def window_add_dataframe_time(date, time, df):
    """
    Add data from a dataframe
    All data on the dataframe mush have the same date and the same time (in hour and minutes)
    :param date: day
    :param time: hour and minutes
    :param df:data
    :return:
    """
    if date not in window_data:
        window_data[date] = {}
    if time not in window_data[date]:
        window_data[date][time] = pd.DataFrame()
    window_data[date][time] = window_data[date][time].append(df)


def window_is_window_ready(max_window):
    """
    Check if we have more than max_windows windows in data.
    :param max_window:
    :return: true .- there is more than max_window windows
    """
    n_window = 0
    for data in window_data:
        n_window += len(window_data[data])
    return n_window > max_window


def window_get_older():
    """
    Get the older time window and remove from window_data.
    If the date of the older window do not have more time,
     date from window_data
    Calculate aggredate data from windows car:
        IMH.- Average Hourly Traffic AHT
        Travel_Time
    :return:
        None .- No window
        Dataframe with data from the older time windown
            Columns; Date,Time,N_Orixen,N_Destino,Travel_Time,IMH
    """
    if len(window_data) == 0:
        return None
    date = sorted(window_data.keys())[0]
    if len(window_data[date]) == 0:
        window_data.pop(date, None)
        return None
    time = sorted(window_data[date].keys())[0]
    df = window_get_aggr(window_data[date][time])
    df["Date"] = date
    df["Time"] = time
    window_data[date].pop(time, None)
    if len(window_data[date]) == 0:
        window_data.pop(date, None)
    return df


def window_get_window_ready(max_window):
    """
    :param max_window:
    :return:
        None .- No more than max_window available
        Dataframe with data from the older time windown
            Columns; Date,Time,N_Orixen,N_Destino,Travel_Time,IMH
    """
    if window_is_window_ready(max_window):
        return window_get_older()
    else:
        return None


def window_get_aggr(df):
    """
    Calculate aggregate data for a window dataframe
        IMH.- Average Hourly Traffic AHT
        Travel_Time
    :param df: Dataframe with data from the same date and time
    :return:
            Dataframe with data calculate from df
            Columns; Date,Time,N_Orixen,N_Destino,Travel_Time,IMH
    """

    # Count car by path, with a column IMH
    serie_aggr_imh = df.\
        groupby(["N_Source", "N_Destination"])["N_Message"].count()
    df_aggr_imh = serie_aggr_imh.to_frame().rename(
        columns={"N_Message": "AHT", "N_Source" : "Source"})

    # Get min time travel by path
    serie_aggr_travel_time = df[df["Travel_Time_Second"] > 0].\
        groupby(["N_Source", "N_Destination"])["Travel_Time_Second"].min()
    df_aggr_travel_time = pd.DataFrame(serie_aggr_travel_time).rename(
        columns={"Travel_Time_Second": "Travel_Time"})

    # Source and destination are index, transform to column
    df_aggr_imh = df_aggr_imh.reset_index(
                    level=["N_Source", "N_Destination"])
    df_aggr_travel_time = df_aggr_travel_time.reset_index(
                    level=["N_Source", "N_Destination"])

    # Merge both dataframe an rename columns
    df_aggr = pd.merge(df_aggr_imh, df_aggr_travel_time, how='left',
                       left_on=["N_Source", "N_Destination"],
                       right_on=["N_Source", "N_Destination"])
    df_aggr = pd.DataFrame(df_aggr).rename(
            columns={"N_Source" : "Source", "N_Destination" : "Destination"})

    df_aggr = df_aggr.fillna(0)
    df_aggr[["Travel_Time"]] = df_aggr[["Travel_Time"]].astype(int)
    return df_aggr


class TestWindow(unittest.TestCase):
    def test_window(self):
        def window_create_df_test(columns_name):
            lst = [[0, 6, 2, "2018-01-01", "00:00:00", "55555", 6, 7, 8, 9, 10, 8, "2018-01-02", "01:00:00", 13, 14],
                   [1, 6, 2, "2018-01-02", "00:00:00", "55555", 6, 7, 8, 9, 10, 8, "2018-01-02", "01:00:00", 13, 16],
                   [1, 6, 2, "2018-01-02", "00:00:00", "55555", 6, 1, 8, 9, 10, 8, "2018-01-02", "01:00:00", 13, 10],
                   [1, 6, 2, "2018-01-02", "00:00:00", "55555", 6, 1, 8, 9, 10, 8, "2018-01-02", "01:00:00", 13, 16],
                   [1, 6, 2, "2018-01-02", "00:01:00", "55555", 6, 7, 8, 9, 10, 8, "2018-01-02", "01:00:00", 13, 0],
                   [2, 6, 2, "2018-01-02", "00:01:00", "55555", 6, 7, 8, 9, 10, 8, "2018-01-02", "01:00:00", 13, 16],
                   [1, 6, 2, "2018-01-02", "00:02:00", "55555", 6, 7, 8, 9, 10, 8, "2018-01-02", "01:00:00", 13, 0],
                   [2, 6, 2, "2018-01-02", "00:02:00", "55555", 6, 7, 8, 9, 10, 8, "2018-01-02", "01:00:00", 13, 0]
                   ]
            return pd.DataFrame(lst,
                                columns=columns_name)

        df_test = window_create_df_test(bq.get_columns_from_list(bq.TABLE_RAW_SCHEMA))
        window_add_dataframe(df_test)
        df_aggr = window_get_window_ready(1)
        assert len(df_aggr) == 1
        assert df_aggr["AHT"][0] == 1
        assert df_aggr["Travel_Time"][0] == 14

        df_aggr = window_get_window_ready(1)
        assert len(df_aggr) == 2
        assert df_aggr["AHT"][0] == 2
        assert df_aggr["AHT"][1] == 1
        assert df_aggr["Travel_Time"][0] == 10
        assert df_aggr["Travel_Time"][1] == 16

        df_aggr = window_get_window_ready(1)
        assert len(df_aggr) == 1
        assert df_aggr["AHT"][0] == 2
        assert df_aggr["Travel_Time"][0] == 16

        df_aggr = window_get_window_ready(1)
        assert df_aggr is None

        df_aggr = window_get_window_ready(0)
        assert len(df_aggr) == 1
        assert df_aggr["AHT"][0] == 2
        assert df_aggr["Travel_Time"][0] == 0
