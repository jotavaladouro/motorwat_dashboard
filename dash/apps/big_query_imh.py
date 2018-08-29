"""
Read data from bigquery, and create window rolling info
"""
from pandas_gbq import read_gbq
import pandas as pd
from datetime import datetime, timedelta

# Big query info
PROJECT_ID = "audasa-154511"
DATASET = "toll_data"
TABLE_AGGR = "aggr"
# Window rolling time in minutes
WINDOW_ROLLING_IMH = 10
WINDOW_ROLLING_TIME = 5
# Time from series
index = [x.time() for x in pd.date_range("00:11", "23:59", freq="min")]


class ImhDataCompare:
    """
    Load the data for the day, and the data for the same date
    on previous weeks.
    Init .- Calculate the dates to use
    Load .- Load the data from database
    get_imh_all .- Return list of imh
    get_time_travel_path .- Return list of travil time.
    get_dates .- Dates used in calc
    """
    def __init__(self, lst_path, date):
        """

        :param lst_path: list paths in for (source,destination)
        :param date:  datatime
        """
        self.lst_path = lst_path
        self.dates = []
        dt = datetime.strptime(date, "%Y-%m-%d")
        # Calculate dates to use
        for delay in range(3):
            dt_calc = dt - timedelta(days=(7 * delay))
            sz_date = dt_calc.strftime("%Y-%m-%d")
            self.dates.append(sz_date)
        self.data = []
        print(self.dates)
        # Create class to store data, no data loaded
        for date_calc in self.dates:
            self.data.append(ImhData(lst_path, date_calc))

    def load(self):
        """
        Load data for all days
        """
        for imh in self.data:
            imh.load()

    def load_last(self):
        """
        Load data from last day
        """
        self.data[0].load()

    def get_imh_all(self):
        """
        return: a list with 3 series with imh info along day
        """
        lst = []
        for imh in self.data:
            lst.append(imh.get_imh_all())
        return lst

    def get_dates(self):
        """
        :return: a list of string with the dates
        """
        return self.dates

    def get_imh_path(self, origin, destination):
        """
        :param origin: path origin
        :param destination: path destination
        :return: return: a list with 3 series with imh info along day
        """
        lst = []
        for imh in self.data:
            lst.append(imh.get_imh_path(origin, destination))
        return lst

    def get_time_travel_path(self, origin, destination):
        """
        :param origin: path origin
        :param destination: path destination
        :return: return: a list with 3 series with time travel info along day
        """
        lst = []
        for imh in self.data:
            lst.append(imh.get_time_travel_path(origin, destination))
        return lst


class ImhData:
    """
    Object to load and store imh data from a date.
    Use load before use the object
    get_imh_all .- Get imh for all path
    get_imh_path .- Get img for a path
    get_time_travel_path .- Get time travel for a path

    """
    def __init__(self, lst_path, date):
        """
        :param lst_path: list paths in for (source,destination)
        :param date:date string
        """
        self.df = None
        self.lst_path = lst_path
        self.date = date

    def build_sql(self):
        """
        return: sql sentence to load the data
        """
        sql = "SELECT Date,Time,Source,Destination,AHT,Travel_Time \
            FROM " + DATASET + "." + TABLE_AGGR + " where Date " \
            "= \"" + self.date + "\" and Time > \"00:10\" "
        sql = sql + " and ("
        for counter, (origin, destination) in enumerate(self.lst_path):
            if counter > 0:
                sql = sql + " or "
            sql = sql + "(Source=" + str(origin) + " and Destination=" + str(destination) + ") "
        sql = sql + ")"
        sql = sql + " order by Date,Time"
        return sql

    def load(self):
        """
        Load data
        """
        sql = self.build_sql()
        self.df = read_gbq(sql, PROJECT_ID, dialect='standard')
        self.df["Time"] = self.df["Time"].apply(lambda time:  datetime.strptime(time, '%H:%M').time())

    def get_imh_all(self):
        """
        return: a  series with imh info along day
        """
        return self.df.groupby("Time").sum()["AHT"].\
            reindex(index, fill_value=0).\
            rolling(WINDOW_ROLLING_IMH).mean() * 60

    def get_imh_path(self, origin, destination):
        """
        :param origin: path origin
        :param destination: path destination
        :return: return: a  series with imh info along day
        """
        if origin is None:
            # destination Imh
            return self.df.loc[
                                      (self.df['Destination'] == destination)
                                      ].groupby("Time").sum()["AHT"]. \
                       reindex(index, fill_value=0). \
                       rolling(WINDOW_ROLLING_IMH).mean() * 60
        if destination is None:
            # source Imh
            return self.df.loc[(self.df['Source'] == origin)
                               ].groupby("Time").sum()["AHT"]. \
                       reindex(index, fill_value=0). \
                       rolling(WINDOW_ROLLING_IMH).mean() * 60

        return self.df.loc[(self.df['Source'] == origin) &
                           (self.df['Destination'] == destination)
                           ].groupby("Time").sum()["AHT"].\
            reindex(index, fill_value=0).\
            rolling(WINDOW_ROLLING_IMH).mean() * 60

    def get_time_travel_path(self, origin, destination):
        """
        :param origin: path origin
        :param destination: path destination
        :return: return: a  series with time travel info along day
        """
        if origin is None:
            time_serie = self.df.loc[
                                       (self.df['Destination'] == destination)
                                    ].groupby("Time").min()["Travel_Time"]

        elif destination is None:
            time_serie = self.df.loc[(self.df['Source'] == origin)
                                     ].groupby("Time").min()["Travel_Time"]
        else:
            time_serie = self.df.loc[(self.df['Source'] == origin) &
                                     (self.df['Destination'] == destination)
                                     ].groupby("Time").min()["Travel_Time"]
        return time_serie.iloc[time_serie.nonzero()[0]].reindex(index,method="ffill").\
            rolling(WINDOW_ROLLING_TIME).min() / 60
