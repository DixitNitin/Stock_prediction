#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from datetime import timedelta, date
from mysql import connector
import utility
import datetime
import matplotlib.pyplot as plt
from scipy.signal import argrelextrema
import numpy as np

class Min_Max:

    def __init__(self):
        self.mydb = None
        self.mycursor = None
        self.df_colpal = None

        plt.rcParams["figure.figsize"] = (40,20)
        
        self.setup_db()
        self.read_data_from_db()

    def setup_db(self):
        self.mydb = connector.connect(user='vignan', password='d@t@b@se',
                                    host='127.0.0.1', auth_plugin='mysql_native_password',
                                    database='screener')
        self.mycursor = self.mydb.cursor()

    def read_data_from_db(self):
        sql = "use screener"
        self.mycursor.execute(sql)
        sql = "SELECT timestamp, close FROM daily_data  where Symbol = 'COLPAL'"
        self.df_colpal = pd.read_sql(sql,con=self.mydb)

    def print_data(self):
        print(self.df_colpal.head())
        print(self.df_colpal.tail())
        print(self.df_colpal.shape)


    def plot_data(self):
        plt.plot(df_colpal['timestamp'], df_colpal['close'])

    def cal_min_max(self):
        n=10
        self.df_colpal['min'] = self.df_colpal.iloc[argrelextrema(self.df_colpal.close.values, np.less_equal, order=n)[0]]['close']
        self.df_colpal['max'] = self.df_colpal.iloc[argrelextrema(self.df_colpal.close.values, np.greater_equal, order=n)[0]]['close']

    def plot_min_max(self):
        plt.scatter(self.df_colpal.timestamp, self.df_colpal['min'], c='r')
        plt.scatter(self.df_colpal.timestamp, self.df_colpal['max'], c='g')
        plt.plot(self.df_colpal.timestamp, self.df_colpal['close'])
        plt.show()


def main():
    min_max_colpal = Min_Max()
    min_max_colpal.cal_min_max()
    min_max_colpal.print_data()
    min_max_colpal.plot_min_max()

if __name__ == '__main__':
    main()