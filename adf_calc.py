import os
from statsmodels.tsa.stattools import adfuller
import pandas as pd
from pandas import Timestamp
import numpy as np
import matplotlib.pyplot as plt
import csv
from datetime import datetime, timedelta

def main():
    timeframe = str(input("What's the timeframe? {week, month or year}: ")).lower()
    df = load_data()
    if timeframe[0] == 'y':
        data = split(dataframe=df)
        if timeframe == 'y':
            timeframe = "year"
    elif timeframe[0] == 'm':
        data = split_to_monthly(dataframe=df)
        if timeframe == 'm':
            timeframe = "month"
    elif timeframe[0] == 'w':
        data = split_to_weekly(dataframe=df)
        if timeframe == 'w':
            timeframe = "week"
    the_loop(matrix=data, timeframe=timeframe)

def load_data(directory=f"/Volumes/TICK/spreadsheets/MYX_DLY_FCPOX2020, 5_1M_2M.csv", column="time"):
    dataframe = pd.read_csv(directory)
    dataframe.dropna(subset=["PlotCandle (Close) Nov Jan"], inplace=True)
    dataframe[column] = pd.to_datetime(dataframe[column], format="%Y-%m-%dT%H:%M:%SZ", errors='raise')
    print("dataframe\n", dataframe)
    return dataframe

# def create_csv(dataframe):
#     dataframe.to_csv("PlotCandle_(Close)_Nov_Dec_adf_values.csv", columns=["time", "PlotCandle (Close) Nov Dec"], index=False)
#     return "PlotCandle_(Close)_Nov_Dec_adf_values.csv"

"""
Splitting the data into yearly, weekly and monthly values
"""
def split(dataframe):
    years = [g for n, g in dataframe.groupby(pd.Grouper(key='time',freq='Y'))]
    for i in range(len(years)):
        year = pd.DataFrame(years[i])
        year.to_csv(f"year_{i}.csv", columns=["time", "PlotCandle (Close) Nov Jan"], index=False)
    print(years)
    return years

def split_to_weekly(dataframe):
    weeks = [g for n, g in dataframe.groupby(pd.Grouper(key='time',freq='W'))]
    for i in range(len(weeks)):
        week = pd.DataFrame(weeks[i])
        week.to_csv(f"week_{i}.csv", columns=["time", "PlotCandle (Close) Nov Jan"], index=False)
    print("Weeks", weeks)
    return weeks

def split_to_monthly(dataframe):
    months = [g for n, g in dataframe.groupby(pd.Grouper(key='time',freq='M'))]
    for i in range(len(months)):
        month = pd.DataFrame(months[i])
        month.to_csv(f"month_{i}.csv", columns=["time", "PlotCandle (Close) Nov Jan"], index=False)
    print("Months", months)
    return months

# Higher order function
"""
Takes input a matrix (or array) and a timeframe
Removes the split csv files created and calculates a seperate ADF for each file
"""
def the_loop(matrix, timeframe):
    data = dict()
    ADF_stats = list()
    p_value = list()
    used_lag = list()
    times = list()
    for i in range(len(matrix)): # dataframe == weeks
        time = str(matrix[i]["time"]).split()[1]
        print("type", type(matrix[i]["time"]))
        times.append(time)
        statistics = adf_test(f"{timeframe}_{i}.csv")
        print("stats", statistics)
        ADF_stats.append(statistics[0])
        p_value.append(statistics[1])
        used_lag.append(statistics[2])
        os.remove(f"{timeframe}_{i}.csv")
    data["time"] = times
    data["ADF"] = ADF_stats
    data["p_value"] = p_value
    data["used_lag"] = used_lag
    data = pd.DataFrame(data)
    data.to_csv(f"{timeframe}ly_adf_values.csv", index=False)

def adf_test(path):
    """
    Takes a csv file path as input (as a string)
    This file must have one heading as Dates and the other as Close
    This csv file will be converted into a series and then the ADF test will be
    completed using data from that csv file
    (Optional:
    will plot the data using matplotlib as a line graph)
    """
    if not os.path.exists(path):
        raise Exception("The path specified does not exist")
    df = pd.read_csv(path, parse_dates=['time'], index_col='time')
    series = df.loc[:, 'PlotCandle (Close) Nov Jan'].values
    # # Plotting the graph of the date against the close
    # df.plot(figsize=(14,8), label="Close price", title='Series', marker=".")
    # plt.ylabel("Close prices")
    # plt.legend()
    # plt.show()

    # ADF test
    result = adfuller(series, autolag="AIC")
    print(f"ADF Statistic = {result[0]}")
    print(f"p_value = {result[1]}")
    print(f"usedlags = {result[2]}")
    # Result 4 is a dictionary that contains the critical values
    for k, v in result[4].items():
        print(f"Critical Values are:\n {k}, {v}")

    print(result)
    return result[0], result[1], result[2]

if __name__ == "__main__":
    main()
