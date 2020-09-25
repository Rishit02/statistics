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
    print("Loading data...")
    df = load_data()
    # path = split_minute(dataframe=df)
    # df = pd.read_csv(f"{path}")

    print("Splitting data...")
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
    print("Looping through the data...")
    the_loop(matrix=data, timeframe=timeframe)

"""
Loading the data into a dataframe
"""
def load_data(directory="/Volumes/TICK/required_info"):
    month_codes = {"Jan":"F", "Feb":"G", "Mar":"H", "Apr":"J", "May":"K", "Jun":"M", "Jul":"N", "Aug":"Q", "Sep":":U", "Oct":"V", "Nov":"X", "Dec":"Z"}
    # Collect all the csv files into one DataFrame
    all_files = list()
    dir_list = sorted(os.listdir(directory))
    for file in dir_list:
        file = str(file)
        if file[0:2] == "._":
            continue
            print('._', file)
        print(file)
        csv_file = pd.read_csv(f"/Volumes/TICK/required_info/{file}")
        csv_file['Time'] = csv_file[csv_file.columns[5:7]].apply(lambda x: ','.join(x.dropna().astype(str)),axis=1)
        csv_file['Time'] = pd.to_datetime(csv_file['Time'], format="%Y%m%d,%H%M%S")
        del df['Log_Time']
        del df['Year']
        csv_file = csv_file.loc[(csv_file['Msg_Code'] == "T")]
        all_files.append(csv_file)

    all_files = pd.concat(all_files, axis=0, ignore_index=True)

    print(f"All files header\n{all_files.head(5)}")
    all_files.to_csv("all_files.csv")
    return pd.DataFrame(all_files)

"""
Splitting the data into yearly, weekly and monthly values
"""
def split_minute(dataframe):
    mins = [g for n, g in dataframe.groupby(pd.Grouper(key='Time',freq='Min'))]
    minutes = list()

    for i in range(len(mins)):
        min = pd.DataFrame(mins[i])
        print("Min: \n", min.head(3))
        if min.empty:
            print("EMPTY:", i)
            del min
        else:
            min.reset_index(drop=True, inplace=True)
            time = min.at[0, 'Time']
            # df.insert(0, 'Time', (min.at[0, 'Time']))
            # df.insert(1, 'Open', int(min.at[0, 'Price']))
            min['Time'] = time

            min['Open'] = min['Price'][0]
            min['Close'] = min['Price'][len(min) - 1]
            # df.insert(2, 'Close', int(min.at[len(min.index), 'Price']))

            # Sorting the values to get high low
            print("min and i is \n", min, i)
            # df.insert(3, 'High', min.at[len(min.index), 'Price'])
            min.sort_values(by=['Price'], ascending=True, inplace=True)
            min.reset_index(drop=True, inplace=True)
            min['Low'] = min['Price'][0]
            min['High'] = min['Price'][len(min)-1]
            print("min.head() is: \n", min.head(5))

            print("min.head() is: \n", min.head(5))
            print("MIN.columns is: \n", min.columns)

            minutes.append(min)

    print("minutes is: \n", minutes, type(minutes))

    minutes = pd.concat(minutes, axis=0, ignore_index=True)
    minutes.to_csv("minutes.csv")
    return f"minutes.csv"

def split(dataframe):
    years = [g for n, g in dataframe.groupby(pd.Grouper(key='Time',freq='Y'))]
    for i in range(len(years)):
        year = pd.DataFrame(years[i])
        year.to_csv(f"year_{i}.csv", columns=["Time", "Price"], index=False)
    print(years)
    return years
def split_to_weekly(dataframe):
    weeks = [g for n, g in dataframe.groupby(pd.Grouper(key='Time',freq='W'))]
    for i in range(len(weeks)):
        week = pd.DataFrame(weeks[i])
        week.to_csv(f"week_{i}.csv", columns=["Time", "Price"], index=False)
    print("Weeks", weeks)
    return weeks

def split_to_monthly(dataframe):
    months = [g for n, g in dataframe.groupby(pd.Grouper(key='time',freq='M'))]
    for i in range(len(months)):
        month = pd.DataFrame(months[i])
        month.to_csv(f"month_{i}.csv", columns=["Time", "Price"], index=False)
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
    df = pd.read_csv(path, parse_dates=['Time'])
    series = df.loc[:, 'Close'].values
    # # Plotting the graph of the date against the close
    # df.plot(figsize=(14,8), label="Close Price", title='Series', marker=".")
    # plt.ylabel("Close Prices")
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
