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
    df = pd.read_csv("/Volumes/TICK/spreadsheets/all_files.csv")
    df['Time'] = pd.to_datetime(df['Time'], format="%Y-%m-%d %H:%M:%S")

    print("Splitting into the initial minutes")
    path = split_minute(dataframe=df)
    print("Reading into the minutes.csv file")
    df = pd.read_csv(f"{path}")
    df['Time'] = pd.to_datetime(df['Time'], format="%Y-%m-%d %H:%M:%S")

    print("Splitting data...")
    # data = split(dataframe=df)  # Yearly
    # data = split_to_monthly(dataframe=df)   # Monthly
    # data = split_to_weekly(dataframe=df)    # Weekly
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
Splitting the data into yearly, weekly and monthly values
"""
def split_minute(dataframe):
    mins = [g for n, g in dataframe.groupby(pd.Grouper(key='Time',freq='Min'))]
    minutes = list()
    month_codes = {"F":1, "G":2, "H":3, "J":4, "K":5, "M":6, "N":7, "Q":8, "U":9, "V":10, "X":11, "Z":12}
    for i in range(len(mins)):
        min_data = pd.DataFrame(mins[i])
        print("min_data: \n", min_data.head(3))
        if min_data.empty:
            print("EMPTY:", i)
            del min_data
        else:
            # Reset the index
            min_data.reset_index(drop=True, inplace=True)
            mth_num = np.array([])
            sr = pd.Series(min_data["Mth_Code"])

            # To filter out the required month codes
            for item in sr.iteritems():
                mth_num = np.append(mth_num, month_codes[item[1]])

            print("mth_num", mth_num)
            min_data["Mth_num"] = mth_num
            min_data['Months'] = pd.DatetimeIndex(min_data['Time']).month
            min_data['diff'] = min_data["Mth_num"] - min_data['Months']
            print("Futures market 3 mnths ahead")
            print(min_data)
            min_data.drop(min_data[min_data["diff"] != 3].index, inplace=True) # Change the number here in order to change the number of years ahead you want the furtures market

            # If all the rows are deleted continue with the next min_data
            if min_data.empty:
                print("None:", i)
                del min_data
                continue

            min_data.reset_index(drop=True, inplace=True)
            time = min_data.at[0, 'Time']
            min_data['Time'] = time

            min_data['Open'] = min_data['Price'][0]
            min_data['Close'] = min_data['Price'][len(min_data) - 1]
            # df.insert(2, 'Close', int(min_data.at[len(min_data.index), 'Price']))

            # Sorting the values to get high low
            print("min_data and i is \n", min_data, i)
            # df.insert(3, 'High', min_data.at[len(min_data.index), 'Price'])
            min_data.sort_values(by=['Price'], ascending=True, inplace=True)
            min_data.reset_index(drop=True, inplace=True)
            min_data['Low'] = min_data['Price'][0]
            min_data['High'] = min_data['Price'][len(min_data)-1]
            print("min_data.head() is: \n", min_data.head(5))

            print("min_data.head() is: \n", min_data.head(5))
            print("min_data.columns is: \n", min_data.columns)

            minutes.append(min_data)

    print("minutes is: \n", minutes[0:10], type(minutes))

    try:
        minutes = pd.concat(minutes, axis=0, ignore_index=True)
        minutes.to_csv("minutes.csv")
    except Exception as e:
        print(e)

    return f"minutes.csv"

def split(dataframe):
    years = [g for n, g in dataframe.groupby(pd.Grouper(key='Time',freq='Y'))]
    for i in range(len(years)):
        year = pd.DataFrame(years[i])
        year.to_csv(f"year_{i}.csv", columns=["Time", "Close"], index=False)
    print(years)
    return years
def split_to_weekly(dataframe):
    weeks = [g for n, g in dataframe.groupby(pd.Grouper(key='Time',freq='W'))]
    for i in range(len(weeks)):
        week = pd.DataFrame(weeks[i])
        week.to_csv(f"week_{i}.csv", columns=["Time", "Close"], index=False)
    print("Weeks", weeks)
    return weeks

def split_to_monthly(dataframe):
    months = [g for n, g in dataframe.groupby(pd.Grouper(key='time',freq='M'))]
    for i in range(len(months)):
        month = pd.DataFrame(months[i])
        month.to_csv(f"month_{i}.csv", columns=["Time", "Close"], index=False)
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
        time = str(matrix[i]["Time"]).split()[1]
        print("type", type(matrix[i]["Time"]))
        times.append(time)
        statistics = adf_test(f"{timeframe}_{i}.csv")
        print("stats", statistics)
        ADF_stats.append(statistics[0])
        p_value.append(statistics[1])
        used_lag.append(statistics[2])
        os.remove(f"{timeframe}_{i}.csv")
    data["Time"] = times
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
"""
Loading the data into a dataframe
"""
def load_data(directory="/Volumes/TICK/required_info"):
    month_codes = {"F":1, "G":2, "H":3, "J":4, "K":5, "M":6, "N":7, "Q":8, "U":9, "V":10, "X":11, "Z":12}
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
        del csv_file['Log_Time']
        del csv_file['Year']
        csv_file = csv_file.loc[(csv_file['Msg_Code'] == "T")]
        all_files.append(csv_file)

    all_files = pd.concat(all_files, axis=0, ignore_index=True)

    print(f"All files header\n{all_files.head(5)}")
    all_files.to_csv("all_files.csv")
    return pd.DataFrame(all_files)


if __name__ == "__main__":
    main()
