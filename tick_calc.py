import os
import sys
import pandas

def main():
  pass

def load_data(directory="files"):

    # Collect all teh csv files into one DataFrame
    all_files = pd.DataFrame()
    for file in os.listdir(directory):
        csv_file = pd.read_csv(f"file")
        all_files.append(csv_file, ignore_index=True)

    # Apply statistical tests on each dataframe
    all_files['mean'] = all_files.mean()
    all_files['std_deviation'] = all_files.std()

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

if __name__ == "__main__":
    main()
