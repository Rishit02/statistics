import os
import sys
import pandas as pd
import scipy.stats as stats
from datetime import datetime
import numpy as np
from numpy import log, polyfit, sqrt, std, subtract
import statsmodels.tsa.stattools as ts
import statsmodels.api as sm
import matplotlib.pyplot as plt

def main():
    print("hello")
    m_num = 2
    column = f"PlotCandle ({m_num}M)"
    df = load_data()
    plot(df=df, column=column)
    data_frame = calculation(df=df, column=column)
    print(data_frame)
    data_frame.to_csv(f"z-score_{m_num}M win=3.csv", index=False)

def load_data(directory=f"/Volumes/TICK/spreadsheets/SGX_DLY_TFX2020, 5.csv"):
    df = pd.DataFrame(pd.read_csv(directory))
    df['DateText'] = pd.to_datetime(df['DateText'], format="%Y-%m-%dT%H:%M:%SZ")
    df = df.set_index(['DateText'])
    del df['Date']
    del df['Time']
    return df

def calculation(df, column):

    # Half-life calculation
    z_array = df[column]
    z_array = z_array.to_numpy()
    z_lag = np.roll(z_array,1)
    z_lag[0] = 0
    z_ret = z_array - z_lag
    z_ret[0] = 0

    #adds intercept terms to X variable for regression
    z_lag2 = sm.add_constant(z_lag)

    model = sm.OLS(z_ret,z_lag2)
    res = model.fit()

    halflife = -log(2) / res.params[1]
    print(f"The half-life is: {halflife}")
    halflife = int(halflife)

    # Other stat calculations
    df['average'] = df[column].rolling(window=halflife).mean(skipna=True)
    df['std_deviation'] = df[column].rolling(window=halflife).std()
    df['z_score'] = (df[column] - df['average']) / df['std_deviation']

    return df

def plot(df, column):
    df = df[column]
    df.plot(figsize=(14,8), label="Price", title='Series', marker=".")
    plt.ylabel("Prices")
    plt.legend()
    plt.show()

if __name__ == "__main__":
    main()
