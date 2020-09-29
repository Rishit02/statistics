import os
import sys
import pandas as pd
import scipy.stats as stats
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt

usecols = ['Time', 'Close']
df_2 = pd.read_csv('minutes_2.csv', usecols=usecols)[0:10]
df_3 = pd.read_csv('minutes_3.csv', usecols=usecols)[0:10]

print('Converting to Datetime format')
df_2['Time'] = pd.to_datetime(df_2['Time'], format="%Y-%m-%d %H:%M:%S")
df_3['Time'] = pd.to_datetime(df_3['Time'], format="%Y-%m-%d %H:%M:%S")

print('Grouping by minute')
mins_2 = [g for n, g in df_2.groupby(pd.Grouper(key='Time',freq='Min'))]
mins_3 = [g for n, g in df_3.groupby(pd.Grouper(key='Time',freq='Min'))]

print(mins_2)
print(mins_3)

if len(mins_2) > len(mins_3):
    length = len(mins_3)
else:
    length = len(mins_2)

df = pd.DataFrame()
for i in range(length):
    mins_2 = pd.DataFrame(mins_2[i])
    mins_3 = pd.DataFrame(mins_3[i])
    if mins_2.empty or mins_3.empty:
        continue
    else:
        df['Time'] = mins_2[i][0]
        df['2M'] = mins_2[i][1]
        df['3M'] = mins_2[i][2]
