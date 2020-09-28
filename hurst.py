import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from hurst import compute_Hc, random_walk

df = pd.read_csv("/Volumes/TICK/spreadsheets/all_files.csv")
series = pd.Series(df["Price"])
