import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from hurst import compute_Hc

def main():
    df = pd.read_csv("minutes.csv")
    series = pd.Series(df["Close"])

    # Evaluate Hurst equation
    H, c, data = compute_Hc(series, kind='price', simplified=True)
    print(f"H={H:.4f}, c={c:.4f}")

    # Plot
    f, ax = plt.subplots()
    ax.plot(data[0], c*data[0]**H, color="deepskyblue")
    ax.scatter(data[0], data[1], color="purple")
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('Time interval')
    ax.set_ylabel('R/S ratio')
    ax.grid(True)
    plt.show()

if __name__ == "__main__":
    main()
