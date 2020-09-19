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
    

if __name__ == "__main__":
    main()
