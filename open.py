# Look at the tick data for October and then using file 07
import os
import time
import pandas as pd
import requests, zipfile, io

# Doing 4500 to 4700 (4500-4699 inclusive)
# for num in range(4400, 4500):
# for num in range(4300, 4400):
def main():
    num_hvnt_worked = list()
    print("Downloading data...")
    for num in range(4700, 4750):
        try:
            print(f"Downloading zip no. {num} is in progress")
            download_zip(f"https://links.sgx.com/1.0.0/derivatives-historical/{num}/WEBPXTICK_DT.zip")
            print(f"Zip {num} has been downloaded")

        except Exception as e:
            print(f"This is the exception that has occured for {num}: \n{e}")
            num_hvnt_worked.append(num)

    print("Parsing through required data...")
    parser()
    print(num_hvnt_worked)

def parser(thumb_drive_directory="/Volumes/TICK/tick_info"):
    thumb_drive_list = os.listdir(thumb_drive_directory)
    index = 0
    for paths in thumb_drive_list:
        index += 1
        # Reading in the relevant information from the big file
        df = pd.read_csv(f"{thumb_drive_directory}/{paths}")
        # Converting the relevant information into a csv file
        date = str(paths)
        date = paths.split('.')[0]
        date = date.split('-')[1]
        df = df.loc[(df['Comm'] == "TF")] # Only require TF right now
        df.to_csv(f"/Volumes/TICK/required_info/info{date}.csv", index=False)
        print(f"Index has been made with Filename /Volumes/TICK/required_info/info{date}.csv")
        os.remove(f"{thumb_drive_directory}/{paths}") # Delete the big zip file collected
        print(f"{thumb_drive_directory}/{paths} has been removed")
    print(f"Total files deleted: {index}")

def download_zip(zip_file_url):
    """
    Takes a single argument: the zip url that we require
    Stores the downloaded zip file in the usb/thumb_drive_directory
    """
    try:
        r = requests.get(zip_file_url, stream=True)
        print("r")
        z = zipfile.ZipFile(io.BytesIO(r.content))
        print("z")
        z.extractall("/Volumes/TICK/tick_info")
        print("ze")
        print("We are gonna take a break before going for the next one")
        time.sleep(10)
    except requests.exceptions.ConnectionError:
        r.status_code = "Connection refused"
        print("ConnectionError")
        time.sleep(10)


# def split(dataframe):
#     years = [g for n, g in dataframe.groupby(pd.Grouper(key='time',freq='Y'))]
#     for i in range(len(years)):
#         year = pd.DataFrame(years[i])
#         year.to_csv(f"year_{i}.csv", columns=["time", "Price"], index=False)
#     print(years)
#     return years
# def split_to_weekly(dataframe):
#     weeks = [g for n, g in dataframe.groupby(pd.Grouper(key='time',freq='W'))]
#     for i in range(len(weeks)):
#         week = pd.DataFrame(weeks[i])
#         week.to_csv(f"week_{i}.csv", columns=["time", "Price"], index=False)
#     print("Weeks", weeks)
#     return weeks
#
# def split_to_monthly(dataframe):
#     months = [g for n, g in dataframe.groupby(pd.Grouper(key='time',freq='M'))]
#     for i in range(len(months)):
#         month = pd.DataFrame(months[i])
#         month.to_csv(f"month_{i}.csv", columns=["time", "Price"], index=False)
#     print("Months", months)
#     return months

if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))
