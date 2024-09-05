"""
CCEI Web Scraping - British Colombia (New Format)
Data updated every 5 minutes
For NSI https://www.bchydro.com/energy-in-bc/operations/transmission/transmission-system/actual-flow-data/historical-data.html
"""
import pandas as pd
import urllib.request
import datetime as dt
from dateutil import tz
import pytz
import os
import requests
import json
from urllib.parse import urlparse
import ssl
from datetime import datetime

ssl._create_default_https_context = ssl._create_unverified_context
debug = True
# url = "https://www.bchydro.com/content/dam/BCHydro/customer-portal/documents/corporate/suppliers/transmission-system/actual_flow_data/historical_data/HourlyTielineData2023.xls"
url = "https://www.bchydro.com/content/dam/BCHydro/customer-portal/documents/corporate/suppliers/transmission-system/actual_flow_data/historical_data/HourlyTielineData2022.xls"
# url = "https://www.bchydro.com/content/dam/BCHydro/customer-portal/documents/corporate/suppliers/transmission-system/actual_flow_data/historical_data/HourlyTielineData2021.xls"

df = pd.read_excel(
    url, skiprows=2, header=None, parse_dates=[0]
)  # read in the file to a pandas dataframe
# extract filename from url.
fullpath = urlparse(url)
print(
    "File path: " + fullpath.path
) 
print("Filename: " + os.path.basename(fullpath.path))
filename = os.path.basename(fullpath.path)
df = df.rename(columns={0: "TIME_PERIOD", 1: "HOUR", 3: "OBS_VALUE"})
file_time = pd.to_datetime(df["HOUR"][1]).strftime("%Y%m%d%H%M%S")
# this part loops through the date and time fields, figures out if it's daylight savings time, and sets the local and daylight offsets
local_offset = []
daylight_offset = []
# loop through the rows using iterrows()
isInteger = 0
# nov2023DLTime = datetime.strptime("2023-11-05", "%Y-%m-%d").date()
# find first daylight saving day
# =8 Mar =7 Nov =8
findDate = df[(df['TIME_PERIOD'] == "2022-11-06") & (df['HOUR'] == 2)]
print(df['TIME_PERIOD'])
print(findDate)
findIndex = findDate['HOUR'].index.values[0]
df.loc[findIndex:findIndex,'HOUR'] = 1 # change day hour.
print("******: " + str(df.loc[(df['TIME_PERIOD'] == "2022-11-06") & (df['HOUR'] == '2')]))
for index, row in df.iterrows():
    if isinstance(row['HOUR'], int):
        isInteger+=1
    else:
        print("Not Integer: " + str(index) +" : " + row['HOUR'])

for (date, time) in zip(df["TIME_PERIOD"], df["HOUR"]):
    if(time == "2*"): # replace 2* to 1 in november.
        df.loc[df['HOUR'] == "2*", 'HOUR'] = 2
        print("Replace 2* value to 1")
# loop through all the rows in dataframe.
for (date, time) in zip(df["TIME_PERIOD"], df["HOUR"]):
    if(time != "2*"):
        date_time = date + pd.to_timedelta(time, unit="h")
        month = str(date_time.month)
        day = str(date_time.day)
        time = str(date_time.time)        
        if date_time > datetime(2022, 3, 13) and date_time < datetime(2022, 11, 6):
            local_offset.append(7)
            daylight_offset.append(1)
        elif date_time <= datetime(2022, 3, 13) or date_time >= datetime(2022, 11, 6):
            local_offset.append(8)
            daylight_offset.append(0)
        

################################ Time correction END #######################################

# create new columns
df['DATAFLOW'] = "CCEI:DF_HFED_BC(1.0)"
df["DAYLIGHT_OFFSET"] = daylight_offset
df["DATETIME_LOCAL_OFFSET"] = local_offset
df["DATETIME_LOCAL"] = (
    df["TIME_PERIOD"] + pd.to_timedelta(df["HOUR"], unit="h")
).astype(str)
df["DATETIME_LOCAL"] = (
    df["DATETIME_LOCAL"].str.slice(0, 10)
    + "T"
    + df["DATETIME_LOCAL"].str.slice(11, 20)
)
df["TIME_PERIOD"] = (
    df["TIME_PERIOD"]
    + pd.to_timedelta(df["HOUR"] + df["DATETIME_LOCAL_OFFSET"], unit="h")
).astype(str)
df["TIME_PERIOD"] = (
    df["TIME_PERIOD"].str.slice(0, 10) + "T" + df["TIME_PERIOD"].str.slice(11, 20)
)
df["UNIT_MEASURE"] = "MW"
df["FREQ"] = "N"
df["REF_AREA"] = "CA_BC"
df["COUNTERPART_AREA"] = "CA_AB"
df["ENERGY_FLOWS"] = "NSI"
df["UNIT_MULT"] = "0"
df["MEASURE_TYPE"] = "ENERGY"
df["OBS_STATUS"] = "A"
df["CONF_STATUS"] = "F"
#********************

df = df.drop(columns=["HOUR"])

# correct column order
df = df[
    [
        "DATAFLOW",
        "FREQ",
        "REF_AREA",
        "COUNTERPART_AREA",
        "ENERGY_FLOWS",
        "TIME_PERIOD",
        "OBS_VALUE",
        "DATETIME_LOCAL",
        "DAYLIGHT_OFFSET",
        "DATETIME_LOCAL_OFFSET",
        "UNIT_MEASURE",
        "UNIT_MULT",
        "MEASURE_TYPE",
        "OBS_STATUS",
        "CONF_STATUS",
    ]
]
# if debug:
#     print(df)

df.to_csv("bcnsi2022ab.csv")
