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
import xlrd
from datetime import datetime

ssl._create_default_https_context = ssl._create_unverified_context
debug = True
historical_url = [
    "https://www.bchydro.com/content/dam/BCHydro/customer-portal/documents/corporate/suppliers/transmission-system/balancing_authority_load_data/Historical%20Transmission%20Data/BalancingAuthorityLoadApr-Dec2001.xls"
]

for url in historical_url:
    df = pd.read_excel(
        url, skiprows=2, header=None, parse_dates=[0]
    )  # read in the file to a pandas dataframe
    # extract filename from url.
    fullpath = urlparse(url)
    print(
        "File path: " + fullpath.path
    )  # Output: /kyle/09-09-201315-47-571378756077.jpg
    print("Filename: " + os.path.basename(fullpath.path))
    filename = os.path.basename(fullpath.path)
    df = df.rename(columns={0: "TIME_PERIOD", 1: "HOUR", 2: "OBS_VALUE"})
    file_time = pd.to_datetime(df["HOUR"][0]).strftime("%Y%m%d%H%M%S")
    # this part loops through the date and time fields, figures out if it's daylight savings time, and sets the local and daylight offsets
    local_offset = []
    daylight_offset = []

    # loop through the rows using iterrows()
    isInteger = 0
    # nov2023DLTime = datetime.strptime("2023-11-05", "%Y-%m-%d").date()
    findDate = df[(df['TIME_PERIOD'] == "2001-10-28") & (df['HOUR'] == 2)]
    findIndex = findDate['HOUR'].index.values[0]
    df.loc[findIndex:findIndex,'HOUR'] = 1
    print("******: " + str(df.loc[(df['TIME_PERIOD'] == "2001-10-28") & (df['HOUR'] == '2')]))
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

            if str(date_time) != "2001-04-01 02:00:00" :
            # and str(date_time) != "2022-03-13 02:00:00" and str(date_time) != "2021-03-14 02:00:00" and str(date_time) != "2020-03-08 02:00:00" and str(date_time) != "2019-03-10 02:00:00" and str(date_time) != "2018-03-11 02:00:00" and str(date_time) != "2017-03-12 02:00:00" and str(date_time) != "2016-03-13 02:00:00" and str(date_time) != "2015-03-08 02:00:00" and str(date_time) != "2014-03-09 02:00:00" and str(date_time) != "2013-03-10 02:00:00" and str(date_time) != "2012-03-11 02:00:00" and str(date_time) != "2011-03-13 02:00:00" and str(date_time) != "2010-03-14 02:00:00" and str(date_time) != "2009-03-08 02:00:00" and str(date_time) != "2008-03-09 02:00:00" and str(date_time) != "2007-03-11 02:00:00" and str(date_time) != "2006-04-02 02:00:00" and str(date_time) != "2005-04-03 02:00:00" and str(date_time) != "2004-04-04 02:00:00" and str(date_time) != "2003-04-06 02:00:00" and str(date_time) != "2002-04-07 02:00:00" and str(date_time) != "2001-04-01 02:00:00":
                date_time = date_time.tz_localize(tz="America/Vancouver", ambiguous=True)
                if date_time.dst():
                    local_offset.append(7)
                    daylight_offset.append(1)
                else:
                    local_offset.append(8)
                    daylight_offset.append(0)
            else:
                local_offset.append(8)
                daylight_offset.append(0)
        else: # replace 2* to 1 in november.
            df.loc[df['HOUR'] == "2*", 'HOUR'] = 1
            print("Replace 2* value to 1")

    ################################ Time correction END #######################################

    # create new columns
    df["DATAFLOW"] = "CCEI:DF_HFED_BC(1.0)"
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
    df["FREQ"] = "H"
    df["REF_AREA"] = "CA_BC"
    df["COUNTERPART_AREA"] = "_Z"
    df["ENERGY_FLOWS"] = "LOAD"
    df["UNIT_MULT"] = "0"
    df["MEASURE_TYPE"] = "ENERGY"
    df["OBS_STATUS"] = "A"
    df["CONF_STATUS"] = "F"

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
    
    df.to_csv("./BC/LOAD/output/" + filename + ".csv")
