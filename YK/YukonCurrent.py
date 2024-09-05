"""
CCEI Web Scraping - YUKON (New Format)
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
import re

def getYearFromFileName(filename):
    match = re.match(r'.*([1-3][0-9]{3})', filename)
    if match is not None:
        print(match.group(1))
        return match.group(1)

debug = True

dst = {'2001':['2001-04-01','2001-10-28'],
       '2002':['2002-04-07','2002-10-27'],
       '2003':['2003-04-06','2003-10-26'],
       '2004':['2004-04-04','2004-10-31'],
       '2005':['2005-04-03','2005-10-30'],
       '2006':['2006-04-02','2006-10-29'],
       '2007':['2007-03-11','2007-11-04'],
       '2008':['2008-03-09','2008-11-02'],
       '2009':['2009-03-08','2009-11-01'],
    #    '2009':['8-3-2009','28-11-2009'],
       '2010':['2010-03-14','2010-11-07'],
       '2011':['2011-03-13','2011-11-06'],
       '2012':['2012-03-11','2012-11-04'],
       '2013':['2013-03-10','2013-11-03'],
       '2014':['2014-03-09','2014-11-02'],
       '2015':['2015-03-08','2015-11-01'],
       '2016':['2016-03-13','2016-11-06'],
       '2017':['2017-03-12','2017-11-05'],
       '2018':['2018-03-11','2018-11-04'],
       '2019':['2019-03-10','2019-11-03'],
       '2020':['2020-03-08','2020-11-01'],
    #    '2020':['08-03-2020','01-11-2020'],
       '2021':['2021-03-14','2021-11-07'],
       '2022':['2022-03-13','2022-11-06'],
    #    '2022':['13-03-2022','06-11-2022'],
       '2023':['2023-03-12','2023-11-05'],
       '2024':['2024-03-10','2024-11-03']}
        # '2023':['12-03-2023','05-11-2023']}

# read directory
for x in os.walk('./ON/files/'):
    # read files
    print(x)
    for file in x[2]:

        df = pd.read_csv(
            './ON/files/' + file, skiprows=4, header=None, parse_dates=[0]
        )  # read in the file to a pandas dataframe
        # extract filename from url.
        fullpath = urlparse(file)
        print(
            "File path: " + fullpath.path
        )  # Output: /kyle/09-09-201315-47-571378756077.jpg
        print("Filename: " + os.path.basename(fullpath.path))
        filename = os.path.basename(fullpath.path)
        year = getYearFromFileName(filename)
        print("The year to process: " + year)
        df = df.rename(columns={0: "TIME_PERIOD", 1: "HOUR", 2: "OBS_VALUE"})
        # this part loops through the date and time fields, figures out if it's daylight savings time, and sets the local and daylight offsets
        local_offset = []
        daylight_offset = []

        # loop through the rows using iterrows()
        isInteger = 0
        # nov2023DLTime = datetime.strptime("2023-11-05", "%Y-%m-%d").date()
        # findDate = df[(df['TIME_PERIOD'] == "2001-10-28") & (df['HOUR'] == 2)]
        findDate = df[(df['TIME_PERIOD'] == dst[year][1]) & (df['HOUR'] == 2)]
        findIndex = findDate['HOUR'].index.values[0]
        df.loc[findIndex:findIndex,'HOUR'] = 1
        # print("******: " + str(df.loc[(df['TIME_PERIOD'] == "2001-10-28") & (df['HOUR'] == '2')]))
        print("******: " + str(df.loc[(df['TIME_PERIOD'] == dst[year][1]) & (df['HOUR'] == '2')]))
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

                # if str(date_time) != "2001-04-01 02:00:00" :
                if str(date_time) != dst[year][0] + " 02:00:00" :
                # and str(date_time) != "2022-03-13 02:00:00" and str(date_time) != "2021-03-14 02:00:00" and str(date_time) != "2020-03-08 02:00:00" and str(date_time) != "2019-03-10 02:00:00" and str(date_time) != "2018-03-11 02:00:00" and str(date_time) != "2017-03-12 02:00:00" and str(date_time) != "2016-03-13 02:00:00" and str(date_time) != "2015-03-08 02:00:00" and str(date_time) != "2014-03-09 02:00:00" and str(date_time) != "2013-03-10 02:00:00" and str(date_time) != "2012-03-11 02:00:00" and str(date_time) != "2011-03-13 02:00:00" and str(date_time) != "2010-03-14 02:00:00" and str(date_time) != "2009-03-08 02:00:00" and str(date_time) != "2008-03-09 02:00:00" and str(date_time) != "2007-03-11 02:00:00" and str(date_time) != "2006-04-02 02:00:00" and str(date_time) != "2005-04-03 02:00:00" and str(date_time) != "2004-04-04 02:00:00" and str(date_time) != "2003-04-06 02:00:00" and str(date_time) != "2002-04-07 02:00:00" and str(date_time) != "2001-04-01 02:00:00":
                    date_time = date_time.tz_localize(tz="America/Toronto", ambiguous=True)
                    if date_time.dst():
                        local_offset.append(4)
                        daylight_offset.append(1)
                    else:
                        local_offset.append(5)
                        daylight_offset.append(0)
                else:
                    local_offset.append(5)
                    daylight_offset.append(0)
            else: # replace 2* to 1 in november.
                df.loc[df['HOUR'] == "2*", 'HOUR'] = 1
                print("Replace 2* value to 1")

        ################################ Time correction END #######################################

        # create new columns
        df["STRUCTURE"] = "DATAFLOW"
        df["STRUCTURE_ID"] = "CCEI:DF_HFED_ON(1.0)"
        df["ACTION"] = "I"
        df["DAYLIGHT_OFFSET"] = daylight_offset
        # df["DATETIME_LOCAL_OFFSET"] = local_offset
        df["DATETIME_LOCAL_OFFSET"] = 5
        df["DATETIME_LOCAL"] = (
            df["TIME_PERIOD"] + pd.to_timedelta(df["HOUR"], unit="h")
        ).astype(str)
        df["DATETIME_LOCAL"] = (
            df["DATETIME_LOCAL"].str.slice(0, 10)
            + "T"
            + df["DATETIME_LOCAL"].str.slice(11, 20)
        )
        df["TIME_PERIOD"] = (
            # df["TIME_PERIOD"] + pd.to_timedelta(df["HOUR"] + df["DATETIME_LOCAL_OFFSET"], unit="h")
            df["TIME_PERIOD"] + pd.to_timedelta(df["HOUR"] + df["DATETIME_LOCAL_OFFSET"], unit="h")
        ).astype(str)
        df["TIME_PERIOD"] = (
            df["TIME_PERIOD"].str.slice(0, 10) + "T" + df["TIME_PERIOD"].str.slice(11, 20)
        )
        df["UNIT_MEASURE"] = "MW"
        df["FREQ"] = "H"
        df["REF_AREA"] = "CA_ON"
        df["COUNTERPART_AREA"] = "_Z"
        df["ENERGY_FLOWS"] = "HOEP"
        df["UNIT_MULT"] = "0"
        df["MEASURE_TYPE"] = "ENERGY"
        df["OBS_STATUS"] = "A"
        df["CONF_STATUS"] = "F"

        df = df.drop(columns=["HOUR"])

        # correct column order
        df = df[
            [
                "STRUCTURE",
                "STRUCTURE_ID",
                "ACTION",
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
        
        df.to_csv("./ON/output/" + filename, index=False)