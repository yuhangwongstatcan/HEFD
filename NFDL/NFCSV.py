import urllib.request as urllib2
import requests
import os
import time
import pandas as pd
import datetime as dt
from datetime import timedelta
from urllib.parse import urlparse
from bs4 import BeautifulSoup

year_to_scan = ["2024"]
month_to_scan = ["01","02","03","04","05","06","07","08","09","10","11","12"]

dst = {'2024':['2024-03-10','2024-11-03']}


# read directory
for x in os.walk('./NFDL/files/'):
    # read files
    print(x)
    for file in x[2]:
        print("File: " + file)
        df = pd.read_csv('./NFDL/files/'+file)
        print(df)
        # template: HOUR,NB_LOAD,NB_DEMAND,ISO_NE,NMISA,QUEBEC,NOVA_SCOTIA,PEI
        filename = file
                    
        str_time = str(df["DATETIME_LOCAL"].iloc[0])
        print(str_time)
        # dt.datetime.strptime(str_time, "%d-%b-%y %H:%M:%S")
        dt.datetime.strptime(str_time, "%Y-%m-%d %H:%M")
        # start to parse CSV files.
        # to set time offset.
        date_time_local = dt.datetime.strptime(str_time, "%Y-%m-%d %H:%M") # convert string to datetime format
        date_time_local = date_time_local.strftime("%Y-%m-%dT%H:%M:%S")
        # set the timezone to utc and figure out if it's daylight savings time
        date_time_utc = pd.to_datetime(date_time_local)
        time_period = date_time_utc.strftime("%Y-%m-%dT%H:%M:%S")
        
        #file_time = pd.to_datetime(date_time)
        file_time = date_time_utc.strftime("%Y%m%d%H%M%S")
        print(file_time)

        # df = df.rename(columns={0: "DATETIME_LOCAL", 1: "OBS_VALUE"})

        # print(df)
        # create new columns datetime is Dec 12, 2023 13:43:44 
        df['DATAFLOW'] = "CCEI:DF_HFED_NL(1.0)"
        # df['FREQ'] = "N"
        df['REF_AREA'] = "CA_NL"
        df['COUNTERPART_AREA'] = "_Z"
        df['DAYLIGHT_OFFSET'] = ""
        df['DATETIME_LOCAL_OFFSET'] = ""
        # df['DATETIME_LOCAL'] = date_time_local
        df['TIME_PERIOD'] = ""
        df['UNIT_MEASURE'] = "MW"
        df['UNIT_MULT'] = "0"
        df['MEASURE_TYPE'] = "ENERGY"
        df["ENERGY_FLOWS"] = "DEMAND"
        df['OBS_STATUS'] = "A"
        df['CONF_STATUS'] = "F"
        df['RM_10'] = ""
        df['RM_30'] = ""
        df['SRM_10'] = ""
        df['US_MPS'] = ""

        # loop through all the records and change the time_period to utc
        for index, row in df.iterrows():
            str_time = str(df.at[index, 'DATETIME_LOCAL'])
            print(str_time)
            date = dt.datetime.strptime(str_time, "%Y-%m-%d %H:%M")
            date_local = date.strftime("%Y-%m-%dT%H:%M:%S")
            df.at[index, 'DATETIME_LOCAL'] = date_local
            for item in dst:
                # print(dst[item])
                if(item in str(date_local)):

                    if (date_local > dst[item][0] and date_local < dst[item][1]):
                        local_offset = 2.5
                        daylight_offset = 1
                    else:
                        local_offset = 3.5
                        daylight_offset = 0
            
            
            date = pd.to_datetime(date)
            # date = date.tz_localize(tz.gettz('Canada/St_Johns'), ambiguous=False).astimezone(tz.gettz('UTC'))
            date = date + timedelta(hours=local_offset)
            date = date.strftime("%Y-%m-%dT%H:%M:%S")
            df.at[index, 'TIME_PERIOD'] = date
            df.at[index, 'DAYLIGHT_OFFSET'] = daylight_offset
            df.at[index, 'DATETIME_LOCAL_OFFSET'] = local_offset
            
        # correct column order
        
        # display(df)
        
        # df = df.drop('LocalDateTime', axis=1)

        df.to_csv("./NFDL/output/NEW"+file, index=False)