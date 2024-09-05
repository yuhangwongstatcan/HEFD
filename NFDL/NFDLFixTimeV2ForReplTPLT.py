import urllib.request as urllib2
import requests
import os
import time
import pandas as pd
import datetime as dt
from datetime import timedelta
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from datetime import datetime

dst = {'2022':['2022-03-12','2022-11-07'],
       '2023':['2023-03-12','2023-11-06'],
       '2024':['2024-03-10','2024-11-04']}


# read directory
for x in os.walk('./NFDL/files/'):
    # read files
    print(x)
    for file in x[2]:
        print("File: " + file)
        df = pd.read_csv('./NFDL/files/'+file)
        # print(df)
        # template: HOUR,NB_LOAD,NB_DEMAND,ISO_NE,NMISA,QUEBEC,NOVA_SCOTIA,PEI
        filename = file
                    
        str_time = str(df["TIME_PERIOD"].iloc[0])
        # print(str_time)
        dt.datetime.strptime(str_time, "%Y-%m-%dT%H:%M:%S")
        # start to parse CSV files.
        # to set time offset.
        date_time_local = dt.datetime.strptime(str_time, "%Y-%m-%dT%H:%M:%S") # convert string to datetime format
        
        # set the timezone to utc and figure out if it's daylight savings time
        date_time_utc = pd.to_datetime(date_time_local)
        time_period = date_time_utc.strftime("%Y-%m-%dT%H:%M:%S")
        
        #file_time = pd.to_datetime(date_time)
        file_time = date_time_utc.strftime("%Y%m%d%H%M%S")
        # print(file_time)

        # print(df)
        # create new columns datetime is Dec 12, 2023 13:43:44 
        df['DATAFLOW'] = "CCEI:DF_HFED_NL(1.0)"
        # df['FREQ'] = "N"
        df['REF_AREA'] = "CA_NL"
        df['COUNTERPART_AREA'] = "_Z"
        df['DAYLIGHT_OFFSET'] = ""
        df['DATETIME_LOCAL_OFFSET'] = ""
        # df['DATETIME_LOCAL'] = date_time_local
        # df['TIME_PERIOD'] = date_time_utc
        df['UNIT_MEASURE'] = "MW"
        df['UNIT_MULT'] = "0"
        df['MEASURE_TYPE'] = "ENERGY"
        df['OBS_STATUS'] = "A"
        df['CONF_STATUS'] = "F"

        # loop through all the records and change the time_period to utc
        for index, row in df.iterrows():

            # date = row['TIME_PERIOD']
            date = df.at[index, 'DATETIME_LOCAL']
            # fix local date from 2024-03-10T06:30:00 minus 1 : -1 hour
            # print(date)
            datetime =  dt.datetime.strptime(date, "%Y-%m-%dT%H:%M:%S")
            month = str(datetime.month)
            day = str(datetime.day)
            hour = str(datetime.hour)      
            # print("hour: " + date)

            baddate = False
            if date >= "2024-03-10T06:30:00" :
                # print("--------" + date)
                baddate = True

            for item in dst:
                # print(dst[item])
                if(item in str(date)):

                    if (date >= dst[item][0] and date <= dst[item][1]):
                        local_offset = 2.5
                        daylight_offset = 1
                    else:
                        local_offset = 3.5
                        daylight_offset = 0 
            
            date = pd.to_datetime(date)
            if baddate:
                date = date - timedelta(hours=1) 
                
            # date = date.tz_localize(tz.gettz('Canada/St_Johns'), ambiguous=False).astimezone(tz.gettz('UTC'))
            dateutc = date + timedelta(hours=local_offset)
            dateutc = dateutc.strftime("%Y-%m-%dT%H:%M:%S")
            date = date.strftime("%Y-%m-%dT%H:%M:%S")
            df.at[index, 'DATETIME_LOCAL'] = str(date)
            df.at[index, 'TIME_PERIOD'] = dateutc
            df.at[index, 'DAYLIGHT_OFFSET'] = daylight_offset
            df.at[index, 'DATETIME_LOCAL_OFFSET'] = local_offset
            
        # correct column order
        
        # display(df)

        df.to_csv("./NFDL/output/"+file, index=False)