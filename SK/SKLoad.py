"""
CCEI Web Scraping - Sask
Data updated every 5 minutes
https://www.saskpower.com/our-power-future/our-electricity/electrical-system/where-your-power-comes-from
"""
import pandas as pd
import urllib.request
import datetime as dt
from dateutil import tz
import pytz
import os
import requests
import json
import csv
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# after NOV datenight -4 between date -3
dst = { 
       '2012':['2012-03-11 03:00:00','2012-11-04 02:00:00'],
       '2013':['2013-03-10 03:00:00','2013-11-03 02:00:00'],
       '2014':['2014-03-09 03:00:00','2014-11-02 02:00:00'],
       '2015':['2015-03-08 03:00:00','2015-11-01 02:00:00'],
       '2016':['2016-03-13 03:00:00','2016-11-06 02:00:00'],
       '2017':['2017-03-12 03:00:00','2017-11-05 02:00:00'],
       '2018':['2018-03-11 03:00:00','2018-11-04 02:00:00'],
       '2019':['2019-03-10 03:00:00','2019-11-03 02:00:00'],
       '2020':['2020-03-08 03:00:00','2020-11-01 02:00:00'],
       '2021':['2021-03-14 03:00:00','2021-11-07 02:00:00'],
       '2022':['2022-03-13 03:00:00','2022-11-06 02:00:00'],
       '2023':['2023-03-12 03:00:00','2023-11-05 02:00:00'],
       '2024':['2024-03-10 03:00:00','2024-11-03 02:00:00']}
    
# create CSV file.
with open('./PEI/output/island_load.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    field = ["STRUCTURE","STRUCTURE_ID","ACTION","FREQ","REF_AREA","COUNTERPART_AREA","ENERGY_FLOWS","DATETIME_LOCAL","OBS_VALUE","TIME_PERIOD","DAYLIGHT_OFFSET","DATETIME_LOCAL_OFFSET","UNIT_MEASURE","UNIT_MULT","MEASURE_TYPE","OBS_STATUS","CONF_STATUS"]
    writer.writerow(field)

def writeToFile(obsval, date, filename):
    # print(date, obsval)
    # create CSV file.
    with open('./PEI/output/'+filename+'.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        # writer.writerow(["Oladele Damilola", "40", "Nigeria"])
        # convert time.
        offset = 4
        year = dt.datetime.strptime(date, "%Y-%m-%d %H:%M:%S").year
        if((dt.datetime.strptime(date, "%Y-%m-%d %H:%M:%S") >= dt.datetime.strptime(dst[str(year)][0], "%Y-%m-%d %H:%M:%S")) 
           and (dt.datetime.strptime(date, "%Y-%m-%d %H:%M:%S") <= dt.datetime.strptime(dst[str(year)][1], "%Y-%m-%d %H:%M:%S"))):
            offset = 3
        date_time_local = dt.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        date_time_local = date_time_local.strftime("%Y-%m-%dT%H:%M:%S")

        # print("LOCAL TIME: " + date_time_local)

        time_period = dt.datetime.strptime(date, "%Y-%m-%d %H:%M:%S") + timedelta(hours=offset)
        time_period = time_period.strftime("%Y-%m-%dT%H:%M:%S")

        writer.writerow(["DATAFLOW","CCEI:DF_HFED_PE(1.0)","I",
                         "N","CA_PE","_Z","ON_ISL_LOAD",date_time_local,
                         obsval,time_period,"0.0","4.0","MW","0","ENERGY","A","F"])
        

soup = BeautifulSoup(html_doc, 'html.parser')

# for x in os.walk('./PEI/files/'):
#     # read files
#     print(x)
#     for file in x[2]: # loop files.
#         print("File: " + file)
#         df = pd.read_csv('./PEI/files/'+file)
#         print(df)
#         filename = file
        
#         df = df.rename(columns={"updatetime": "DATETIME_LOCAL"})
        
#         # iterate through the dataframe.
#         # iterate through each row and select
#         # 'Name' and 'Stream' column respectively.
#         for ind in df.index:
            
#             writeToFile(df['on-island-load'][ind], df['DATETIME_LOCAL'][ind],"island_load")
#             writeToFile(df['on-island-wind'][ind], df['DATETIME_LOCAL'][ind],"island_wind")
#             writeToFile(df['on-island-fossil'][ind], df['DATETIME_LOCAL'][ind],"islandfossil")
#             writeToFile(df['wind-local'][ind], df['DATETIME_LOCAL'][ind],"windlocal")
#             writeToFile(df['wind-export'][ind], df['DATETIME_LOCAL'][ind],"windeport")
#             writeToFile(df['percentage-wind'][ind], df['DATETIME_LOCAL'][ind],"percentagewind")
#             writeToFile(df['cables-import'][ind], df['DATETIME_LOCAL'][ind],"cablesimport")
#             # print(df['DATETIME_LOCAL'][ind], df['on-island-load'][ind])

