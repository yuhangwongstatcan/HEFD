"""
CCEI Web Scraping - New Brunswick (New Format)
Data updated every 5 minutes
https://tso.nbpower.com/Public/en/system_information_archive.aspx
"""
import pandas as pd
import urllib.request
import datetime as dt
from dateutil import tz
import pytz
import os
import requests
import json

year_to_scan = ["2017"]
month_to_scan = ["1","2","3","4","5","6","7","8","9","10","11","12"]

for year in year_to_scan:
  
  for month in month_to_scan:
    print("Year: " + year + " Month: "+ month)
    # read directory
    for x in os.walk('./NB/csv/'):
        # read files
        print(x)
        for file in x[2]:
            print("File: " + file)
            df = pd.read_csv('./NB/csv/'+file)
            print(df)
            # template: HOUR,NB_LOAD,NB_DEMAND,ISO_NE,NMISA,QUEBEC,NOVA_SCOTIA,PEI
            filename = file
            
            df = df.rename(columns={"HOUR": "TIME", "NB_LOAD": "LOAD", "NB_DEMAND": "DEMAND", "ISO_NE": "US_NE", "NMISA": "US_EMEC", "QUEBEC": "CA_QC", "NOVA_SCOTIA": "CA_NS", "PEI": "CA_PEI"})
                
            str_time = str(df["TIME"].iloc[0])
            print(str_time)
            dt.datetime.strptime(str_time, "%Y-%m-%d %H:%M")
            # start to parse CSV files.
            # to set time offset.
            date_time_local = dt.datetime.strptime(str_time, "%Y-%m-%d %H:%M") # convert string to datetime format
            
            # set the timezone to utc and figure out if it's daylight savings time
            date_time_utc = pd.to_datetime(date_time_local)
            date_time_utc = date_time_utc.tz_localize(tz.gettz('America/Moncton')).astimezone(tz.gettz('UTC'))
            time_period = date_time_utc.strftime("%Y-%m-%dT%H:%M:%S")
            print("local: " + str(date_time_local))
            print("date_time_utc: ", date_time_utc)
            print("time_period: ", time_period)

            #file_time = pd.to_datetime(date_time)
            file_time = date_time_utc.strftime("%Y%m%d%H%M%S")
            print(file_time)

            if (date_time_utc.dst()):
                local_offset = 3
                daylight_offset = 1
            else:
                local_offset = 4
                daylight_offset = 0

            df["TIME"] = pd.to_datetime(df["TIME"])
            # df["TIME"] = df['TIME'].dt.tz_localize(tz.gettz('America/Moncton'))
            df['TIME'] = df['TIME'].dt.strftime("%Y-%m-%dT%H:%M:%S")

            # print(df)
            # create new columns datetime is Dec 12, 2023 13:43:44 
            df['DATAFLOW'] = "CCEI:DF_HFED_NB(1.0)"
            df['FREQ'] = "H"
            df['REF_AREA'] = "CA_NB"
            #df['COUNTERPART_AREA'] = ""
            df['DAYLIGHT_OFFSET'] = daylight_offset
            df['DATETIME_LOCAL_OFFSET'] = local_offset
            df['TIME_PERIOD'] = df['TIME']
            df['DATETIME_LOCAL'] = df['TIME']
            df['UNIT_MEASURE'] = "MW"
            df['UNIT_MULT'] = "0"
            df['MEASURE_TYPE'] = "ENERGY"
            df['OBS_STATUS'] = "A"
            df['CONF_STATUS'] = "F"
            df['RM_10'] = ""
            df['RM_30'] = ""
            df['SRM_10'] = ""
            df['US_MPS'] = ""

            # loop through all the records and change the time_period to utc
            for index, row in df.iterrows():
                date = row['TIME_PERIOD']
                date = pd.to_datetime(date)
                date = date.tz_localize(tz.gettz('America/Moncton'), ambiguous=False).astimezone(tz.gettz('UTC'))
                date = date.strftime("%Y-%m-%dT%H:%M:%S")
                df.at[index, 'TIME_PERIOD'] = date
            # correct column order
            df = df[['DATAFLOW', 'FREQ', 'REF_AREA', 'TIME_PERIOD', 'LOAD', 'DEMAND', 'US_NE', 'US_EMEC', 'US_MPS', 'CA_QC', 'CA_NS', 'CA_PEI', 'RM_10', 'SRM_10', 'RM_30', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS']]
            #display(df)

            # Names of ‘variable’ and ‘value’ columns can be customized
            pdf = pd.melt(df, id_vars =['DATAFLOW', 'FREQ', 'REF_AREA', 'TIME_PERIOD', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS'], value_vars =['LOAD', 'DEMAND'],
                    var_name ='ENERGY_FLOWS', value_name ='OBS_VALUE')

            pdf[ 'COUNTERPART_AREA'] = "_Z"

            pdf = pdf[['DATAFLOW', 'FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD',  'OBS_VALUE', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS']]
            #display(pdf)

            df['ENERGY_FLOWS'] = "NSI"
            # Names of ‘variable’ and ‘value’ columns can be customized
            pdf2 = pd.melt(df, id_vars =['DATAFLOW', 'FREQ', 'REF_AREA',  'ENERGY_FLOWS',  'TIME_PERIOD', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS'], value_vars =['US_NE', 'US_EMEC', 'CA_QC', 'CA_NS', 'CA_PEI'],
                    var_name ='COUNTERPART_AREA', value_name ='OBS_VALUE')

            pdf2 = pdf2[['DATAFLOW', 'FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD',  'OBS_VALUE', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS']]
            #display(pdf2)

            frames = [pdf, pdf2]            

            result = pd.concat(frames)

            print(result)
            result.to_csv("./NB/output/csv/"+file, index=False)