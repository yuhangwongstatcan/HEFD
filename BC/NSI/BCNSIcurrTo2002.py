'''
CCEI Web Scraping - British Colombia (New Format)
Data updated every 5 minutes
'''
import pandas as pd
import urllib.request
import datetime as dt
from dateutil import tz
import pytz
import os
import requests
import json
import ssl


ssl._create_default_https_context = ssl._create_unverified_context
debug = True
url = "https://www.bchydro.com/content/dam/BCHydro/customer-portal/documents/corporate/suppliers/transmission-system/actual_flow_data/historical_data/HourlyTielineData2023.xls"

df_source = pd.read_excel(
        url, skiprows=1, header=None, parse_dates=[0]
    )  # read in the file to a pandas dataframe

if debug:
  print(df_source)


dfi = pd.DataFrame()

dfi = dfi.rename(columns={0: "TIME_PERIOD", 1: "HOUR", 2: "OBS_VALUE"})

dfi['DATETIME_LOCAL'] = df_source['stopTime'].str.slice(0, 19)
dfi['DATAFLOW'] = "CCEI:DF_HFED_BC(1.0)"
dfi['FREQ'] = "N"
dfi['REF_AREA'] = "CA_BC"
dfi['COUNTERPART_AREA'] = ""
dfi['ENERGY_FLOWS'] = "NSI"
# dfi['TIME_PERIOD'] = ""
dfi['DAYLIGHT_OFFSET'] = ""
dfi['DATETIME_LOCAL_OFFSET'] = ""
# dfi['OBS_VALUE'] = df_source['transValue']
dfi['UNIT_MEASURE'] = "MW"
dfi['UNIT_MULT'] = "0"
dfi['MEASURE_TYPE'] = "ENERGY"
dfi['OBS_STATUS'] = "A"
dfi['CONF_STATUS'] = "F"

file_time = dt.datetime.now().strftime("%Y%m%d%H%M%S")

for i, path in enumerate(df_source['path']):
  date_time = pd.to_datetime(dfi['DATETIME_LOCAL'].iloc[i])
  time_period = date_time.tz_localize(tz='America/Vancouver', ambiguous=True)
  dfi['TIME_PERIOD'].iloc[i] = time_period.astimezone(tz.gettz('UTC')).strftime("%Y-%m-%dT%H:%M:%S")
  if (time_period.dst()):
    dfi['DATETIME_LOCAL_OFFSET'].iloc[i] = "7"
    dfi['DAYLIGHT_OFFSET'].iloc[i] = "1"
  else:
    dfi['DATETIME_LOCAL_OFFSET'].iloc[i] = "8"
    dfi['DAYLIGHT_OFFSET'].iloc[i] = "0"   
  if(path == 'BCH:USA'):
    dfi['COUNTERPART_AREA'].iloc[i] = "US"
  elif (path == 'BCH:TAU'):
    dfi['COUNTERPART_AREA'].iloc[i] = "CA_AB"
  else:
    print("new value"+str(path))

dfi = dfi[['DATAFLOW', 'FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD',  'OBS_VALUE', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS']]
if debug:
  print(dfi)

dfi = dfi.drop(columns=["HOUR"])

# correct column order
dfi = dfi[
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

dfi.to_csv("bcnci.csv")