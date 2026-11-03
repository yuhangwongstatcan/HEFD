'''
CCEI Web Scraping - Quebec
Data updated every 15 minutes
'''
import pandas as pd
from dateutil import tz
import datetime as dt
import os
import json
import logging
from sdmx_utilities import processing
from sys import exit
import ssl
import requests
from sdmx_utilities import processing, mail_recipient, SMTP_MAIL_TO, SMTP_MAIL_SUBJECT
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
import urllib3
from urllib3.util.retry import Retry
from urllib3.util import Timeout
SMTP_MAIL_SUBJECT = "QC: "+ SMTP_MAIL_SUBJECT
import logging
log = logging.getLogger(__name__)
debug = False
url = "https://www.hydroquebec.com/data/documents-donnees/donnees-ouvertes/json/demande.json"
http = urllib3.PoolManager()

class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            ssl_context=ctx,
        )

try:
    session = requests.Session()
    session.mount("https://", TLSAdapter())
    response = session.get(url, timeout=15)
    response.raise_for_status()
    data = json.loads(response.text)

except urllib3.exceptions.MaxRetryError as e:
    log.exception(e)
    mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT,f"Max retries exceeded with url: {e.reason}")
    exit()
except urllib3.exceptions.TimeoutError as e:
    log.exception(e)
    mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, f"Request timed out: {e}")
    exit()
# req=Request(url, headers={'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/151.0'})

# try:
#   with urlopen(req, timeout=30) as response:
#     data = json.loads(response.read())
# except HTTPError as error:
#     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, error.reason)
# except URLError as error:
#     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, error.reason)
# except TimeoutError:
#     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, "timed out")


#print(data)
try:
  dfhead = pd.json_normalize(data)

  #display(dfhead)

  df = pd.json_normalize(data['details'])
  df = df.rename(columns={"date":"DATETIME_LOCAL","valeurs.demandeTotal":"OBS_VALUE"})
  df = df.sort_values('DATETIME_LOCAL', ascending=False)
  df = df.dropna()

  if debug:
    print(df)


  date_time_local = dfhead['recentHour'][0]
  if debug:
    print("date_time_local: ", date_time_local)

  # set the timezone to utc and figure out if it's daylight savings time
  date_time_local = pd.to_datetime(date_time_local).tz_localize(tz.gettz('America/Montreal'))

  date_time_utc = date_time_local.astimezone(tz.gettz('UTC'))

  file_time = date_time_utc.strftime("%Y%m%d%H%M%S")
  # spark.conf.set('file_time', str(file_time))
  if debug:
    print(file_time)

  time_period = date_time_utc.strftime("%Y-%m-%dT%H:%M:%S")
  result = []
  if (date_time_local.dst()):
    local_offset = 4
    daylight_offset = 1
    is_dst =True
  else:
    local_offset = 5
    daylight_offset = 0
    is_dst = False

  for i in range(len(df)):
    row = df['DATETIME_LOCAL'].iloc[i]
    result.append(
    pd.to_datetime(row).tz_localize('America/Montreal', ambiguous=is_dst).astimezone(tz.gettz('UTC')).strftime("%Y-%m-%dT%H:%M:%S")
      )
  if debug:    
    print("date_time_utc: ", date_time_utc)
    print("time_period: ", time_period)

  # if (date_time_local.dst()):
  #   local_offset = 4
  #   daylight_offset = 1
  # else:
  #   local_offset = 5
  #   daylight_offset = 0



  # create new columns
  df['DATAFLOW'] = "CCEI:DF_HFED_QC(1.0)"
  df['FREQ'] = "N"
  df['REF_AREA'] = "CA_QC"
  df['COUNTERPART_AREA'] = "_Z"
  df['ENERGY_FLOWS'] = 'DEMAND'
  df['TIME_PERIOD'] = result
  df['TIME_PERIOD']=pd.to_datetime(df['TIME_PERIOD']).dt.strftime("%Y-%m-%dT%H:%M:%S")
  df['DATETIME_LOCAL']=pd.to_datetime(df['DATETIME_LOCAL']).dt.strftime("%Y-%m-%dT%H:%M:%S")
  df['DAYLIGHT_OFFSET'] = daylight_offset
  df['DATETIME_LOCAL_OFFSET'] = local_offset
  df['UNIT_MEASURE'] = "MW"
  df['UNIT_MULT'] = "0"
  df['MEASURE_TYPE'] = "ENERGY"
  df['OBS_STATUS'] = "A"
  df['CONF_STATUS'] = "F"

  # correct column order

  df = df[['DATAFLOW', 'FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD', 'OBS_VALUE', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS']]
  df_hourly = pd.DataFrame()

  for i in range(0,df.shape[0],1):
    minutes = df.iloc[i]['TIME_PERIOD'][14:16]
    if minutes == "59" or minutes == "03":
      if debug:
        print(str(i) +":"+str(minutes))
      df_hourly = df_hourly.append(df.iloc[i])
  if df_hourly.shape[0] != 0:
    df_hourly['FREQ'] = df_hourly['FREQ'].replace('N','H')

  df = pd.concat([df, df_hourly], ignore_index= True)
  if debug:
    print(df)



  #production
  
  url = "https://www.hydroquebec.com/data/documents-donnees/donnees-ouvertes/json/production.json"
  try:
      session = requests.Session()
      session.mount("https://", TLSAdapter())
      response = session.get(url, timeout=15)
      response.raise_for_status()

      data = json.loads(response.text)
  except urllib3.exceptions.MaxRetryError as e:
    mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT,f"Max retries exceeded with url: {e.reason}")
    exit()
  except urllib3.exceptions.TimeoutError as e:
    mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, f"Request timed out: {e}")
    exit()
  # req=Request(url)

  # try:
  #   with urlopen(req, timeout=10) as response:
  #     data = json.loads(response.read())
  # except HTTPError as error:
  #     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, error.reason)
  # except URLError as error:
  #     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, error.reason)
  # except TimeoutError:
  #     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, "timed out")


  #print(data)

  dfhead2 = pd.json_normalize(data)

  #display(dfhead)

  df2 = pd.json_normalize(data['details'])
  df2 = df2.rename(columns={"date":"DATETIME_LOCAL","valeurs.total":"TOTAL_PRODUCTION","valeurs.hydraulique":"HYDRO","valeurs.eolien":"WIND","valeurs.autres":"OTHER","valeurs.solaire":"SOLAR","valeurs.thermique":"THERMAL"})
  #valeurs.total, valeurs.hydraulique, valeurs.eolien, valeurs.autres, valeurs.solaire, valeurs.thermique
  df2 = df2.sort_values('DATETIME_LOCAL', ascending=False)
  df2 = df2.dropna()



  date_time_local = dfhead2['recentHour'][0]
  if debug:
    print("date_time_local: ", date_time_local)

  # set the timezone to utc and figure out if it's daylight savings time
  date_time_utc = pd.to_datetime(date_time_local)
  date_time_utc = date_time_utc.tz_localize(tz.gettz('America/Montreal'), ambiguous=is_dst).astimezone(tz.gettz('UTC'))

  time_period = date_time_utc.strftime("%Y-%m-%dT%H:%M:%S")
  result = []
  for i in range(len(df2)):
    row = df2['DATETIME_LOCAL'].iloc[i]
    result.append(
    pd.to_datetime(row).tz_localize(tz.gettz('America/Montreal'), ambiguous=is_dst).astimezone(tz.gettz('UTC')).strftime("%Y-%m-%dT%H:%M:%S")
      )

  if debug:    
    print("date_time_utc: ", date_time_utc)
    print("time_period: ", time_period)

  if (date_time_utc.dst()):
    local_offset = 4
    daylight_offset = 1
  else:
    local_offset = 5
    daylight_offset = 0


  # create new columns
  df2['DATAFLOW'] = "CCEI:DF_HFED_QC(1.0)"
  df2['FREQ'] = "H"
  df2['REF_AREA'] = "CA_QC"
  df2['COUNTERPART_AREA'] = "_Z"
  df2['TIME_PERIOD'] = result
  df2['DAYLIGHT_OFFSET'] = daylight_offset
  df2['DATETIME_LOCAL_OFFSET'] = local_offset
  df2['UNIT_MEASURE'] = "MW"
  df2['UNIT_MULT'] = "0"
  df2['MEASURE_TYPE'] = "ENERGY"
  df2['OBS_STATUS'] = "A"
  df2['CONF_STATUS'] = "F"

  # correct column order

  #display(df)
  # correct column order 'TOTAL_PRODUCTION', 'HYDRO', 'WIND', 'OTHER', 'SOLAR', 'THERMAL'
  df2 = df2[['DATAFLOW', 'FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'TIME_PERIOD', 'TOTAL_PRODUCTION', 'HYDRO', 'WIND', 'OTHER', 'SOLAR', 'THERMAL', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS']]
  #display(df2)

  # Names of ‘variable’ and ‘value’ columns can be customized
  pdf = pd.melt(df2, id_vars =['DATAFLOW', 'FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'TIME_PERIOD', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS'], value_vars =['TOTAL_PRODUCTION', 'HYDRO', 'WIND', 'OTHER', 'SOLAR', 'THERMAL'],
        var_name ='ENERGY_FLOWS', value_name ='OBS_VALUE')

  pdf = pdf[['DATAFLOW', 'FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD',  'OBS_VALUE', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS']]
  #pdf.append(df)
  pdf2 = pd.concat([df, pdf], ignore_index= True)
  pdf2.reset_index()

  #pdf2 = pdf2.drop_duplicates(subset=['FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD'], keep='first')
except Exception as e:
  log.exception(e)
  mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, str(e))
  exit()
if debug:
  print(pdf2)


# save(df,"/qc/backup/data_qc_"+str(file_time)+".csv")
# save(df,"/qc/data_qc.csv")

# loading("/qc/data_qc.csv")
# pdf2.to_csv("outputqc.csv", index=False)

if __name__ == '__main__':
  processing(pdf2, "qc", f'{dt.datetime.now():%Y-%m}')