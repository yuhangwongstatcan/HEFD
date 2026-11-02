'''
CCEI Web Scraping - Ontario (New Format)
Data updated every 5 minutes
'''
import pandas as pd
import datetime as dt
from dateutil import tz
import io
# from urllib.request import urlopen, Request
# from urllib.error import HTTPError, URLError
from sys import exit
from sdmx_utilities import processing, mail_recipient, SMTP_MAIL_TO, SMTP_MAIL_SUBJECT
import urllib3
from urllib3.util.retry import Retry
from urllib3.util import Timeout
import logging

log = logging.getLogger(__name__)
SMTP_MAIL_SUBJECT = "ON: "+ SMTP_MAIL_SUBJECT
debug = False
url = "http://reports.ieso.ca/public/Demand/PUB_Demand.csv"
http = urllib3.PoolManager()

retries = Retry(total=5,
                backoff_factor=0.2,
                status_forcelist=[500, 502, 503, 504])

http = urllib3.PoolManager(retries=retries)

try:
    response = http.request('GET', url, timeout=Timeout(connect=20.0, read=10.0))
    df = pd.read_csv(io.StringIO(response.data.decode("utf-8")), skiprows=3)
    response.release_conn()
except urllib3.exceptions.MaxRetryError as e:
    log.exception(e)
    mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT,f"Max retries exceeded with url: {e.reason}")
    exit()
except urllib3.exceptions.TimeoutError as e:
    log.exception(e)
    mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, f"Request timed out: {e}")
    exit()
# req=Request(url,headers={'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/151.0'})

# try:
#   with urlopen(req, timeout=30) as response:
#     df = pd.read_csv(io.StringIO(response.read().decode("utf-8")), skiprows=3)
# except HTTPError as error:
#     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, error.reason)
# except URLError as error:
#     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, error.reason)
# except TimeoutError:
#     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, "timed out")

#df = pd.read_csv(url, skiprows=3) # read in csv file to dataframe
# rename columns
if debug:
  print(len(df))
try:
  df = df.rename(columns={"Date": "DATETIME_LOCAL","Ontario Demand":"ONTARIO_DEMAND","Market Demand":"MARKET_DEMAND"})

  df = pd.melt(df, id_vars=['DATETIME_LOCAL','Hour'], value_vars=['ONTARIO_DEMAND', 'MARKET_DEMAND'], var_name="ENERGY_FLOWS", value_name="OBS_VALUE") 
  df = df.sort_values(by=['DATETIME_LOCAL']) # sort ascencing by date

  if debug:
    print(df)
  result = []
  rows_drop = []
  local_offset = []
  daylight_offset = []
  df.is_copy = False
  for i in range(len(df)):
    
    if(df['Hour'].iloc[i]== 24):
      df['DATETIME_LOCAL'].iloc[i] = str(df['DATETIME_LOCAL'].iloc[i]) +"T0:00:00"
    else:
      df['DATETIME_LOCAL'].iloc[i] = str(df['DATETIME_LOCAL'].iloc[i]) +"T"+str(df['Hour'].iloc[i])+":00:00"
    
    datetime_local = df['DATETIME_LOCAL'].iloc[i]
    
    time_utc = pd.to_datetime(datetime_local).tz_localize(tz.gettz('America/Toronto'),nonexistent='shift_backward',ambiguous='NaT')

    if(not pd.isnull(time_utc)):

      result.append(time_utc.astimezone(tz.gettz('UTC')))
      if (time_utc.dst()):
        local_offset = 4
        daylight_offset = 1.0
      else:
        local_offset = 5
        daylight_offset = 0.0
    else:
      rows_drop.append(i)
      if debug:
        print ("null")
  df.drop(df.index[rows_drop], inplace=True)
  if debug:
    print(len(df))

  file_time = dt.datetime.now().strftime("%Y%m%d%H%M%S")

    #result.append(
    #pd.to_datetime(row).tz_localize(tz.gettz('America/Toronto'),nonexistent='shift_backward',ambiguous='NaT').astimezone(tz.gettz('UTC')).strftime("%Y-%m-%dT%H:%M:%S")
    #  )
      
  # turn columns into row values

  if debug:
    print(df)

  # create new columns
  df['DATAFLOW'] = "CCEI:DF_HFED_ON(1.0)"
  df['FREQ']='N'
  df['REF_AREA'] = 'CA_ON'
  df['COUNTERPART_AREA'] = '_Z'
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

  #[['DATAFLOW', 'FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD',  'OBS_VALUE', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS']]

  # correct column order
  df = df[['DATAFLOW', 'FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD',  'OBS_VALUE', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS']]
  df = df.sort_values(by = 'TIME_PERIOD', ascending = False)

  # df_hourly = list()
  if debug:
    print(str(len(df.index)))
  # for i in range(0,df.shape[0],1):
  #   minutes = df.iloc[i]['TIME_PERIOD'][14:16]
  #   if minutes >= "59" or minutes <= "02":
  #     if debug:
  #       print(str(i) +":"+str(minutes))
  #     #df.at[i,'FREQ']='H'
  #     df_hourly.append(df.iloc[i])

  # if debug:
  #   print(df_hourly)
  # df_hourly = pd.DataFrame(df_hourly)
  # if not df_hourly.empty:
  #   df_hourly['FREQ'] = df_hourly['FREQ'].replace('N','H')
  #   #df = df.append(df_hourly)
  #   df = pd.concat([df,df_hourly])
  # else:
  #   df['FREQ'] = df['FREQ'].replace('N','H')
  # df_hourly = df.loc[(df['TIME_PERIOD'].str[14:16] >= '58') | (df['TIME_PERIOD'].str[14:16] <='03')]
  # df_hourly['FREQ'] = 'H'
  # if (len(df_hourly) > 0):
  #   df = pd.concat([df, df_hourly])
  #df['FREQ']=np.where((df['TIME_PERIOD'].str[14:16] >= '59') | (df['TIME_PERIOD'].str[14:16] <='02'), 'H', df['FREQ'])
except Exception as e:
  log.exception(e)
  mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, str(e))
  exit()

if debug:
  print(str(len(df.index)))


df = df.drop_duplicates(subset=['FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD'], keep='first')

# save(df,"/on/backup/data_on_"+str(file_time)+".csv")
# save(df, "/on/data_on.csv")

if debug:
  print(str(len(df)))
# loading("/on/data_on.csv")

if __name__ == '__main__':
  processing(df, "on", f'{dt.datetime.now():%Y}')