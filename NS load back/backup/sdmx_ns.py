'''
CCEI Web Scraping - New Brunswick (New Format)
Data updated every 5 minutes
'''
import pandas as pd
import datetime as dt
from dateutil import tz
# import io
# from urllib.request import urlopen, Request
# from urllib.error import HTTPError, URLError
import numpy as np
from sys import exit
from sdmx_utilities import processing, mail_recipient, SMTP_MAIL_TO, SMTP_MAIL_SUBJECT
import urllib3
from urllib3.util.retry import Retry
from urllib3.util import Timeout
import logging
from bs4 import BeautifulSoup
import re
from dateutil import tz

log = logging.getLogger(__name__)
SMTP_MAIL_SUBJECT = "NS: "+ SMTP_MAIL_SUBJECT
debug = False
url = "https://resourcesprd-nspower.aws.silvertech.net/oasis/current_report.shtml"
http = urllib3.PoolManager()

retries = Retry(total=5,
                backoff_factor=0.2,
                status_forcelist=[500, 502, 503, 504])
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
http = urllib3.PoolManager(retries=retries, cert_reqs='CERT_NONE')

def parse_current_report(html):
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text('\n', strip=True)
    match = re.search(r'Last Updated:\s*(?:\n\s*)?(\d{1,2}-[A-Za-z]{3}-\d{2}\s+\d{2}:\d{2}(?::\d{2})?)', text)
    if not match:
        raise ValueError('Could not find Last Updated timestamp in current report')
    table = soup.find('table')
    if table is None:
        raise ValueError('Could not find current conditions table in current report')
    rows = []
    for tr in table.find_all('tr'):
        cells = [cell.get_text(strip=True) for cell in tr.find_all(['th', 'td'])]
        if len(cells) >= 2:
            rows.append(cells[:2])
    if not rows:
        raise ValueError('Current conditions table did not contain any data rows')
    return pd.DataFrame(rows[:14]), match.group(1)


try:
    response = http.request('GET', url, timeout=Timeout(connect=20.0, read=10.0))
    # df = pd.read_html(response.data, skiprows=2)[0][0:14]
    df, date_time_str = parse_current_report(response.data)
    response.release_conn()
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
#     df = pd.read_html(io.StringIO(response.read().decode("utf-8")), skiprows=2)[0][0:14]
# except HTTPError as error:
#     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, error.reason)
# except URLError as error:
#     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, error.reason)
# except TimeoutError:
#     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, "timed out")

#df = pd.read_html(url, skiprows=2)[0][0:14] # get web table
if debug:
  print(df)

try:
  # date_time_str = df[0][0][14:] # save date and time as a string
  # date_time_local = dt.datetime.strptime(date_time_str, "%d-%b-%y %H:%M:%S").isoformat('T', 'seconds') # convert string to datetime format
  # df = df.drop([0]) # remove rows that are no longer needed
  date_time_format = "%d-%b-%y %H:%M:%S" if date_time_str.count(':') == 2 else "%d-%b-%y %H:%M"
  date_time_local = dt.datetime.strptime(date_time_str, date_time_format).isoformat('T', 'seconds') # convert string to datetime format


  if debug:
    print("date_time_local: ", date_time_local)
  # set the timezone and figure out if it's daylight savings time

  date_time_utc = pd.to_datetime(date_time_local)
  date_time_utc = date_time_utc.tz_localize(tz.gettz('America/Moncton')).astimezone(tz.gettz('UTC'))
  time_period = date_time_utc.strftime("%Y-%m-%dT%H:%M:%S")
  if debug:
    print("date_time_utc: ", date_time_utc)
    print("time_period: ", time_period)

  if (date_time_utc.dst()):
    local_offset = 3
    daylight_offset = 1
  else:
    local_offset = 4
    daylight_offset = 0

  if debug:
    print(df)
  #print(local_offset)
  #print(daylight_offset)

  # create new columns
  df['DATAFLOW'] = "CCEI:DF_HFED_NS(1.0)"
  df['FREQ'] = "N"
  df['REF_AREA'] = "CA_NS"
  df['COUNTERPART_AREA']  = np.where(df[0] == 'Net Load', '_Z',
    np.where(df[0] == 'Highlands Export', 'CA_NS_HL',
      np.where(df[0] == 'East End Export (at Sydney)', 'CA_NS_SD',
        np.where(df[0] == 'East End Export (at East Bay)', 'CA_NS_EB',
          np.where(df[0] == 'Cape Breton Export', 'CA_NS_CB',
            np.where(df[0] == 'Onslow Import', 'CA_NS_OL',
              np.where(df[0] == 'NS Export', 'CA_NS',
                np.where(df[0] == 'Onslow South', 'CA_NS_OLS',
                  np.where(df[0] == 'Flow Into Metro', 'CA_NS_MT',
                    np.where(df[0] == 'Western Import', 'CA_NS_W',
                      np.where(df[0] == 'Valley Import', 'CA_NS_V',
                        np.where(df[0] == 'Maritime Link Import', 'CA_NS_ML',
                          np.where(df[0] == 'Wind Generation', '_Z', df[0]
                                  )))))))))))))  
  df['ENERGY_FLOWS'] = np.where(df[0] == 'Net Load', 'LOAD',
    np.where(df[0] == 'Highlands Export', 'EXPORT',
      np.where(df[0] == 'East End Export (at Sydney)', 'EXPORT',
        np.where(df[0] == 'East End Export (at East Bay)', 'EXPORT',
          np.where(df[0] == 'Cape Breton Export', 'EXPORT',
            np.where(df[0] == 'Onslow Import', 'IMPORT',
              np.where(df[0] == 'NS Export', 'EXPORT',
                np.where(df[0] == 'Onslow South', 'NSI',
                  np.where(df[0] == 'Flow Into Metro', 'NSI',
                    np.where(df[0] == 'Western Import', 'IMPORT',
                      np.where(df[0] == 'Valley Import', 'IMPORT',
                        np.where(df[0] == 'Maritime Link Import', 'IMPORT',
                          np.where(df[0] == 'Wind Generation', 'WIND', df[0]
                                  )))))))))))))    
  df['TIME_PERIOD'] = time_period
  df['OBS_VALUE'] = df[1]
  df['DATETIME_LOCAL'] = date_time_local
  df['DAYLIGHT_OFFSET'] = daylight_offset
  df['DATETIME_LOCAL_OFFSET'] = local_offset
  df['UNIT_MEASURE'] = "MW"
  df['UNIT_MULT'] = "0"
  df['MEASURE_TYPE'] = "ENERGY"
  df['OBS_STATUS'] = "A"
  df['CONF_STATUS'] = "F"


  file_time = date_time_utc.strftime("%Y%m%d%H%M%S")

  df = df.drop(columns=[0, 1]) # delete columns that are no longer needed
  # df_hourly = pd.DataFrame()

  # for i in range(0,df.shape[0],1):
  #   minutes = df.iloc[i]['TIME_PERIOD'][14:16]
  #   if minutes >= "58" or minutes <= "03":
  #     if debug:
  #       print(str(i) +":"+str(minutes))
  #     df_hourly = df_hourly.append(df.iloc[i])
  # if df_hourly.shape[0] != 0:
  #   df_hourly['FREQ'] = df_hourly['FREQ'].replace('N','H')
  # df = pd.concat([df, df_hourly], ignore_index=True)
  # df_hourly = df.loc[(df['TIME_PERIOD'].str[14:16] >= '58') | (df['TIME_PERIOD'].str[14:16] <='03')]
  # df_hourly['FREQ'] = 'H'
  # if (len(df_hourly) > 0):
  #   df = pd.concat([df, df_hourly])
  #df['FREQ']=np.where((df['TIME_PERIOD'].str[14:16] >= '58') | (df['TIME_PERIOD'].str[14:16] <='03'), 'H', df['FREQ'])
  df = df.drop_duplicates(subset=['FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD'], keep='first')
except Exception as e:
  log.exception(e)
  mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, str(e))
  exit()
if debug:
  print(df)



# save(df,"/ns/backup/data_ns_"+str(file_time)+".csv")
# save(df,"/ns/data_ns.csv")

# loading("/ns/data_ns.csv")

if __name__ == '__main__':
  processing(df, "ns", f'{dt.datetime.now():%Y}')