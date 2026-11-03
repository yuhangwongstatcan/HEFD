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
SMTP_MAIL_SUBJECT = "NS Demand: "+ SMTP_MAIL_SUBJECT

debug = False
url = "https://resourcesprd-nspower.aws.silvertech.net/oasis/current_report.shtml"
df_peak_demand = pd.DataFrame()

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
    response = http.request('GET', url, timeout=Timeout(connect=20.0, read=10.0),headers={'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/151.0'})
    # df_peak_demand = pd.read_html(response.data, skiprows=1)[0][19:20]
    df_peak_demand, date_time_str = parse_current_report(response.data)
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
#     df_peak_demand = pd.read_html(io.StringIO(response.read().decode("utf-8")), skiprows=1)[0][19:20]
# except HTTPError as error:
#     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, error.reason)
# except URLError as error:
#     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, error.reason)
# except TimeoutError:
#     mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, "timed out")

# 5 day peak demand
# df_peak_demand = pd.DataFrame()
# df_peak_demand = pd.read_html(url, skiprows=1)[0][19:20] # get web table

if debug:
  print(df_peak_demand)
try:
  # date_time_str_peak =str(df_peak_demand[0].iat[0])

  # date_time_local_peak = dt.datetime.strptime(date_time_str_peak, "%d-%b-%y %H:%M").isoformat('T', 'seconds')

  date_time_format = "%d-%b-%y %H:%M:%S" if date_time_str.count(':') == 2 else "%d-%b-%y %H:%M"
  date_time_local_peak = dt.datetime.strptime(date_time_str, date_time_format).isoformat('T', 'seconds') # convert string to datetime format

  date_time_utc = pd.to_datetime(date_time_local_peak)
  date_time_utc = date_time_utc.tz_localize(tz.gettz('America/Moncton')).astimezone(tz.gettz('UTC'))
  time_period = date_time_utc.strftime("%Y-%m-%dT%H:%M:%S")

  file_time = date_time_utc.strftime("%Y%m%d%H%M%S")

  date_time_local_peak_old = 0

  if (date_time_local_peak != date_time_local_peak_old):
      date_time_utc_peak = pd.to_datetime(date_time_local_peak)
      date_time_utc_peak = date_time_utc_peak.tz_localize(tz.gettz('America/Moncton')).astimezone(tz.gettz('UTC'))
      time_period_peak = date_time_utc_peak.strftime("%Y-%m-%dT%H:%M:%S")
      if debug:
        print("date_local: ", date_time_local_peak)
        print("time_period: ", time_period_peak)

  else: 
      print("already exists")
      
  if (date_time_utc_peak.dst()):
    local_offset = 3
    daylight_offset = 1
  else:
    local_offset = 4
    daylight_offset = 0
  #print(local_offset)
  #print(daylight_offset)

  if (date_time_local_peak != date_time_local_peak_old):

    df_peak = pd.DataFrame()
    df_peak['OBS_VALUE'] = df_peak_demand[1]
    df_peak['DATETIME_LOCAL'] = date_time_local_peak
    df_peak['DAYLIGHT_OFFSET'] = daylight_offset
    df_peak['DATETIME_LOCAL_OFFSET'] = local_offset
    df_peak['TIME_PERIOD'] = date_time_utc_peak.strftime("%Y-%m-%dT%H:%M:%S")
    df_peak['DATAFLOW'] = "CCEI:DF_HFED_NS(1.0)"
    df_peak['FREQ'] = "N"
    df_peak['REF_AREA'] = "CA_NS"
    df_peak['COUNTERPART_AREA']  = "_Z"  
    df_peak['ENERGY_FLOWS'] = "DEMAND_PEAK"
    df_peak['UNIT_MEASURE'] = "MW"
    df_peak['UNIT_MULT'] = "0"
    df_peak['MEASURE_TYPE'] = "ENERGY"
    df_peak['OBS_STATUS'] = "A"
    df_peak['CONF_STATUS'] = "F"

    df_peak = df_peak[['DATAFLOW', 'FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD',  'OBS_VALUE', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS']]
    df_peak = df_peak.drop_duplicates(subset=['FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD'], keep='first')
    if debug:
      print(df_peak)
  else: 
    print("already exists")
except Exception as e:
  log.exception(e)
  mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, str(e))
  exit()

# save(df_peak,"/ns/backup/data_ns_peak_"+str(file_time)+".csv")
# save(df_peak,"/ns/data_ns_peak.csv")

# loading("/ns/data_ns_peak.csv")
if __name__ == '__main__':
  processing(df_peak, "ns_peak", f'{dt.datetime.now():%Y-%m}')