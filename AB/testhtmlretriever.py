import pandas as pd
import urllib.request
import datetime as dt
from dateutil import tz
import pytz
import os
import requests
import json
import pytz
from datetime import datetime
from bs4 import BeautifulSoup


debug = False
url_pool_price = "http://ets.aeso.ca/ets_web/ip/Market/Reports/ActualForecastWMRQHReportServlet"
url_system_marginal_price = "http://ets.aeso.ca/ets_web/ip/Market/Reports/CSMPriceReportServlet"
pool_price = pd.DataFrame()
sys_mar_price = pd.DataFrame()

####################### Create POOL PRICE Dataframe ###########################
df_poolprice = pd.DataFrame()
df_poolprice['ENERGY_FLOWS'] = "POOL_PRICE"
df_poolprice['OBS_VALUE'] = ""
df_poolprice['DATETIME_LOCAL'] = "" 
df_poolprice['TIME_PERIOD'] = ""
df_poolprice['DAYLIGHT_OFFSET'] = ""
df_poolprice['DATETIME_LOCAL_OFFSET'] = ""
df_poolprice['DATAFLOW'] = 'CCEI:DF_HFED_AB(1.0)'
df_poolprice['FREQ']='N'
df_poolprice['REF_AREA'] = "CA_AB"
df_poolprice['COUNTERPART_AREA'] = '_Z'
df_poolprice['UNIT_MEASURE'] = "MW"
df_poolprice['UNIT_MULT'] = "0"
df_poolprice['MEASURE_TYPE'] = "ENERGY"
df_poolprice['OBS_STATUS'] = "A"
df_poolprice['CONF_STATUS'] = "F"
if debug:
  print(df_poolprice)
df_poolprice = df_poolprice[['DATAFLOW', 'FREQ', 'REF_AREA', 'COUNTERPART_AREA', 'ENERGY_FLOWS', 'TIME_PERIOD', 'OBS_VALUE', 'DATETIME_LOCAL', 'DAYLIGHT_OFFSET', 'DATETIME_LOCAL_OFFSET', 'UNIT_MEASURE', 'UNIT_MULT', 'MEASURE_TYPE', 'OBS_STATUS', 'CONF_STATUS']]
print(df_poolprice)
####################### ACTUAL POSTED POOL PRICE CREATING PD ###########################

####################### ACTUAL POSTED POOL PRICE ###########################
getpagepoolprice = requests.get(url_pool_price)
getpagepoolprice_soup= BeautifulSoup(getpagepoolprice.text, 'html.parser')
pool_price_table = getpagepoolprice_soup.findAll("table")
index = 0
dfplp = pd.DataFrame()
dfmp = pd.DataFrame()

for tr in pool_price_table[2]('tr'):
        
        if index == 0:
                # print(tr)
                th = tr.findAll(lambda tag: tag.name=='th')
                # print(th[0].string)
                # print(th[2].string)
        if index > 0:
                td = tr.findAll(lambda tag: tag.name=='td')
                if td[2].string != "-":
                        
                        dateObj = dt.datetime.strptime(td[0].string, "%m/%d/%Y %H")
                        localDateTime = dateObj
                        # change to UTC time
                        local = pytz.timezone("America/Edmonton")
                        local_dt = local.localize(dateObj, is_dst=None)
                        utcDateTime = local_dt.astimezone(pytz.utc)
                        newplpdict = {'DATAFLOW': "CCEI:DF_HFED_AB(1.0)",
                                'ENERGY_FLOWS': "POOL_PRICE",
                                'OBS_VALUE': td[2].string, 
                                'DATETIME_LOCAL': localDateTime.strftime("%Y-%m-%dT%H:%M:%S"),
                                'TIME_PERIOD': utcDateTime.strftime("%Y-%m-%dT%H:%M:%S"),
                                'DAYLIGHT_OFFSET': "-6",
                                'DATETIME_LOCAL_OFFSET': "0",
                                'FREQ': "N",
                                'REF_AREA': "CA_AB",
                                'COUNTERPART_AREA': "_Z",
                                'UNIT_MEASURE': "CAD",
                                'UNIT_MULT': "0",
                                'MEASURE_TYPE': "ENERGY",
                                'OBS_STATUS': "A",
                                'CONF_STATUS': "F"}
                        dfplp = dfplp._append(newplpdict, ignore_index=True)      

        index+=1
# print(dfplp)        
####################### ACTUAL POSTED POOL PRICE END ###########################

####################### SYSTEM MARGINAL PRICE ###########################
getpagemarginalprice = requests.get(url_system_marginal_price)
getpagemarginalprice_soup= BeautifulSoup(getpagemarginalprice.text, 'html.parser')

pool_price_table = getpagemarginalprice_soup.findAll("table")
index = 0
df = pd.DataFrame()

for tr in pool_price_table[2]('tr'):        
        if index == 0:
                # print(tr)
                th = tr.findAll(lambda tag: tag.name=='th')
        if index > 0:
                td = tr.findAll(lambda tag: tag.name=='td')
                # print(td[0].string + td[1].string)
                # print(td[3].string)
                dateObj = dt.datetime.strptime(td[0].string[0:10]+" "+td[1].string, "%m/%d/%Y %H:%M")
                localDateTime = dateObj
                # change to UTC time
                local = pytz.timezone("America/Edmonton")
                local_dt = local.localize(dateObj, is_dst=None)
                utcDateTime = local_dt.astimezone(pytz.utc)
                newplpdict = {'DATAFLOW': "CCEI:DF_HFED_AB(1.0)",
                                'ENERGY_FLOWS': "SYSTEM_MARGINAL_PRICE",
                                'OBS_VALUE': td[3].string, 
                                'DATETIME_LOCAL': localDateTime.strftime("%Y-%m-%dT%H:%M:%S"),
                                'TIME_PERIOD': utcDateTime.strftime("%Y-%m-%dT%H:%M:%S"),
                                'DAYLIGHT_OFFSET': "-6",
                                'DATETIME_LOCAL_OFFSET': "0",
                                'FREQ': "N",
                                'REF_AREA': "CA_AB",
                                'COUNTERPART_AREA': "_Z",
                                'UNIT_MEASURE': "CAD",
                                'UNIT_MULT': "0",
                                'MEASURE_TYPE': "ENERGY",
                                'OBS_STATUS': "A",
                                'CONF_STATUS': "F"}
                dfmp = dfmp._append(newplpdict, ignore_index=True)        
                
        index+=1
# print(dfmp)          
####################### SYSTEM MARGINAL PRICE END ###########################

## rearrange columes
dfplp[['DATAFLOW','FREQ','REF_AREA','COUNTERPART_AREA', 'ENERGY_FLOWS','TIME_PERIOD','OBS_VALUE','DATETIME_LOCAL','DAYLIGHT_OFFSET','DATETIME_LOCAL_OFFSET','UNIT_MEASURE','UNIT_MULT','MEASURE_TYPE','OBS_STATUS','CONF_STATUS']]
dfmp[['DATAFLOW','FREQ','REF_AREA','COUNTERPART_AREA', 'ENERGY_FLOWS','TIME_PERIOD','OBS_VALUE','DATETIME_LOCAL','DAYLIGHT_OFFSET','DATETIME_LOCAL_OFFSET','UNIT_MEASURE','UNIT_MULT','MEASURE_TYPE','OBS_STATUS','CONF_STATUS']]

df_final = pd.DataFrame()

dfplp = dfplp.astype(str)
dfmp = dfmp.astype(str)

df_final = df_final._append(dfplp)
df_final = df_final._append(dfmp)

print(df_final)
        
df_final.to_csv("./output/" + "abtest.csv", index=False)