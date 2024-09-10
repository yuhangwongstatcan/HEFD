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
import time
from selenium import webdriver
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen

hydroObvVal = 0
windObvVal = 0
solar = 0
naturalGas = 0
coal = 0
other = 0

url = 'https://www.saskpower.com/our-power-future/our-electricity/electrical-system/where-your-power-comes-from'
options = webdriver.ChromeOptions()
options.add_argument('headless')
browser = webdriver.Chrome(options)
browser.get(url)
time.sleep(5)
bs = BeautifulSoup(browser.page_source, 'html.parser')
energyBtns = bs.find_all('button', attrs={'class':'power-use-accordion'})
for energy in energyBtns:
    # HYDRO, WIND, SOLAR, NATURAL GAS, COAL, OTHER* | "--"" represent none
    # print(energy.text)
    if "HYDRO" in energy.text:
        print("Find Hydro Value: " + energy.text[energy.text.index('Generation')+10:energy.text.index('MW')-1])
        hydro = energy.text[energy.text.index('Generation')+10:energy.text.index('MW')-1]
    elif "WIND" in energy.text:
        print("Find Wind Value: " + energy.text[energy.text.index('Generation')+10:energy.text.index('MW')-1])
        wind = energy.text[energy.text.index('Generation')+10:energy.text.index('MW')-1]
    elif "SOLAR" in energy.text:
        print("Find Solar Value: " + energy.text[energy.text.index('Generation')+10:energy.text.index('MW')-1])
        solar = energy.text[energy.text.index('Generation')+10:energy.text.index('MW')-1]
    elif "NATURAL GAS" in energy.text:
        print("Find Natural Gas Value: " + energy.text[energy.text.index('Generation')+10:energy.text.index('MW')-1])
        naturalGas = energy.text[energy.text.index('Generation')+10:energy.text.index('MW')-1]
    elif "COAL" in energy.text:
        print("Find Coal Value: " + energy.text[energy.text.index('Generation')+10:energy.text.index('MW')-1])
        coal = energy.text[energy.text.index('Generation')+10:energy.text.index('MW')-1]
    elif "OTHER" in energy.text:
        print("Find Other* Value: " + energy.text[energy.text.index('Generation')+10:energy.text.index('MW')-1])
        other = energy.text[energy.text.index('Generation')+10:energy.text.index('MW')-1]


