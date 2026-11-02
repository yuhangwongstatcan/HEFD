"""
CCEI Web Scraping - ONTARIO ADDITION DATA (New Format)
"""
import pandas as pd
import urllib.request
import datetime as dt
from dateutil import tz
import pytz
import os
import math
import requests
from urllib.parse import urlparse
import ssl
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pathlib import Path
import re
import csv
from io import StringIO
import xml.etree.ElementTree as ET 
from sdmx_codelist_mapper import *
from zipfile import ZipFile
from io import BytesIO
from sdmx_utilities import processing, mail_recipient, SMTP_MAIL_TO, SMTP_MAIL_SUBJECT
import io
import urllib3
import logging
logging.basicConfig(level="INFO")
log = logging.getLogger(__name__)

# Outter bound -5 BEFORE OR AFTER -4
dst = {'2023':['2023-03-12','2023-11-05'],
       '2024':['2024-03-10','2024-11-03'],
       '2025':['2025-03-09','2025-11-02'],
       '2026':['2026-03-08','2026-11-01']}

# Hourly Consumption By Forward Sortation Area object.
class HourlyConptFSAReport:
  def __init__(self, fsa, date, hour, customer_type, price_plan, total_consumption, premise_count):
    self.fsa = fsa
    self.hour = hour
    self.customer_type = customer_type
    self.price_plan = price_plan
    self.total_consumption = total_consumption
    self.premise_count = premise_count
    self.date = date

def getUTCDate(date, hour):
    offset = 6
    # get year, month, and day.
    year = date.today().year
    month = date.today().month
    day = date.today().day

    # if outter bound
    if(date <= datetime.strptime(dst[str(year)][0], "%Y-%m-%d") or date >= datetime.strptime(dst[str(year)][0], "%Y-%m-%d")):
        offset = 5
    else: 
        offset = 4
    # UTC time:
    utcTime = date.replace(hour=int(hour)-1, minute=00) + timedelta(hours=offset)
    # log("LOCAL TIME: " + date_time_local)
    return utcTime

def getUTCDateFixOffset(date, hour, offset):
    # get year, month, and day.
    year = date.year
    month = date.month
    day = date.day
   
    # UTC time:
    utcTime = date.replace(hour=int(hour)-1, minute=00) + timedelta(hours=offset)
    # log("LOCAL TIME: " + date_time_local)
    return utcTime

def getONDate(date, hour):
    offset = 6
    # get year, month, and day.
    year = date.today().year
    month = date.today().month
    day = date.today().day

    # if outter bound
    if(date <= datetime.strptime(dst[str(year)][0], "%Y-%m-%d") or date >= datetime.strptime(dst[str(year)][0], "%Y-%m-%d")):
        offset = 5
    else: 
        offset = 4
    # Ontario time:
    ONTime = date.replace(hour=int(hour)-1, minute=00)
    return ONTime

##################
# This method return the records with data conversion and UTC time in CSV format.
##################
def getValue(obsval, date, energyflow, hour):  
    # return CSV record string.
    # convert time.
    ONTime = getONDate(date, hour).strftime("%Y-%m-%dT%H:%M:%S")
    UTCTime = getUTCDate(date, hour).strftime("%Y-%m-%dT%H:%M:%S")
    return "DATAFLOW,CCEI:DF_HFED_ON(1.0),I,N,CA_ON,_Z," + energyflow + "," + ONTime + "," + obsval.rstrip() + "," + UTCTime + ",0.0,4.0,MW,0,ENERGY,A,F" + "\n"

################################################### Load Spec ##################################################### 
def parseXML(xmlfile): 
  
    # create element tree object 
    xmlfile = xmlfile.replace("'", "")
    tree = ET.fromstring(xmlfile) 
    
    # create CSV .
    csvString = "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,COUNTERPART_AREA,ENERGY_FLOWS,DATETIME_LOCAL,OBS_VALUE,TIME_PERIOD,DAYLIGHT_OFFSET,DATETIME_LOCAL_OFFSET,UNIT_MEASURE,UNIT_MULT,MEASURE_TYPE,OBS_STATUS,CONF_STATUS" + "\n"
    date = ''
    hour = ''
    ironsteel = ''
    metalora =''
    motorvehicle = ''
    petrocoal = ''
    pulppaper = ''
    otherindstr = ''
    manufactr = ''
    elecpower = ''
    ldc = ''
    directconnect = ''
    energyFlow = ''
    # get root element 
    for child in tree:
        
        log.debug(child.tag, child.attrib)
        dataDate = ""
        if child.tag in "DocHeader" : # header information.
            for headerChild in child:
                log.debug(headerChild.tag, headerChild.attrib, headerChild.text)
                if headerChild.tag in "CreatedAt" :
                    log.debug("Create date: " + headerChild.text)
                    dataDate = headerChild.text
                    date = dt.datetime.strptime(dataDate, "%Y-%m-%dT%H:%M:%S")
        if child.tag in "DocBody": # body information.
            for bodyChild in child:
                log.debug(bodyChild.tag)
                log.debug(bodyChild.attrib)
                log.debug(bodyChild.text)
                if bodyChild.tag in "AQEW": # body information.
                    for aqewChild in bodyChild:
                        changedDate = ''
                        log.debug(aqewChild.tag, aqewChild.attrib, aqewChild.text)
                        if aqewChild.tag in "DeliveryDate":
                                ddate = dt.datetime.strptime(aqewChild.text, "%Y-%m-%d")
                        if aqewChild.tag in "AQEW_Hour":
                            for aqewHourChild in aqewChild: # the hour of the day information.
                                log.debug(aqewHourChild.tag, aqewHourChild.attrib, aqewHourChild.text)
                                if aqewHourChild.tag in "Hour" :
                                    hour = aqewHourChild.text
                                    changedDate = getONDate(ddate, hour)
                                if aqewHourChild.tag in "QtyIronSteel" :
                                    energyFlow = aqewHourChild.text
                                    csvString = csvString + getValue(energyFlow, changedDate, "IRON_STEEL", hour) 
                                if aqewHourChild.tag in "QtyMetalOre" :
                                    energyFlow = aqewHourChild.text
                                    csvString = csvString + getValue(energyFlow, changedDate, "METAL_ORE", hour)
                                if aqewHourChild.tag in "QtyMotorVehicle" :
                                    energyFlow = aqewHourChild.text
                                    csvString = csvString + getValue(energyFlow, changedDate, "MOTOR_VEHICLE", hour)
                                if aqewHourChild.tag in "QtyPetroCoal" :
                                    energyFlow = aqewHourChild.text
                                    csvString = csvString + getValue(energyFlow, changedDate, "PETRO_COAL", hour)
                                if aqewHourChild.tag in "QtyPulpPaper" :
                                    energyFlow = aqewHourChild.text
                                    csvString = csvString + getValue(energyFlow, changedDate, "PULP_PAPER", hour)
                                if aqewHourChild.tag in "QtyOtherIndstr" :
                                    energyFlow = aqewHourChild.text
                                    csvString = csvString + getValue(energyFlow, changedDate, "OTHER_INDSTR", hour)
                                if aqewHourChild.tag in "QtyManufactr" :
                                    energyFlow = aqewHourChild.text
                                    csvString = csvString + getValue(energyFlow, changedDate, "MANU_FACTR", hour)
                                if aqewHourChild.tag in "QtyElecPower" :
                                    energyFlow = aqewHourChild.text
                                    csvString = csvString + getValue(energyFlow, changedDate, "ELEC_POWER", hour)
                                if aqewHourChild.tag in "QtyLdc" :
                                    energyFlow = aqewHourChild.text
                                    csvString = csvString + getValue(energyFlow, changedDate, "LDC", hour)
                                if aqewHourChild.tag in "QtyDirectConnect" :
                                    energyFlow = aqewHourChild.text
                                    csvString = csvString + getValue(energyFlow, changedDate, "DIRECT_CONNECT", hour)
    return csvString 


################################################### Hourly Consumption FSA Report #####################################################
def getHourlyConsumptionFSACsvValue(obj):  
    # return CSV record string.
    #(self, fsa, date, hour, customer_type, price_plan, total_consumption, premise_count)
    # convert time.
    obsval = obj.total_consumption
    date = dt.datetime.strptime(obj.date, "%Y-%m-%d")
    customer_type = obj.customer_type.replace(' <50', '_50')
    energyflow = customer_type + '_' + obj.price_plan
    hour = obj.hour
    counterpartArea = obj.fsa
    ONTime = getONDate(date, hour).strftime("%Y-%m-%dT%H:%M:%S")
    UTCTime = getUTCDate(date, hour).strftime("%Y-%m-%dT%H:%M:%S")
    
    try:
        return "DATAFLOW,CCEI:DF_HFED_ON(1.0),I,N,CA_ON,"+ "CA_"+counterpartArea +"," + energyflow.upper() + "," + ONTime + "," + str(obsval) + "," + UTCTime + ",0.0,4.0,MW,0,ENERGY,A,F" + "\n"
    except:
        log.debug(counterpartArea + " Error")

def getHourlyConsumptionFSACsvFromList(hourlyConsumptionFSAList):
    csvString = "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,COUNTERPART_AREA,ENERGY_FLOWS,DATETIME_LOCAL,OBS_VALUE,TIME_PERIOD,DAYLIGHT_OFFSET,DATETIME_LOCAL_OFFSET,UNIT_MEASURE,UNIT_MULT,MEASURE_TYPE,OBS_STATUS,CONF_STATUS" + "\n"
    i = 0
    for hourlyConsumptionFSA in hourlyConsumptionFSAList:
        # log.debug(hourlyConsumptionFSA)
        csvString = csvString + getHourlyConsumptionFSACsvValue(hourlyConsumptionFSA)
        i=i+1    
    return csvString

################################################### HourlyConsumptionByFSA #####################################################
def parseHourlyConsumptionByFSA(zipFile, csvFileNm):
    log.debug("Parse Hourly Consumption By FSA")
    csvString = "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,COUNTERPART_AREA,ENERGY_FLOWS,DATETIME_LOCAL,OBS_VALUE,TIME_PERIOD,DAYLIGHT_OFFSET,DATETIME_LOCAL_OFFSET,UNIT_MEASURE,UNIT_MULT,MEASURE_TYPE,OBS_STATUS,CONF_STATUS" + "\n"
    date = ''
    total = ''
    fileDate = ''
    startToRead = False
    hourlyConsumptionFSAList = []
    ## sample FSA,DATE,HOUR,CUSTOMER_TYPE,PRICE_PLAN,TOTAL_CONSUMPTION,PREMISE_COUNT
    for line in zipFile.open(csvFileNm).readlines():
        line = line.decode('utf-8')
        hour = ''
        counterpartArea = ''
        energyflow = ''
        hour = ''

        
        if "Created at" in line:
            idx = line.index("Created at")
            fileDate = line[13:]
            date = datetime.strptime(fileDate.replace('\n',''), "%Y-%m-%d %H:%M:%S")
        if startToRead :
            result = [x.strip() for x in line.split(',')]
            date = result[1]
            hour = result[2]

            ## (self, fsa, date, hour, customer_type, price_plan, total_consumption, premise_count)
            hourlyConptFSAObj = HourlyConptFSAReport(result[0], date, hour,  result[3], result[4], result[5], 0)
            hourlyConsumptionFSAList.append(hourlyConptFSAObj)
        if "FSA,DATE,HOUR,CUSTOMER_TYPE,PRICE_PLAN,TOTAL_CONSUMPTION,PREMISE_COUNT"  in line and startToRead is False:
            startToRead = True   
    return getHourlyConsumptionFSACsvFromList(hourlyConsumptionFSAList)

def parseHourlyConsumptionByFSASingular(zipFile, csvFileNm):
    log.debug("Parse Hourly Consumption By FSA")
    csvString = "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,COUNTERPART_AREA,ENERGY_FLOWS,DATETIME_LOCAL,OBS_VALUE,TIME_PERIOD,DAYLIGHT_OFFSET,DATETIME_LOCAL_OFFSET,UNIT_MEASURE,UNIT_MULT,MEASURE_TYPE,OBS_STATUS,CONF_STATUS" + "\n"
    date = ''
    total = ''
    fileDate = ''
    startToRead = False
    
    ## sample FSA,DATE,HOUR,CUSTOMER_TYPE,PRICE_PLAN,TOTAL_CONSUMPTION,PREMISE_COUNT
    data = ''
    file = zipFile.open(csvFileNm).readlines()
    rtn = []
    chunkSize = len(file) / 250000
    y = 0
    for i in range (1, math.ceil(chunkSize)):
        
        data = file[y:(y+250000)]
        hourlyConsumptionFSAList = []    
        for line in data:
            line = line.decode('utf-8')
            hour = ''
            counterpartArea = ''
            energyflow = ''
            hour = ''

            
            if "Created at" in line:
                idx = line.index("Created at")
                fileDate = line[13:]
                date = datetime.strptime(fileDate.replace('\n',''), "%Y-%m-%d %H:%M:%S")
            if startToRead :
                result = [x.strip() for x in line.split(',')]
                date = result[1]
                hour = result[2]

                ## (self, fsa, date, hour, customer_type, price_plan, total_consumption, premise_count)
                hourlyConptFSAObj = HourlyConptFSAReport(result[0], date, hour,  result[3], result[4], result[5], 0)
                hourlyConsumptionFSAList.append(hourlyConptFSAObj)
            if "FSA,DATE,HOUR,CUSTOMER_TYPE,PRICE_PLAN,TOTAL_CONSUMPTION,PREMISE_COUNT"  in line and startToRead is False:
                startToRead = True   
                
        rtn.append(getHourlyConsumptionFSACsvFromList(hourlyConsumptionFSAList))   
        
        y=y+250000
    return rtn



def stringToCsv(filename, data):
    buffer = StringIO(data)

    reader = csv.reader(buffer, skipinitialspace=True)
    with open(filename+".csv", 'w', newline='') as out_file:
        writer = csv.writer(out_file)
        writer.writerows(reader)

def retrieveHourlyConsumptionByFSA(url, isOnlyCurrent):
    reqs = requests.get(url, verify=False)
    soup = BeautifulSoup(reqs.text, "html.parser")
    log.debug(datetime.today())
    urls = []
    for link in soup.find_all('a'):
        log.debug(link.get('href'))

        if ".zip" in link.get('href'): # link has .xml and _v characters.
            fileNm = link.get('href')
            content = ""            
            name = link.get('href').split("_")
            if len(name) > 3 and int(name[2]) > 202304:
                dateNum = name[2]
                log.debug(dateNum)
                with urllib.request.urlopen(url+link.get('href')) as f:
                    
                    zipFile = ZipFile(BytesIO(f.read()))
                    csvFileName = zipFile.infolist()[0].filename
                    csvstr = parseHourlyConsumptionByFSA(zipFile, csvFileName)
                    # if ".csv" in csvFileName:
                    #     csvFileName = csvFileName.replace('.csv', '')
                    stringToCsv(fileNm, csvstr)
                      
            elif fileNm == "PUB_HourlyConsumptionByFSA.zip" and isOnlyCurrent:
                log.debug(fileNm)
                with urllib.request.urlopen(url+link.get('href')) as f:
                    
                    zipFile = ZipFile(BytesIO(f.read()))
                    csvFileName = zipFile.infolist()[0].filename
                    csvstrArr = parseHourlyConsumptionByFSASingular(zipFile, csvFileName)
                    
                    # for csvstr in csvstrArr:
                    #     stringToCsv(fileNm, csvstr)
                    if fileNm == "PUB_HourlyConsumptionByFSA.zip" and isOnlyCurrent:
                        break
                    
        log.debug(datetime.today())        
    if isOnlyCurrent:
        return csvstrArr
    return StringIO(csvstr)

# MAIN.

hourlyConsumptionByFSAUrl = "https://reports-public.ieso.ca/public/HourlyConsumptionByFSA/"

hourlyArr = retrieveHourlyConsumptionByFSA(hourlyConsumptionByFSAUrl, True)

if __name__ == '__main__':
        
    # write to dataframe
    for index, hourlyStr in enumerate(hourlyArr):
        with open("output.csv" + str(index), "w", newline="", encoding="utf-8") as f:
            f.write(hourlyStr)
    # For Hourly data.
    # for hourlyStr in hourlyArr:
        # processing(pd.read_csv(hourlyStr), "on", f'{dt.datetime.now():%Y-%m}')