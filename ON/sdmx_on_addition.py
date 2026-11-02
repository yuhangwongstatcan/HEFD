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
logging.basicConfig(level="ERROR")
log = logging.getLogger(__name__)

# Outter bound -5 BEFORE OR AFTER -4
dst = {'2023':['2023-03-12','2023-11-05'],
       '2024':['2024-03-10','2024-11-03'],
       '2025':['2025-03-09','2025-11-02'],
       '2026':['2026-03-08','2026-11-01']}

# Generator object.
class GeneratorsReport:
  def __init__(self, generatorRefArea, hour, energyMW, fuelType, output, capability, forecast, available_capacity, date):
    self.generatorRefArea = generatorRefArea
    self.hour = hour
    self.fuelType = fuelType
    self.output = output
    self.capability = capability
    self.energyMW = energyMW
    self.date = date
    self.forecast = forecast
    self.available_capacity = available_capacity

# Intertie object.
class IntertieReport:
  def __init__(self, zone, hour, imp, exp, flow, date):
    self.zone = zone
    self.hour = hour
    self.imp = imp
    self.exp = exp
    self.flow = flow
    self.date = date

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

################################################### Intertie Schedule Report #####################################################
# COUNTERPART AREA US_NY, CA_MINN, CA_MICH, CA_MAN_SK, CA_MAN, PQ_AT, PQ_B5D_B31L, PQ_D4Z, PQ_D5A, PQ_H4Z, PQ_H9A, PQ_P33C, PQ_Q4C, PQ_X2Y 
def getIntertieCsvValue(obsval, date, energyflow, hour, counterpartArea):  
    # return CSV record string.
    # convert time.
    ONTime = getONDate(date, hour).strftime("%Y-%m-%dT%H:%M:%S")
    UTCTime = getUTCDate(date, hour).strftime("%Y-%m-%dT%H:%M:%S")
    return "DATAFLOW,CCEI:DF_HFED_ON(1.0),I,N,CA_ON,"+ getCounterpartAreaCode(counterpartArea) +"," + energyflow + "," + ONTime + "," + str(obsval) + "," + UTCTime + ",0.0,4.0,MW,0,ENERGY,A,F" + "\n"

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

################################################### Generator Ouputs and Capabilities Report #####################################################

def getGenOutputsAndCapabilityCsvValue(obj):  
    # return CSV record string.
    # convert time.
    obsval = obj.energyMW
    date = obj.date
    energyflow = obj.fuelType
    hour = obj.hour
    counterpartArea = obj.generatorRefArea
    ONTime = getONDate(date, hour).strftime("%Y-%m-%dT%H:%M:%S")
    UTCTime = getUTCDate(date, hour).strftime("%Y-%m-%dT%H:%M:%S")
    if obj.output:
        energyflow = energyflow + "_output".upper()
    if obj.capability :
        energyflow = energyflow + "_capability".upper()
    if obj.forecast :
        # if energyflow == "WIND" : # temporary
        #     energyflow = energyflow + "_capacity".upper() # temporary
        # else : # temporary
        #     energyflow = energyflow + "_capability".upper()
        energyflow = energyflow + "_capability".upper()
    if obj.available_capacity :
        energyflow = energyflow + "_available_capacity".upper()
    try:
        return "DATAFLOW,CCEI:DF_HFED_ON(1.0),I,N,CA_ON,"+ getCounterpartAreaCode(counterpartArea) +"," + energyflow + "," + ONTime + "," + str(obsval) + "," + UTCTime + ",0.0,4.0,MW,0,ENERGY,A,F" + "\n"
    except:
        log.debug(counterpartArea + " Error")

def getGenOutputsAndCapabilityMonthCsvValue(obj):  
    # return CSV record string.
    # convert time.
    obsval = obj.energyMW
    if obsval == ' ':
        obsval = 0
    date = obj.date
    energyflow = obj.fuelType
    hour = obj.hour
    counterpartArea = obj.generatorRefArea
    ONTime = getONDate(date, hour).strftime("%Y-%m-%dT%H:%M:%S")
    UTCTime = getUTCDateFixOffset(date, hour, 5).strftime("%Y-%m-%dT%H:%M:%S")
    if obj.output:
        energyflow = energyflow + "_output".upper()
    if obj.capability :
        energyflow = energyflow + "_capability".upper()
    if obj.forecast :
        energyflow = energyflow + "_capability".upper()
    if obj.available_capacity :
        energyflow = energyflow + "_available_capacity".upper()
    try:
        return "DATAFLOW,CCEI:DF_HFED_ON(1.0),I,N,CA_ON,"+ getCounterpartAreaCode(counterpartArea) +"," + energyflow + "," + ONTime + "," + str(obsval) + "," + UTCTime + ",0.0,4.0,MW,0,ENERGY,A,F" + "\n"
    except:
        print(counterpartArea + " Error")

def getIntertieCsvFromList(intertieZoneList):
    csvString = "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,COUNTERPART_AREA,ENERGY_FLOWS,DATETIME_LOCAL,OBS_VALUE,TIME_PERIOD,DAYLIGHT_OFFSET,DATETIME_LOCAL_OFFSET,UNIT_MEASURE,UNIT_MULT,MEASURE_TYPE,OBS_STATUS,CONF_STATUS" + "\n"
    for intertieZone in intertieZoneList:
        log.debug(intertieZone)
        for intertie in intertieZone:
            log.debug(intertie.zone)
            csvString = csvString + getIntertieCsvValue(intertie.imp, intertie.date, "IMPORT", intertie.hour, intertie.zone)
            csvString = csvString + getIntertieCsvValue(intertie.exp, intertie.date, "EXPORT", intertie.hour, intertie.zone)
            csvString = csvString + getIntertieCsvValue(intertie.flow, intertie.date, "FLOW", intertie.hour, intertie.zone)
    return csvString

def getGenOutputCapabilityCsvFromList(genOutputsCapList):
    csvString = "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,COUNTERPART_AREA,ENERGY_FLOWS,DATETIME_LOCAL,OBS_VALUE,TIME_PERIOD,DAYLIGHT_OFFSET,DATETIME_LOCAL_OFFSET,UNIT_MEASURE,UNIT_MULT,MEASURE_TYPE,OBS_STATUS,CONF_STATUS" + "\n"
    for genOutputsCap in genOutputsCapList:
        # log.debug(genOutputsCap)
        csvString = csvString + str(getGenOutputsAndCapabilityCsvValue(genOutputsCap))
    return csvString

def getGenOutputCapabilityMonthCsvFromList(genOutputsCapList):
    csvString = "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,COUNTERPART_AREA,ENERGY_FLOWS,DATETIME_LOCAL,OBS_VALUE,TIME_PERIOD,DAYLIGHT_OFFSET,DATETIME_LOCAL_OFFSET,UNIT_MEASURE,UNIT_MULT,MEASURE_TYPE,OBS_STATUS,CONF_STATUS" + "\n"
    for genOutputsCap in genOutputsCapList:
        # log.debug(genOutputsCap)
        csvString = csvString + str(getGenOutputsAndCapabilityMonthCsvValue(genOutputsCap))
    return csvString

def getHourlyConsumptionFSACsvFromList(hourlyConsumptionFSAList):
    csvString = "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,COUNTERPART_AREA,ENERGY_FLOWS,DATETIME_LOCAL,OBS_VALUE,TIME_PERIOD,DAYLIGHT_OFFSET,DATETIME_LOCAL_OFFSET,UNIT_MEASURE,UNIT_MULT,MEASURE_TYPE,OBS_STATUS,CONF_STATUS" + "\n"
    i = 0
    for hourlyConsumptionFSA in hourlyConsumptionFSAList:
        # log.debug(hourlyConsumptionFSA)
        csvString = csvString + getHourlyConsumptionFSACsvValue(hourlyConsumptionFSA)
        i=i+1    
    return csvString

def parseIntertieXML(xmlfile): 
  
    # create element tree object 
    xmlfile = xmlfile.replace("'", "")
    tree = ET.fromstring(xmlfile) 
    
    # create CSV .
    csvString = "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,COUNTERPART_AREA,ENERGY_FLOWS,DATETIME_LOCAL,OBS_VALUE,TIME_PERIOD,DAYLIGHT_OFFSET,DATETIME_LOCAL_OFFSET,UNIT_MEASURE,UNIT_MULT,MEASURE_TYPE,OBS_STATUS,CONF_STATUS" + "\n"
    date = ''
    hour = ''
    manitoba = ''
    manitoba_sk =''
    michigan = ''
    minnesota = ''
    newyork = ''
    pqat = ''
    total = ''
    energyFlow = ''
    # get root element 
    for child in tree:
        log.debug(child.tag, child.attrib)
        dataDate = ""
        intertieList = []
        if child.tag in "IMODocHeader" : # header information.
            for headerChild in child:
                log.debug(headerChild.tag, headerChild.attrib, headerChild.text)
                if headerChild.tag in "CreatedAt" :
                    log.debug("Create date: " + headerChild.text)
                    dataDate = headerChild.text
                    date = dt.datetime.strptime(dataDate, "%Y-%m-%dT%H:%M:%S")
        if child.tag in "IMODocBody": # body information.
            intertieZoneList = []
            for bodyChild in child:
                
                log.debug(bodyChild.tag)
                log.debug(bodyChild.attrib)
                log.debug(bodyChild.text)
                if bodyChild.tag in "Date": # date
                    dataDate = bodyChild.text
                    date = dt.datetime.strptime(dataDate, "%Y-%m-%d")
                    #dt.datetime.strptime(aqewChild.text, "%Y-%m-%d")
                if bodyChild.tag in "IntertieZone": # body information.
                    
                    flowArray = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
                    intertieReportList = []
                    for intertieZoneChild in bodyChild:
                        changedDate = ''
                        numberOfHour = 0
                        
                        log.debug(intertieZoneChild.tag)
                        log.debug(intertieZoneChild.attrib)
                        log.debug(intertieZoneChild.text)
                        if intertieZoneChild.tag in "IntertieZoneName":
                            counterpartArea = intertieZoneChild.text
                            zone = counterpartArea
                        if intertieZoneChild.tag in "Schedules":
                            for schedulesChild in intertieZoneChild: # the hour of the day information.
                                log.debug(schedulesChild.tag)
                                izObj = IntertieReport(zone,0,0,0,0,date)
                                for scheduleChild in schedulesChild:
                                    if scheduleChild.tag in "Hour" :
                                        hour = scheduleChild.text
                                        changedDate = getONDate(date, hour)
                                        izObj.hour = hour
                                    if scheduleChild.tag in "Import" :
                                        energyFlow = scheduleChild.text
                                        izObj.imp = energyFlow
                                    if scheduleChild.tag in "Export" :
                                        energyFlow = scheduleChild.text
                                        izObj.exp = energyFlow
                                numberOfHour = numberOfHour + 1
                                intertieReportList.append(izObj)

                        if intertieZoneChild.tag in "Actuals":
                            for actual in intertieZoneChild:
                                actualHour = 0
                                actualFlow = 0
                                for actualChild in actual:
                                    if actualChild.tag in "Hour":
                                        actualHour = int(actualChild.text)
                                    if actualChild.tag in "Flow":
                                        actualFlow = int(float(actualChild.text))
                                    if(actualHour != 0):
                                        flowArray[int(actualHour)] = int(flowArray[int(actualHour)]) + actualFlow
                        
                        
                        log.debug("IntertieZone : " )
                        # add flow.
                        for intertieReport in intertieReportList:
                            hour = intertieReport.hour
                            flow = math.floor(flowArray[int(hour)] / 12)
                            intertieReport.flow = flow

                    intertieZoneList.append(intertieReportList)
            log.debug("Zone list size: " + str(len(intertieZoneList)))
            return getIntertieCsvFromList(intertieZoneList)

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


################################################### GenOutputCapability #####################################################
def parseGenOutputCapacbilityXML(xmlfile): 
    log.debug("Parse Gen Output Capability XML.")
    # create element tree object 
    xmlfile = xmlfile.replace("'", "")
    tree = ET.fromstring(xmlfile) 
    
    # create CSV .
    csvString = "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,COUNTERPART_AREA,ENERGY_FLOWS,DATETIME_LOCAL,OBS_VALUE,TIME_PERIOD,DAYLIGHT_OFFSET,DATETIME_LOCAL_OFFSET,UNIT_MEASURE,UNIT_MULT,MEASURE_TYPE,OBS_STATUS,CONF_STATUS" + "\n"
    date = ''
    hour = ''
    pqat = ''
    total = ''
    energyFlow = ''
    # get root element 
    for child in tree:
        # log.debug(child.tag, child.attrib)
        dataDate = ""
        intertieList = []
        if child.tag in "IMODocHeader" : # header information.
            for headerChild in child:
                # log.debug(headerChild.tag, headerChild.attrib, headerChild.text)
                if headerChild.tag in "CreatedAt" :
                    # log.debug("Create date: " + headerChild.text)
                    dataDate = headerChild.text
                    date = dt.datetime.strptime(dataDate, "%Y-%m-%dT%H:%M:%S")
        if child.tag in "IMODocBody": # body information.
            genOutputCapList = []
            for bodyChild in child:
                # log.debug(bodyChild.tag, bodyChild.attrib, bodyChild.text)
                if bodyChild.tag in "Date": # date
                    dataDate = bodyChild.text
                    date = dt.datetime.strptime(dataDate, "%Y-%m-%d")
                    #dt.datetime.strptime(aqewChild.text, "%Y-%m-%d")
                if bodyChild.tag in "Generators": # body information.
                    
                    flowArray = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
                    for generatorChild in bodyChild:
                        changedDate = ''
                        numberOfHour = 0
                        refArea = ''
                        fuelType = ''
                        # create object for outputs.
                        if generatorChild.tag in "Generator":
                            for generator in  generatorChild:
                                if generator.tag in "GeneratorName":
                                    # log.debug(generator.tag)
                                    refArea = generator.text
                                if generator.tag in "FuelType":
                                    # log.debug(generator.tag)
                                    fuelType = generator.text
                                if generator.tag in "Outputs":
                                    # log.debug(generator.tag)
                                    for outputChilds in generator: # loop and create Gen Report Object.
                                        genCapObj = GeneratorsReport(refArea, 0,0,fuelType,True,False,False,False,date)
                                        # log.debug(outputChilds.tag)
                                        if outputChilds.tag in "Output":
                                            for outputInfo in outputChilds:
                                                if outputInfo.tag in "Hour":
                                                    genCapObj.hour = outputInfo.text
                                                if outputInfo.tag in "EnergyMW":
                                                    genCapObj.energyMW = outputInfo.text
                                            genOutputCapList.append(genCapObj)
                                # create object for capabilities.
                                if generator.tag in "Capabilities":
                                    # log.debug(generator.tag)
                                    for capabilitiesChild in generator: # loop and create Gen Report Object.
                                        # log.debug(capabilitiesChild.tag)
                                        if capabilitiesChild.tag in "Capability" and (fuelType != "WIND" or fuelType != "SOLAR"):
                                            genCapObj = GeneratorsReport(refArea, 0,0,fuelType,False,True,False,False,date) 
                                            for capabilityInfo in capabilitiesChild:
                                                if capabilityInfo.tag in "Hour":
                                                    genCapObj.hour = capabilityInfo.text
                                                if capabilityInfo.tag in "EnergyMW":
                                                    genCapObj.energyMW = capabilityInfo.text
                                        if capabilitiesChild.tag in "Capability" and (fuelType == "WIND" or fuelType == "SOLAR") :
                                            genCapObj = GeneratorsReport(refArea, 0,0,fuelType,False,False,True,False,date) # forecast
                                            for capabilityInfo in capabilitiesChild:
                                                if capabilityInfo.tag in "Hour":
                                                    genCapObj.hour = capabilityInfo.text
                                                if capabilityInfo.tag in "EnergyMW":
                                                    genCapObj.energyMW = capabilityInfo.text
                                            genOutputCapList.append(genCapObj)
                                if generator.tag in "Capacities" and (fuelType == "WIND" or fuelType == "SOLAR"):
                                    # log.debug(generator.tag)
                                    for capabilitiesChild in generator: # loop and create Gen Report Object.
                                        genCapObj = GeneratorsReport(refArea, 0,0,fuelType,False,False,False,True,date) 
                                        # log.debug(capabilitiesChild.tag)
                                        if capabilitiesChild.tag in "AvailCapacity":
                                            for capabilityInfo in capabilitiesChild:
                                                if capabilityInfo.tag in "Hour":
                                                    genCapObj.hour = capabilityInfo.text
                                                if capabilityInfo.tag in "EnergyMW":
                                                    genCapObj.energyMW = capabilityInfo.text
                                            genOutputCapList.append(genCapObj)
                                if generator.tag in "Forecast":
                                    # log.debug(generator.tag)
                                    for capabilitiesChild in generator: # loop and create Gen Report Object.
                                        genCapObj = GeneratorsReport(refArea, 0,0,fuelType,False,False,True,False,date) 
                                        # log.debug(capabilitiesChild.tag)
                                        if capabilitiesChild.tag in "Forecast":
                                            for capabilityInfo in capabilitiesChild:
                                                if capabilityInfo.tag in "Hour":
                                                    genCapObj.hour = capabilityInfo.text
                                                if capabilityInfo.tag in "EnergyMW":
                                                    genCapObj.energyMW = capabilityInfo.text
                                            genOutputCapList.append(genCapObj)

            log.debug("Zone list size: " + str(len(genOutputCapList)))
            return getGenOutputCapabilityCsvFromList(genOutputCapList)
        
def parseGenOutputCapacbilityXMLMonth(csvdf): 
    
    # create CSV .
    csvString = "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,COUNTERPART_AREA,ENERGY_FLOWS,DATETIME_LOCAL,OBS_VALUE,TIME_PERIOD,DAYLIGHT_OFFSET,DATETIME_LOCAL_OFFSET,UNIT_MEASURE,UNIT_MULT,MEASURE_TYPE,OBS_STATUS,CONF_STATUS" + "\n"
    date = ''
    energyFlow = ''
    # print(rows)
    # 2025-01-01,ABKENORA,HYDRO,Capability,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,11,
    genOutputCapMonthList = []
    for index, row in csvdf.iterrows():
        # print(index)
        date = dt.datetime.strptime(row['Delivery Date'], "%Y-%m-%d")
        refArea = row['Generator']
        # energyFlow = row['Fuel Type'] + '_' + row['Measurement'].upper()
        energyFlow = row['Fuel Type'].upper()
       
        if row['Measurement'] in "Output":
            for hour in range(1,25):
                genCapObj = GeneratorsReport(refArea, 0,0,energyFlow,True,False,False,False,date)
                genCapObj.hour = int(hour)
                genCapObj.energyMW = row['Hour ' + str(hour)]
                genOutputCapMonthList.append(genCapObj)
        # create object for capabilities.
        if row['Measurement'] in "Capability":
            for hour in range(1,25):
                genCapObj = GeneratorsReport(refArea, 0,0,energyFlow,False,True,False,False,date)
                genCapObj.hour = int(hour)
                genCapObj.energyMW = row['Hour ' + str(hour)]
                genOutputCapMonthList.append(genCapObj)
        elif row['Measurement'] in "Available Capacity":
            for hour in range(1,25):
                genCapObj = GeneratorsReport(refArea, 0,0,energyFlow,False,False,False,True,date)
                genCapObj.hour = int(hour)
                genCapObj.energyMW = row['Hour ' + str(hour)]
                genOutputCapMonthList.append(genCapObj)
        elif row['Measurement'] in "Forecast":
            for hour in range(1,25):
                genCapObj = GeneratorsReport(refArea, 0,0,energyFlow,False,False,True,False,date)
                genCapObj.hour = int(hour)
                genCapObj.energyMW = row['Hour ' + str(hour)]
                genOutputCapMonthList.append(genCapObj)   
    print("Zone list size: " + str(len(genOutputCapMonthList)))
    return getGenOutputCapabilityMonthCsvFromList(genOutputCapMonthList)


################################################### GenOutputCapability End #####################################################

# HOEP Begin
def parseHOEPHourly(csvdf): 
    
    # create CSV .
    csvString = "STRUCTURE,STRUCTURE_ID,ACTION,FREQ,REF_AREA,COUNTERPART_AREA,ENERGY_FLOWS,DATETIME_LOCAL,OBS_VALUE,TIME_PERIOD,DAYLIGHT_OFFSET,DATETIME_LOCAL_OFFSET,UNIT_MEASURE,UNIT_MULT,MEASURE_TYPE,OBS_STATUS,CONF_STATUS" + "\n"
    date = ''
    energyFlow = ''
    # print(rows)
    df = csvdf.rename(columns={"Date": "TIME_PERIOD", "Hour": "HOUR", "HOEP": "OBS_VALUE"})
    # this part loops through the date and time fields, figures out if it's daylight savings time, and sets the local and daylight offsets
    local_offset = []
    daylight_offset = []

    # decrement hour by 1. 
    df["HOUR"] = df["HOUR"].astype(int) - 1
    # df["HOUR"] = df["HOUR"].astype("string")
    ################################ Time correction START #######################################

    # loop through the rows using iterrows()

    ################################ Time correction END #######################################

    # create new columns
    df["TIME_PERIOD"] = pd.to_datetime(df["TIME_PERIOD"])
    df["STRUCTURE"] = "DATAFLOW"
    df["STRUCTURE_ID"] = "CCEI:DF_HFED_ON(1.0)"
    df["ACTION"] = "I"
    df["DAYLIGHT_OFFSET"] = 0
    df["DATETIME_LOCAL_OFFSET"] = 4
    df["DATETIME_LOCAL"] = (
        df["TIME_PERIOD"] + pd.to_timedelta(df["HOUR"], unit="h")
    ).astype(str)
    df["DATETIME_LOCAL"] = (
        df["DATETIME_LOCAL"].str.slice(0, 10)
        + "T"
        + df["DATETIME_LOCAL"].str.slice(11, 20)
    )
    df["TIME_PERIOD"] = (
        # df["TIME_PERIOD"] + pd.to_timedelta(df["HOUR"] + df["DATETIME_LOCAL_OFFSET"], unit="h")
        df["TIME_PERIOD"] + pd.to_timedelta(df["HOUR"] + df["DATETIME_LOCAL_OFFSET"], unit="h")
    ).astype(str)
    df["TIME_PERIOD"] = (
        df["TIME_PERIOD"].str.slice(0, 10) + "T" + df["TIME_PERIOD"].str.slice(11, 20)
    )
    df["UNIT_MEASURE"] = "MW"
    df["FREQ"] = "H"
    df["REF_AREA"] = "CA_ON"
    df["COUNTERPART_AREA"] = "_Z"
    df["ENERGY_FLOWS"] = "HOEP"
    df["UNIT_MULT"] = "0"
    df["MEASURE_TYPE"] = "ENERGY"
    df["OBS_STATUS"] = "A"
    df["CONF_STATUS"] = "F"

    df = df.drop(columns=["HOUR"])

    # correct column order
    df = df[
        [
            "STRUCTURE",
            "STRUCTURE_ID",
            "ACTION",
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
    # print(df)
    
    
    # return df.to_csv(index=False)
    return df

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

def retrieveGenOutputCapability(url, isOnlyCurrent):
    reqs = requests.get(url, verify=False)
    soup = BeautifulSoup(reqs.text, "html.parser")

    urls = []
    for link in soup.find_all('a'):
        log.debug(link.get('href'))
        if ".xml" in link.get('href') and "_v" not in link.get('href') : # link has .xml and _v characters.
            fileNm = link.get('href')
            content = ""
            with urllib.request.urlopen(url+link.get('href')) as f:
                content = f.read().decode('utf-8')
                # log.debug(content)
                log.debug("File Size: " + str(len(content)))
                content = content.replace("\n", "")
                # content = content.replace('<?xml version="1.0" encoding="UTF-8"?>', "")

                content = content.replace('<?xml-stylesheet type="text/xsl" href="https://reports-public.ieso.ca/docrefs/stylesheet/GenOutputCapability_HTML_t1-4.xsl" ?>', "")
                content = content.replace("docID=\"GenOutputCapability\"","")
                content = content.replace("xmlns=\"http://www.theIMO.com/schema\"","")
                content = content.replace("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"","")
                content = content.replace("xsi:schemaLocation=\"http://www.theIMO.com/schema","")
                content = content.replace("https://reports-public.ieso.ca/docrefs/schema/GenOutputCapability_r3.xsd\"","")
                content = content.replace("<Document     >","<Document>")
                csvstr = parseGenOutputCapacbilityXML(content)
                # stringToCsv(fileNm.replace('.xml',''), csvstr)
                if fileNm == "PUB_GenOutputCapability.xml" and isOnlyCurrent:
                    break
    
    return StringIO(csvstr)

def retrieveIndustrLoadBySector(url, isOnlyCurrent):
    
    reqs = requests.get(url, verify=False)
    soup = BeautifulSoup(reqs.text, "html.parser")

    urls = []
    for link in soup.find_all('a'):
        log.debug(link.get('href'))
        if ".xml" in link.get('href') and "_v" not in link.get('href'): # link has .xml and _v characters.
            fileNm = link.get('href')
            content = ""
            with urllib.request.urlopen(url+link.get('href')) as f:
                content = f.read().decode('utf-8')
                # log.debug(content)
                log.debug(len(content))
                content = content.replace("\n", "")
                # content = content.replace('<?xml version="1.0" encoding="UTF-8"?>', "")
                content = content.replace('<?xml-stylesheet type="text/xsl" href="https://reports-public.ieso.ca/docrefs/stylesheet/IndustrLoadBySector_HTML_t1-1.xsl" ?>', "")
                content = content.replace("docID=\"IndustrLoadBySector\"","")
                content = content.replace("xmlns=\"http://www.ieso.ca/schema\"","")
                content = content.replace("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"","")
                content = content.replace("xsi:schemaLocation=\"http://www.ieso.ca/schema","")
                content = content.replace("https://reports-public.ieso.ca/docrefs/schema/IndustrLoadBySector_r1.xsd\"","")
                content = content.replace("<Document     >","<Document>")
                csvstr = parseXML(content)
                log.debug(csvstr)
                # to file.
                # stringToCsv(fileNm.replace('.xml',''), csvstr)
                if fileNm == "PUB_IndustrLoadBySector.xml" and isOnlyCurrent:
                    break
    return StringIO(csvstr)

def retrieveIntertieScheduleFlowReport(url, isOnlyCurrent):
    reqs = requests.get(url, verify=False)
    soup = BeautifulSoup(reqs.text, "html.parser")

    urls = []
    for link in soup.find_all('a'):
        log.debug(link.get('href'))
        if ".xml" in link.get('href') and "_v" not in link.get('href'): # link has .xml and _v characters.
            fileNm = link.get('href')
            content = ""
            with urllib.request.urlopen(url+link.get('href')) as f:
                content = f.read().decode('utf-8')
                # log.debug(content)
                log.debug(len(content))
                content = content.replace("\n", "")
                # content = content.replace('<?xml version="1.0" encoding="UTF-8"?>', "")
                content = content.replace('<?xml-stylesheet type="text/xsl" href="https://reports-public.ieso.ca/docrefs/stylesheet/IntertieScheduleFlow_HTML_t1-2.xsl" ?>', "")
                content = content.replace("docID=\"IntertieScheduleFlow\"","")
                content = content.replace("xmlns=\"http://www.theIMO.com/schema\"","")
                content = content.replace("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"","")
                content = content.replace("xsi:schemaLocation=\"http://www.theIMO.com/schema","")
                content = content.replace("https://reports-public.ieso.ca/docrefs/schema/IntertieScheduleFlow_r2.xsd\"","")
                content = content.replace("<Document     >","<Document>")
                csvstr = parseIntertieXML(content)
                log.debug(csvstr)
                # to file.
                # stringToCsv(fileNm.replace('.xml',''), csvstr)
                if fileNm == "PUB_IntertieScheduleFlow.xml" and isOnlyCurrent:
                    break
    return StringIO(csvstr)

def retrieveGenOutputCapabilityMonth(url, isOnlyCurrent):
    reqs = requests.get(url, verify=False)
    soup = BeautifulSoup(reqs.text, "html.parser")

    urls = []
    for link in soup.find_all('a'):
        print(link.get('href'))
        if ".csv" in link.get('href') and "_v" not in link.get('href'): # link has .xml and _v characters.
            fileNm = link.get('href')
            content = ""
            toLoad = True
            # if fileNm == "PUB_GenOutputCapabilityMonth_201911.csv":
            # fileNmSplit = fileNm.split("_")
            # if len(fileNmSplit) > 2:
            #     fileDate = fileNmSplit[2]
            #     fileDate = fileDate[0:6]
            
            #     if int(fileDate) > 202403:
            #         toLoad = True
            if toLoad :
                with urllib.request.urlopen(url+link.get('href')) as f:

                    content = f.read().decode('utf-8')
                    # print(content)
                    # print("File Size: " + str(len(content)))
                    content = content.replace("\n", "")
                    # content = content.replace('<?xml version="1.0" encoding="UTF-8"?>', "")
                    data = pd.read_csv(url+link.get('href'), on_bad_lines='skip', skiprows=3, index_col=False)
                    # if fileNm != "PUB_GenOutputCapabilityMonth.csv":
                    if data.size > 250000 and not isOnlyCurrent:
                        first_slice = data[:len(data)//2]
                        second_slice = data[len(data)//2:]
                        csvstrfirstslice = parseGenOutputCapacbilityXMLMonth(first_slice)
                        csvstrsecondslice = parseGenOutputCapacbilityXMLMonth(second_slice)
                        # stringToCsv((fileNm + '_1').replace('.csv',''), csvstrfirstslice)
                        # stringToCsv((fileNm + '_2').replace('.csv',''), csvstrsecondslice)
                    else:
                        if data.size > 250000 and isOnlyCurrent:
                            first_slice = data[:len(data)//2]
                            second_slice = data[len(data)//2:]
                            csvstrfirstslice = parseGenOutputCapacbilityXMLMonth(first_slice)
                            csvstrsecondslice = parseGenOutputCapacbilityXMLMonth(second_slice)
                            rtn = [StringIO(csvstrfirstslice), StringIO(csvstrsecondslice)]
                        else:
                            csvstr = parseGenOutputCapacbilityXMLMonth(data)
                            # stringToCsv(fileNm.replace('.csv',''), csvstr)
                            rtn = [StringIO(csvstr)]
                        
                        return rtn
                        
                    if fileNm == "PUB_GenOutputCapabilityMonth.csv" and isOnlyCurrent:
                        break
    return StringIO(csvstr)

def retrieveHourlyHOEPReport(url, isOnlyCurrent):
    reqs = requests.get(url, verify=False)
    soup = BeautifulSoup(reqs.text, "html.parser")
    csvstr =''
    urls = []
    for link in soup.find_all('a'):
        print(link.get('href'))
        if ".csv" in link.get('href') and "_v" not in link.get('href'): # link has .xml and _v characters.
            fileNm = link.get('href')
            content = ""
            toLoad = True
            if toLoad :
                with urllib.request.urlopen(url+link.get('href')) as f:

                    content = f.read().decode('utf-8')
                    # print(content)
                    # print("File Size: " + str(len(content)))
                    content = content.replace("\n", "")
                    # content = content.replace('<?xml version="1.0" encoding="UTF-8"?>', "")
                    data = pd.read_csv(url+link.get('href'), on_bad_lines='skip', skiprows=3, index_col=False)
                    # if fileNm != "PUB_GenOutputCapabilityMonth.csv":
                    if data.size > 250000 and not isOnlyCurrent:
                        first_slice = data[:len(data)//2]
                        second_slice = data[len(data)//2:]
                        csvstrfirstslice = parseHOEPHourly(first_slice)
                        csvstrsecondslice = parseHOEPHourly(second_slice)
                        # stringToCsv((fileNm + '_1').replace('.csv',''), csvstrfirstslice)
                        # stringToCsv((fileNm + '_2').replace('.csv',''), csvstrsecondslice)
                    else:
                        if data.size > 250000 and isOnlyCurrent:
                            first_slice = data[:len(data)//2]
                            second_slice = data[len(data)//2:]
                            csvstrfirstslice = parseHOEPHourly(first_slice)
                            csvstrsecondslice = parseHOEPHourly(second_slice)
                            rtn = [StringIO(csvstrfirstslice), StringIO(csvstrsecondslice)]
                        else:
                            df = parseHOEPHourly(data)
                            # stringToCsv(fileNm.replace('.csv',''), csvstr)
                            rtn = df
                        
                        return rtn
                        
                    if fileNm == "PUB_PriceHOEPPredispOR.csv" and isOnlyCurrent:
                        break
    return StringIO(csvstr)

# MAIN.

industrLoadBySectorhtmlUrl = "http://reports.ieso.ca/public/IndustrLoadBySector/"
intertieScheduleFlowReportUrl = "https://reports-public.ieso.ca/public/IntertieScheduleFlow/"
genOutputCapabilityUrl = "https://reports-public.ieso.ca/public/GenOutputCapability/"
genOutputCapabilityMonthUrl = "https://reports-public.ieso.ca/public/GenOutputCapabilityMonth/"
hourlyConsumptionByFSAUrl = "https://reports-public.ieso.ca/public/HourlyConsumptionByFSA/"
hourlyHOEPReportUrl = "https://reports-public.ieso.ca/public/PriceHOEPPredispOR/"

csvStringArray = [None] * 6
csvStringArray[0] = retrieveIntertieScheduleFlowReport(intertieScheduleFlowReportUrl, True) # only daily data.
csvStringArray[1] = retrieveGenOutputCapability(genOutputCapabilityUrl, True) # only daily data.
csvStringArray[2] = retrieveIndustrLoadBySector(industrLoadBySectorhtmlUrl, True) # no daily maybe monthly data.
csvStringArray[3] = retrieveHourlyHOEPReport(hourlyHOEPReportUrl, True)
hourlyArr = retrieveHourlyConsumptionByFSA(hourlyConsumptionByFSAUrl, True)
monthArr = retrieveGenOutputCapabilityMonth(genOutputCapabilityMonthUrl, True)
if len(monthArr)==2:
    csvStringArray[4] = monthArr[0]
    csvStringArray[5] = monthArr[1]
else:
    csvStringArray[4] = monthArr[0]

# write to dataframe
# with open("output.csv", "w", newline="", encoding="utf-8") as f:
#     f.write(csvHoep[0].getvalue())

try:
    # log.debug(csvString)
    intertieScheduleFlowdf = pd.read_csv(csvStringArray[0])
    genOutputCapdf = pd.read_csv(csvStringArray[1])
    industrLoadBySectordf = pd.read_csv(csvStringArray[2])
    hoepdf = csvStringArray[3]
    
    if len(monthArr)==2:
        genOutputCapMonthdf = pd.read_csv(csvStringArray[4])
        genOutputCapMonthdf2 = pd.read_csv(csvStringArray[5])
    else:
        genOutputCapMonthdf = pd.read_csv(csvStringArray[4])
    # log.debug(df)

except urllib3.exceptions.MaxRetryError as e:
    log.exception(e)
    mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT,f"Max retries exceeded with url: {e.reason}")
    exit()
except urllib3.exceptions.TimeoutError as e:
    log.exception(e)
    mail_recipient (SMTP_MAIL_TO, SMTP_MAIL_SUBJECT, f"Request timed out: {e}")
    exit()

if __name__ == '__main__':
    processing(intertieScheduleFlowdf, "on_addition", f'{dt.datetime.now():%Y-%m}')
    processing(genOutputCapdf, "on_addition", f'{dt.datetime.now():%Y-%m}')
    processing(industrLoadBySectordf, "on_addition", f'{dt.datetime.now():%Y-%m}')
    processing(hoepdf, "on_addition", f'{dt.datetime.now():%Y-%m}')
    
    if len(monthArr)==2:
        processing(genOutputCapMonthdf, "on_addition", f'{dt.datetime.now():%Y-%m}')
        processing(genOutputCapMonthdf2, "on_addition", f'{dt.datetime.now():%Y-%m}')
    else:
        processing(genOutputCapMonthdf, "on_addition", f'{dt.datetime.now():%Y-%m}')
        
    # For Hourly data.
    # for hourlyStr in hourlyArr:
    #     processing(pd.read_csv(hourlyStr), "on", f'{dt.datetime.now():%Y-%m}')
