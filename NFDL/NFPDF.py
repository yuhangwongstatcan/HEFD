from tabula import read_pdf
from tabula import convert_into
from pathlib import Path
from os import listdir
from os.path import isfile, join
import pandas as pd

# path = "./dwldfiles"
path = "./test"

onlyfiles = [f for f in listdir(path) if isfile(join(path, f))]

for file in onlyfiles:
    df = pd.DataFrame() # create a dataframe.
    ###### Constructing the dataframe 
    # create new columns
    df["STRUCTURE"] = "DATAFLOW"
    df['STRUCTURE_ID'] = "CCEI:DF_HFED_NL(1.0)"
    df['ACTION'] = "I"
    df["DAYLIGHT_OFFSET"] = ""
    df["DATETIME_LOCAL_OFFSET"] = ""
    df["DATETIME_LOCAL"] = ""
    df["DATETIME_LOCAL"] = ""
    df["TIME_PERIOD"] = ""
    df["UNIT_MEASURE"] = "MW"
    df["FREQ"] = "N"
    df["REF_AREA"] = "CA_NL"
    df["COUNTERPART_AREA"] = "_Z"
    df["ENERGY_FLOWS"] = "NSI"
    df["UNIT_MULT"] = "0"
    df["MEASURE_TYPE"] = "ENERGY"
    df["OBS_STATUS"] = "A"
    df["CONF_STATUS"] = "F"
    df['OBS_VALUE'] = ""
    #********************

    # correct column order
    df = df[
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
    ######
    
    print(file)
    dfs=read_pdf(path + "/" + file,pages='all')
    filename = Path(file).stem
    df.to_csv("./test/" + filename+".csv")  
    print("File Name: " + filename)
    # df.convert_into(path + "/" + file, "output.csv", output_format="csv", pages='all')
    # print(dfs)
    print("List Size: " + str(len(dfs)))

    for df in dfs:
        print(df)


    