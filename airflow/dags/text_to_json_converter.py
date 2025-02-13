
from io import BytesIO, StringIO
import pandas as pd
from datetime import date, datetime, timedelta
import numpy as np
from dateutil.relativedelta import relativedelta
import json
from text_json_class import FinancialElementImportDto, FinancialsDataDto, SymbolFinancialsDto, FinancialElementImportSchema, FinancialsDataSchema, SymbolFinancialsSchema
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



s3_hook = S3Hook(aws_conn_id='aws_default')


def get_s3_filelist(bucket_name, folder):

    try:

        # Use prefix to specify nested folder path
        files = s3_hook.list_keys(bucket_name=bucket_name, prefix=folder)

        return files
    
    except Exception as e:
        print(f"Error retrieving list of files: {str(e)}")



def get_s3_file(bucket_name, file_key):

    try:

        # Read file content directly from S3
        file = s3_hook.read_key(bucket_name=bucket_name, key=file_key)
        print(f"Retrieved file {file_key}")

        return file
    
    except Exception as e:
        print(f"Error retrieving file: {str(e)}")


def get_ticker(bucket_name):

    try:

        # Read file content directly from S3
        ticker = s3_hook.read_key(bucket_name=bucket_name, key="importFiles/ticker.txt")
        print("Ticker file retrieved")

        return ticker
    
    except Exception as e:
        print(f"Error retrieving ticker file: {str(e)}")






def npInt_to_str(var):
    return str(list(np.reshape(np.asarray(var), (1, np.size(var)))[0]))[1:-1]

def formatDateNpNum(var):
    dateStr = npInt_to_str(var)
    return dateStr[0:4]+"-"+dateStr[4:6]+"-"+dateStr[6:8]
    #print(dateStr)

    #if dateStr.startswith("np.float"):
    #    return dateStr[11:15]+"-"+dateStr[15:17]+"-"+dateStr[17:19]
    
    #elif dateStr.startswith("np.int"):
    #    return dateStr[9:13]+"-"+dateStr[13:15]+"-"+dateStr[15:17]



def get_converted_files(bucket_name, foldername):

    ticker = get_ticker(bucket_name)
    ticker_file = StringIO(ticker)
    dfSym = pd.read_table(ticker_file, delimiter="\t", header=None, names=['symbol','cik'])

    folder_key = f'importFiles/{foldername}/'
    files = get_s3_filelist(bucket_name, folder_key)

    for file_key in files:
        file = get_s3_file(bucket_name, file_key)
        file_io = StringIO(file)
        
        if file_key.endswith('num.txt'):
            dfNum = pd.read_table(file_io, delimiter="\t", low_memory=False)
        elif file_key.endswith('pre.txt'):
            dfPre = pd.read_table(file_io, delimiter="\t")
        elif file_key.endswith('sub.txt'):
            dfSub = pd.read_table(file_io, delimiter="\t", low_memory=False)
        elif file_key.endswith('tag.txt'):
            dfTag = pd.read_table(file_io, delimiter="\t")

    ticker_file.close()
    #file_io.close()

    fileStartTime = datetime.now()
    for subId in range(len(dfSub)):
        startTime = datetime.now()
        submitted = dfSub.iloc[subId]
        sfDto = SymbolFinancialsDto()
        sfDto.data = FinancialsDataDto()
        sfDto.data.bs = []
        sfDto.data.cf = []
        sfDto.data.ic = []

        try:
            periodStartDate = date.fromisoformat(str(formatDateNpNum(submitted["period"])))
        except ValueError:
            print("File exportFiles/"+foldername+"/"+submitted["adsh"]+".json has Period: "+str(submitted["period"]))
            continue

        sfDto.startDate = periodStartDate
        sfDto.endDate = date.today() #default value
        if submitted["fy"] == np.nan or np.isnan(submitted["fy"]):
            sfDto.year = 0
        else: 
            sfDto.year = submitted["fy"].astype(int)        
        sfDto.quarter = str(submitted["fp"]).strip().upper()

        if sfDto.quarter == "FY" or sfDto.quarter == "CY":
            sfDto.endDate = periodStartDate + relativedelta(months=+12, days=-1)
        elif sfDto.quarter == "H1" or sfDto.quarter == "H2":
            sfDto.endDate = periodStartDate + relativedelta(months=+6, days=-1)
        elif sfDto.quarter == "T1" or sfDto.quarter == "T2" or sfDto.quarter == "T3":
            sfDto.endDate = periodStartDate + relativedelta(months=+4, days=-1)
        elif sfDto.quarter == "Q1" or sfDto.quarter == "Q2" or sfDto.quarter == "Q3" or sfDto.quarter == "Q4":
            sfDto.endDate = periodStartDate + relativedelta(months=+3, days=-1)
        else:
            continue

        #symStr = dfSym[dfSym["cik"]==submitted.cik].symbol.str.upper().astype("string")
        #print(symStr)
        val = dfSym[dfSym["cik"]==submitted["cik"]]

        if len(val) > 0:
            sfDto.symbol = val["symbol"].to_string(index = False).split("\n")[0].split("\\n")[0].strip().split(" ")[0].strip().upper()
            if len(sfDto.symbol) > 19 or len(sfDto.symbol) < 1:
                print("File exportFiles/"+foldername+"/"+submitted["adsh"]+".json has Symbol "+sfDto.symbol)
        else:
            print("File exportFiles/"+foldername+"/"+submitted["adsh"]+".json has Symbol "+val["symbol"].to_string(index = False))
            continue

        sfDto.name = submitted["name"]
        sfDto.country = submitted["countryma"]
        sfDto.city = submitted["cityma"]

        sdDto = FinancialsDataDto()
        #print(filteredDfPre)
        dfNum['value'] = pd.to_numeric(dfNum['value'], errors='coerce')
        dfNum = dfNum.dropna(subset=['value'])
        dfNum['value'] = dfNum['value'].astype(int)
        #filteredDfNum = dfNum[dfNum['adsh'] == submitted['adsh']]       
        filteredDfNum = dfNum.loc[dfNum['adsh'].values == submitted['adsh']]
        filteredDfNum.reset_index()

        for myId in range(len(filteredDfNum)): 
            #print(myId)
            #print(filteredDfNum.iloc[myId])
            myNum = filteredDfNum.iloc[myId]
            myDto = FinancialElementImportDto()
            myTag = dfTag[dfTag["tag"] == myNum['tag']]
            myDto.label = myTag["doc"].to_string(index = False)
            myDto.concept = myNum["tag"]
            #myPre = dfPre[(dfPre['adsh'] == submitted["adsh"]) & (dfPre['tag'] == myNum['tag'])]
            myPre = dfPre.loc[np.where((dfPre['adsh'].values == submitted["adsh"]) & (dfPre['tag'].values == myNum['tag']))]
            myDto.info = myPre["plabel"].to_string(index = False).replace('"',"'")
            myDto.unit = myNum["uom"]
            #print(myNum["value"])        
            myDto.value = myNum["value"]
            #print(myDto.__dict__)
            #FinancialElementImportSchema().dump(myDto)
            if myPre['stmt'].to_string(index = False).strip() == 'BS':
                sfDto.data.bs.append(myDto)
            elif myPre['stmt'].to_string(index = False).strip() == 'CF':
                sfDto.data.cf.append(myDto)
            elif myPre['stmt'].to_string(index = False).strip() == 'IC':
                sfDto.data.ic.append(myDto)


        result = SymbolFinancialsSchema().dump(sfDto)
        result = json.dumps(result)
        encoded_data = result.encode('utf-8')

        #result = result.replace('"','öäü').replace("'", '"').replace("öäü","'").replace(", ",",")
        #result = result.replace('\\r', '').replace('\\n', ' ')

        print("FilterDfNum size: "+str(len(filteredDfNum))+" Dtos size: "+str((len(sfDto.data.bs)+len(sfDto.data.cf)+len(sfDto.data.ic)))+" Bs: "+str(len(sfDto.data.bs))+" Cf: "+str(len(sfDto.data.cf))+" Ic: "+str(len(sfDto.data.ic))+" Json size: "+str(int(len(str(result)) / 1024))+"kb.")

        #print("Json size "+str(int(len(str(result)) / 1024))+"kb. Time: "+str((datetime.now() - marshmellowStart) / timedelta(milliseconds=1))+"ms.")

        #s3_client = get_s3_client()
        s3_path = "exportFiles/"+foldername+"/"+submitted["adsh"]+".json"

        #s3_client.upload_fileobj(BytesIO(encoded_data), bucket_name, s3_path)
        s3_hook.get_conn().upload_fileobj(BytesIO(encoded_data), bucket_name, s3_path)

        endTime = datetime.now()
        print("file "+str(subId+1)+" of "+str(len(dfSub)+1)+" exportFiles/"+foldername+"/"+submitted["adsh"]+".json stored in "+str((endTime - startTime) / timedelta(milliseconds=1))+"ms.")


    fileEndTime = datetime.now()
    print("Time "+str((fileEndTime - fileStartTime) / timedelta(seconds=1))+"sec for "+str(len(dfSub)+1)+" files.")



#bucket_name = 'bigdatasystems2'
#foldername = '2009q2'

#get_converted_files(bucket_name, foldername)
#print("Text to json conversion and S3 upload finished")

