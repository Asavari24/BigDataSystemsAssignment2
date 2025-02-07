
from io import BytesIO
import os
#from zipfile import ZipFile
import boto3
from dotenv import load_dotenv
import pandas as pd
from datetime import date, datetime, timedelta
#from marshmallow import Schema, fields
import numpy as np
from dateutil.relativedelta import relativedelta
import json
from text_json_class import FinancialElementImportDto, FinancialsDataDto, SymbolFinancialsDto, FinancialElementImportSchema, FinancialsDataSchema, SymbolFinancialsSchema





def get_s3_client():

    #load_dotenv()
    load_dotenv(r'C:\Users\Admin\Desktop\MS Data Architecture and Management\DAMG 7245 - Big Data Systems and Intelligence Analytics\Assignment 2\environment\s3_access.env')

    #s3 = boto3.client('s3')
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name=os.getenv('AWS_REGION')
    )

    return s3


def get_s3_folderlist(bucket_name):

    try:
        s3_client = get_s3_client()

        # Use prefix to specify nested folder path
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='importFiles/', Delimiter='/')

        if 'CommonPrefixes' in response:
            folders = [prefix['Prefix'] for prefix in response['CommonPrefixes']]

        return folders

    except Exception as e:
        print(f"Error retrieving list of folders: {str(e)}")



def get_s3_filelist(bucket_name, folder):

    try:
        s3_client = get_s3_client()

        # Use prefix to specify nested folder path
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder)

        files = []
        if 'Contents' in response:
            files = [obj['Key'] for obj in response['Contents'] if not obj['Key'].endswith('/')]
            
            print("File list retrieved")

        return files
    
    except Exception as e:
        print(f"Error retrieving list of files: {str(e)}")



def get_s3_file(bucket_name, file_key):

    try:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file = response['Body']
        print(f"Retrieved file {file_key}")

        return file
    
    except Exception as e:
        print(f"Error retrieving file: {str(e)}")


def get_ticker(bucket_name):

    try:
        s3_client = get_s3_client()
        response = s3_client.get_object(Bucket=bucket_name, Key="importFiles/ticker.txt")
        ticker = response['Body']
        print("Ticker file retrieved")

        return ticker
    
    except Exception as e:
        print(f"Error retrieving ticker file: {str(e)}")






def npInt_to_str(var):
    return str(list(np.reshape(np.asarray(var), (1, np.size(var)))[0]))[1:-1]

def formatDateNpNum(var):
    dateStr = npInt_to_str(var)
    #return dateStr[0:4]+"-"+dateStr[4:6]+"-"+dateStr[6:8]

    if dateStr.startswith("np.float"):
        return dateStr[11:15]+"-"+dateStr[15:17]+"-"+dateStr[17:19]
    elif dateStr.startswith("np.int"):
        return dateStr[9:13]+"-"+dateStr[13:15]+"-"+dateStr[15:17]






def text_json_converter(bucket_name, files, foldername, dfSym):


    for file_key in files:
        file = get_s3_file(bucket_name, file_key)
        
        if file_key.endswith('num.txt'):
            dfNum = pd.read_table(file, delimiter="\t", low_memory=False)
        elif file_key.endswith('pre.txt'):
            dfPre = pd.read_table(file, delimiter="\t")
        elif file_key.endswith('sub.txt'):
            dfSub = pd.read_table(file, delimiter="\t", low_memory=False)
        elif file_key.endswith('tag.txt'):
            dfTag = pd.read_table(file, delimiter="\t")



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
            periodStartDate = date.fromisoformat(formatDateNpNum(submitted["period"]))
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

        #print(len(filteredDfNum))
        #print(submitted)
        #print(sfDto.__dict__)    
        #json.dumps(sfDto.__dict__)    
        #marshmellowStart = datetime.now()
        result = SymbolFinancialsSchema().dump(sfDto)
        result = json.dumps(result)
        encoded_data = result.encode('utf-8')

        #result = result.replace('"','öäü').replace("'", '"').replace("öäü","'").replace(", ",",")
        #result = result.replace('\\r', '').replace('\\n', ' ')

        print("FilterDfNum size: "+str(len(filteredDfNum))+" Dtos size: "+str((len(sfDto.data.bs)+len(sfDto.data.cf)+len(sfDto.data.ic)))+" Bs: "+str(len(sfDto.data.bs))+" Cf: "+str(len(sfDto.data.cf))+" Ic: "+str(len(sfDto.data.ic))+" Json size: "+str(int(len(str(result)) / 1024))+"kb.")

        #print("Json size "+str(int(len(str(result)) / 1024))+"kb. Time: "+str((datetime.now() - marshmellowStart) / timedelta(milliseconds=1))+"ms.")

        s3_client = get_s3_client()
        s3_path = "exportFiles/"+foldername+"/"+submitted["adsh"]+".json"

        s3_client.upload_fileobj(BytesIO(encoded_data), bucket_name, s3_path)

        endTime = datetime.now()
        print("file "+str(subId+1)+" of "+str(len(dfSub)+1)+" exportFiles/"+foldername+"/"+submitted["adsh"]+".json stored in "+str((endTime - startTime) / timedelta(milliseconds=1))+"ms.")

    fileEndTime = datetime.now()
    print("Time "+str((fileEndTime - fileStartTime) / timedelta(seconds=1))+"sec for "+str(len(dfSub)+1)+" files.")



def get_converted_files(bucket_name):

    ticker = get_ticker(bucket_name)
    ticker_df = pd.read_table(ticker, delimiter="\t", header=None, names=['symbol','cik'])

    folders = get_s3_folderlist(bucket_name)
    
    for folder in folders:
        foldername = folder.split('/')[1]
        print(f"Folder received : {foldername}")
        files = get_s3_filelist(bucket_name, folder)

        text_json_converter(bucket_name, files, foldername, ticker_df)
        

    #foldername = '2009q4'
    #files = ['importFiles/2009q4/num.txt', 'importFiles/2009q4/pre.txt', 'importFiles/2009q4/sub.txt', 'importFiles/2009q4/tag.txt']
    #files = "2009q4"
    #text_json_converter(bucket_name, files, foldername, ticker_df)




bucket_name = 'bigdatasystems2'

get_converted_files(bucket_name)
print("Text to json conversion and S3 upload finished")

