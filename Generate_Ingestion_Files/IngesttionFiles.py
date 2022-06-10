# python3 IngestionFiles.py
# create the ingestion files required to create a test user


from os import listdir
from os.path import isfile, join
import json
import sys
from Properties_Fetching import Get_Specific_Details

from datetime import datetime
enrolement_json = Get_Specific_Details.GetSpecificDetails()
obj_dic = {
    "url":'https://nspuatapi.bidgely.com',
    "pilotid":'40003',
    "access_token":'56b02db5-b83c-4c5c-b75d-3b6eaee03438'
}
print(enrolement_json.Get_Data_columns(obj_dic))

path_to_create_files = '/Users/sarveshkulkarni/Sites/nsp/'
enrollment_file_prefix = 'USERENROLL_D_'
raw_file_prefix = 'RAW_D_'
invoice_file_prefix = 'INVOICE_'
delimiter = '|'
columnNames = enrolement_json.Get_Data_columns(obj_dic)
columnValues = ['08906632','02005890','97890','RES','sarvesh+RES8904206765@gmail.com','Sarvesh','Kulkarni','RESMAX01 PAYNE','COMBINED SERVICE ADDRESS','UAT2A01','','','UAT2A01 BEAVER BANK','NS','B3J 3S8','NS','','MAILING ADDRESS LINE 2','MAILING ADDRESS LINE 3','','','B3J 3S8','1234567890','HOME','EN','Test','Test','Test','Test','ELECTRIC','1975-06-17','','02B','','33','gu',false,'AMI']

def createIngestionFiles():
    timestamp = datetime.now().strftime("%Y_%m_%d-%I%M%S")
    try:
        enrollment_file = open(path_to_create_files+enrollment_file_prefix+timestamp + '.txt', 'w+')
        raw_file = open(path_to_create_files + raw_file_prefix + timestamp + '.txt', 'w+')
        invoice_file = open(path_to_create_files + invoice_file_prefix + timestamp + '.txt', 'w+')
    except Exception as error:
        print("Error while reading file : {}".format(error))

    writeValuesToFiles(enrollment_file)
    writeValuesToFiles(raw_file)
    writeValuesToFiles(invoice_file)



def writeValuesToFiles(fileInstance):
    for name in columnNames:
        fileInstance.write(name+delimiter)
    fileInstance.write("\n")
    for value in columnValues:
        fileInstance.write(value+delimiter)
    fileInstance.close()

createIngestionFiles()