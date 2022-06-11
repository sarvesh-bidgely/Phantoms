import logging
import boto3
import os

from Generate_Ingestion_Files.Ingestion_File_Type import IngestionFileType
from Properties_Fetching.Get_Specific_Details import GetSpecificDetails

class FetchSampleAwsBucket:
    def __init__(self,bucket,prefix,env,filetype):
        self.log = logging.getLogger(self.__class__.__name__)
        self.BucketName=bucket
        self.FilePrefix=prefix
        self.Env_Variables=env
        self.FileType=filetype

    def DowloadFile(self,filename):
        Dest_Path=os.path.join(os.getcwd(),"../Aws_Files/")
        os.chdir(Dest_Path)
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket(self.BucketName)
        my_bucket.download_file(filename, filename)
        self.log.info(f"The File {filename} Downloaded and placed in the folder {Dest_Path}")

    def GetFile(self):
        self.log.info(f"Getting the Latest File by calling the function get_most_recent_s3_object")
        LatestFile=self.get_most_recent_s3_object()
        if LatestFile!=None:
            self.log.info(f"The Latest File name = {LatestFile}")
            self.DowloadFile(LatestFile)
            return str(LatestFile)
        else:
            raise Exception(f"No files available with the file format {self.FilePrefix} to download from the S3 {self.BucketName}")

    def ValidateRowData(self, Row_data):
        if len(Row_data):
            self.log.info(f"Row_data = {Row_data}")
            columns_data=GetSpecificDetails().Get_Data_columns(self.Env_Variables)
            for i,c_val in enumerate(columns_data):
                if i<len(Row_data):
                    self.log.info(f"c_val={c_val}   Row_data[i] = {Row_data[i]} , type(Row_data[i]) = {type(Row_data[i])}")
                    if "account" in c_val and Row_data[i].isalnum():
                        self.Env_Variables["PartnerID"] = Row_data[i]
                        User_details_fetched=GetSpecificDetails().Get_Users_On_Partner_id(self.Env_Variables).get('data')
                        if len(User_details_fetched):
                            return True
                    elif "customer" in c_val and Row_data[i].isalnum():
                        self.Env_Variables["PartnerID"] = Row_data[i]
                        User_details_fetched=GetSpecificDetails().Get_Users_On_Partner_id(self.Env_Variables).get('data')
                        if len(User_details_fetched):
                            return True
                    elif "premise" in c_val and Row_data[i].isalnum():
                        self.Env_Variables["PartnerID"] = Row_data[i]
                        User_details_fetched=GetSpecificDetails().Get_Users_On_Partner_id(self.Env_Variables).get('data')
                        if len(User_details_fetched):
                            return True
            return False
        else:
            return False

    def Is_File_Valid(self, filename):
        self.log.info(f"In Function Is_File_Valid checking validity of file {filename}")

        #Read the aws file
        s3 = boto3.resource('s3')
        self.DowloadFile(filename)

        Dest_Path = os.path.join(os.getcwd(), "../Aws_Files/")
        file_path = os.path.join(Dest_Path,filename)

        f_name, file_extension = os.path.splitext(file_path)
        if file_extension.casefold() in [".csv"]:
            os.rename(file_path, os.path.join(Dest_Path,f_name+".txt"))
        file_path = os.path.join(Dest_Path,f_name+".txt")

        Row_data=[]
        with open(file_path, 'r') as f:
            #print(f"[f.readline()[1:]] =  {[f.readlines()[1:]]} adn type = {type([f.readlines()[1:]])}")
            for i, x in enumerate(f):
                #print(f"i={i}")
                if i<1:
                    Row_data=x.split(GetSpecificDetails().Get_Delimiter(self.Env_Variables, self.FileType))
                    del Row_data[-1]
                    #print(Row_data)
                    self.log.info(f"The line taken to verify is = {Row_data} and the orginal line before splilting is {x}")
                elif i>1:
                    break

        ret_val=self.ValidateRowData(Row_data)

        if ret_val==False:
            self.log.info(f"The filename {filename} is not valid as the data validation failed")
            os.remove(file_path)
            return False
        else:
            return True

    def get_most_recent_s3_object(self):
        s3 = boto3.client('s3')
        paginator = s3.get_paginator( "list_objects_v2" )
        page_iterator = paginator.paginate(Bucket=self.BucketName, Prefix=self.FilePrefix)
        latest = None
        for page in page_iterator:
            if "Contents" in page and latest==None:
                for item in page['Contents']:
                    f_name=item.get("Key")
                    #f_name="USERENROLL_D_202108181936_01.txt"
                    filename, file_extension = os.path.splitext(f_name)
                    self.log.info(f"The filename = {filename}, file_extension={file_extension}")
                    self.log.info(f"File getting processed for validity check is = {item}")

                    if file_extension in [".txt", ".csv"] and self.Is_File_Valid(f_name):
                        if file_extension.casefold() in [".csv"]:
                            latest = filename + ".txt"
                        else:
                            latest=f_name
                        self.log.info(f"The File { latest } is validated moving the file for further process")
                        return latest
        return latest

if __name__ == '__main__':
    d = {
        "url": "http://nonprodqaapi.bidgely.com",
        "pilotid": str(10055),
        "access_token": "bb9d8b3f-a740-4631-b634-d2b6c3949298",
        "uuid": "0d06ce34-c4c5-439e-94ab-a622c03dfaf3"
    }
    a=FetchSampleAwsBucket("bidgely-nspsmb-nonprodqa",'USERENROLL_D_',d,IngestionFileType.EnrollmentFile)
    #a.MoveDowloadedFile('USERENROLL_D_202206101801_016756921518341772442.txt')
    a.GetFile()

# readonly-nonprod 189675173661 189675173661
# readonly-saml 857283459404
# bidgely-jp 224853341059
# bidgely-eu 967871724166
# bidgely-ca 076900401824