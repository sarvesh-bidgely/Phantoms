import logging
from datetime import datetime
import os

from Generate_Ingestion_Files.Enrolement_Columns_To_Update_Enum import EnrollementColumnsToUpdate
from Generate_Ingestion_Files.Ingestion_File_Type import IngestionFileType
from Properties_Fetching.Get_Specific_Details import GetSpecificDetails
from Data_Ingestion.IngestFile import Ingestion
import uuid
import string
import random

class RawDataFile:
    raw_file_prefix = 'RAW_D_'
    timestamp = datetime.now().strftime("%Y_%m_%d-%I%M%S")
    Ingestion_file_type = IngestionFileType.RawFile
    Ingestion_file_path = os.path.join(os.getcwd(), "../Ingestion_txt_files_generated")
    AWS_enrollement_file = os.path.join(os.getcwd(), "../Aws_Files/")

    def __init__(self,Env_Variables, accountid):
        self.log = logging.getLogger(self.__class__.__name__)
        self.Env_Variables=Env_Variables
        self.accountid = accountid

    def Create_File(self):
        delimiter=GetSpecificDetails().Get_Delimiter(self.Env_Variables,self.Ingestion_file_type)
        bucket=GetSpecificDetails().Get_S3BucketName(self.Env_Variables)
        print('in Rawdata file', self.accountid)


if __name__ == '__main__':
    d = {
        "url": "http://nonprodqaapi.bidgely.com",
        "pilotid": str(40003),
        "access_token": "bb9d8b3f-a740-4631-b634-d2b6c3949298",
        "uuid": "0d06ce34-c4c5-439e-94ab-a622c03dfaf3"
    }
    myobj = RawDataFile(d)
    myobj.Create_File()


