import logging
from datetime import datetime
import os

from Generate_Ingestion_Files.Enrolement_Columns_To_Update_Enum import EnrollementColumnsToUpdate
from Generate_Ingestion_Files.Ingestion_File_Type import IngestionFileType
from Properties_Fetching.Get_Specific_Details import GetSpecificDetails
from Generate_Ingestion_Files.Get_Samplefile_aws_Bucket import FetchSampleAwsBucket
from Data_Ingestion.IngestFile import Ingestion
from Generate_Ingestion_Files.Rawdata_File_Creation import RawDataFile
import uuid
import string
import random

class EnrollmentFile:
    enrollment_file_prefix = 'USERENROLL_D_'
    timestamp = datetime.now().strftime("%Y_%m_%d-%I%M%S")
    row_string = ""
    Ingestion_file_type = IngestionFileType.EnrollmentFile
    Ingestion_file_path = os.path.join(os.getcwd(),"../Ingestion_txt_files_generated")
    AWS_enrollement_file = os.path.join(os.getcwd(),"../Aws_Files/")

    def __init__(self,Env_Variables):
        self.log = logging.getLogger(self.__class__.__name__)
        self.Env_Variables=Env_Variables

    def Create_File(self):
        delimiter=GetSpecificDetails().Get_Delimiter(self.Env_Variables,self.Ingestion_file_type)
        bucket=GetSpecificDetails().Get_S3BucketName(self.Env_Variables)

        timestamp = datetime.now().strftime("%Y_%m_%d-%I%M%S")
        try:
            Ingestion_file_name = EnrollmentFile.enrollment_file_prefix+timestamp+'.txt'
            Ingestion_file_path = EnrollmentFile.Ingestion_file_path+"/"+EnrollmentFile.enrollment_file_prefix+timestamp+'.txt'
            print(f"Ingestion_file_name = {Ingestion_file_name},  Ingestion_file_path = {Ingestion_file_path}")
            enrollment_file_ingest = open(Ingestion_file_path, 'w+')
        except Exception as error:
            print("Error while reading file : {}".format(error))
            raise

        self.log.info(f"The file name for the ingestion of Enrollment file = {enrollment_file_ingest}")

        #Get the sample file from Aws
        aws_obj=FetchSampleAwsBucket(bucket, EnrollmentFile.enrollment_file_prefix)

        #The Aws file will be saved into the folder Aws_Files and returns us the FileName
        #enrollment_file_aws = aws_obj.GetFile()
        enrollment_file_aws="USERENROLL_D_202201281215_015282769457700148513.txt"

        #parse the date from the file by reading one record and convert it into a list for easy updating of values
        enrollment_file_aws = self.AWS_enrollement_file+enrollment_file_aws
        enrollment_file = open(enrollment_file_aws, 'r')
        enrollment_file_row = [enrollment_file.readline()[1:]]
        enrollment_file_row_list = [item.replace("|", ",") for item in enrollment_file_row]
        enrollment_file_row_explode =  enrollment_file_row_list[0].split(",")[:-1]
        enrollment_file_columns = GetSpecificDetails().Get_Data_columns(self.Env_Variables)

        self.log.info(f"Created list from the aws file {enrollment_file_row_explode}")

        index_customerid = enrollment_file_columns.index(EnrollementColumnsToUpdate.customer_id.value)
        index_accountid = enrollment_file_columns.index(EnrollementColumnsToUpdate.account_id.value)
        index_premiseid = enrollment_file_columns.index(EnrollementColumnsToUpdate.premise_id.value)
        index_email = enrollment_file_columns.index(EnrollementColumnsToUpdate.email_id.value)
        index_firstname = enrollment_file_columns.index(EnrollementColumnsToUpdate.first_name.value)
        index_lastname = enrollment_file_columns.index(EnrollementColumnsToUpdate.last_name.value)

        self.buildandUpdateCustomerId(enrollment_file_row_explode, index_customerid)
        self.buildandUpdateAccountId(enrollment_file_row_explode, index_accountid)
        self.buildandUpdatePremiseId(enrollment_file_row_explode, index_premiseid)
        self.buildandUpdateEmailId(enrollment_file_row_explode, index_email)
        self.buildandUpdateFirstName(enrollment_file_row_explode, index_firstname)
        self.buildandUpdateLastName(enrollment_file_row_explode, index_lastname)

        for element in enrollment_file_row_explode:
            self.row_string = self.row_string + element + '|'

        self.log.info(f"After updating the values, writting back to file {self.row_string}")

        enrollment_file_ingest.write(self.row_string)
        enrollment_file_ingest.close()
        ingest_object = Ingestion(Ingestion_file_name, bucket, Ingestion_file_path).uploadtos3bucket()
        RawDataFile(enrollment_file_row_explode[index_accountid]).Create_File()
        self.log.info(f"After updating the values, writting back to file {self.row_string}")

    def random_char(num):
        return ''.join(random.choice(string.ascii_letters) for _ in range(num))

    def buildandUpdateCustomerId(self, list_to_update, position):
        self.customerid =random.randint(11111111, 99999999)
        list_to_update[position] = self.customerid

    def buildandUpdateAccountId(self, list_to_update, position):
        self.accountid = random.randint(11111111, 99999999)
        list_to_update[position] = self.accountid

    def buildandUpdatePremiseId(self, list_to_update, position):
        self.premiseid = random.randint(11111, 99999)
        list_to_update[position] = self.premiseid

    def buildandUpdateEmailId(self, list_to_update, position):
        list_to_update[position] = 'TestUser_Resi_' + self.accountid + "@gmail.com"

    def buildandUpdateFirstName(self, list_to_update, position):
        list_to_update[position] = 'Test User_' + self.accountid

    def buildandUpdateLastName(self, list_to_update, position):
        list_to_update[position] = 'Test User' + self.premiseid

if __name__ == '__main__':
    d = {
        "url": "http://nonprodqaapi.bidgely.com",
        "pilotid": str(40003),
        "access_token": "bb9d8b3f-a740-4631-b634-d2b6c3949298",
        "uuid": "0d06ce34-c4c5-439e-94ab-a622c03dfaf3"
    }
    myobj = EnrollmentFile(d)
    myobj.Create_File()