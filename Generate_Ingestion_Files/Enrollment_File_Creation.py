import logging
from datetime import datetime
import os

from Generate_Ingestion_Files.Ingestion_File_Type import IngestionFileType
from Properties_Fetching.Get_Specific_Details import GetSpecificDetails
from Generate_Ingestion_Files.Get_Samplefile_aws_Bucket import FetchSampleAwsBucket

class EnrollmentFile:
    enrollment_file_prefix = 'USERENROLL_D_'
    Ingestion_file_type = IngestionFileType.EnrollmentFile
    Ingestion_file_path = os.path.join(os.getcwd(),"../Ingestion_txt_files_generated")
    print(Ingestion_file_path)

    def __init__(self,Env_Variables):
        self.log = logging.getLogger(self.__class__.__name__)
        self.Env_Variables=Env_Variables

    def Create_File(self):
        delimiter=GetSpecificDetails().Get_Delimiter(self.Env_Variables,self.Ingestion_file_type)
        bucket=GetSpecificDetails().Get_S3BucketName(self.Env_Variables)

        timestamp = datetime.now().strftime("%Y_%m_%d-%I%M%S")
        try:
            Ingestion_file_name = EnrollmentFile.Ingestion_file_path+"/"+EnrollmentFile.enrollment_file_prefix+timestamp+'.txt'
            enrollment_file_ingest = open(Ingestion_file_name, 'w+')
        except Exception as error:
            print("Error while reading file : {}".format(error))
            raise

        self.log.info(f"The file name for the ingestion of Enrollment file = {enrollment_file_ingest}")

        #Get the sample file from Aws
        aws_obj=FetchSampleAwsBucket(bucket, EnrollmentFile.Ingestion_file_type, EnrollmentFile.enrollment_file_prefix)

        #The Aws file will be saved into the folder Aws_Files and returns us the FileName
        enrollment_file_aws = aws_obj.GetFile()

        self.log.info(f"The FileName sameple Reffered for creating ingestion file = {enrollment_file_aws} ")

        #Logic to fetch the awsfile stored in the path and process creating the ingestion file
        #Ingestion file created has to be stored in the path Ingestion_txt_files_generated


if __name__ == '__main__':
    d = {
        "url": "http://aurorauatapi.bidgely.com",
        "pilotid": str(10058),
        "access_token": "56b02db5-b83c-4c5c-b75d-3b6eaee03438",
        "uuid": "7c2b7a79-ec13-497b-9c4d-a3c5fe302264"
    }
    myobj = EnrollmentFile(d)
    myobj.Create_File()