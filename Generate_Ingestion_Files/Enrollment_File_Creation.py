from Generate_Ingestion_Files.Ingestion_File_Type import IngestionFileType
from Properties_Fetching.Get_Specific_Details import GetSpecificDetails

class EnrollmentFile:
    enrollment_file_prefix = 'USERENROLL_D_'
    Ingestion_file_type = IngestionFileType.EnrollmentFile
    path_to_create_files = "/Users/sarveshkulkarni/Sites/nsp/"

    def __init__(self,Env_Variables,IngestionFileType):
        self.Env_Variables=Env_Variables
        self.IngestionFileType =  IngestionFileType


    def Create_File(self):
        delimiter=GetSpecificDetails.Get_Delimiter(self.Env_Variables,self.Ingestion_file_type)
        timestamp = datetime.now().strftime("%Y_%m_%d-%I%M%S")
        try:
            enrollment_file = open(path_to_create_files + enrollment_file_prefix + timestamp + '.txt', 'w+')
        except Exception as error:
            print("Error while reading file : {}".format(error))

        #call the builder which creates the file using delimiter enrollment_file



if __name__ == '__main__':
    obj_dic = {
        "url": 'https://nspuatapi.bidgely.com',
        "pilotid": '40003',
        "access_token": '56b02db5-b83c-4c5c-b75d-3b6eaee03438'
    }
    myobj = EnrollmentFile(obj_dic, IngestionFileType)
