from Generate_Ingestion_Files.Ingestion_File_Type import IngestionFileType
from Properties_Fetching.Get_Specific_Details import GetSpecificDetails

class InvoiceFile:
    invoice_file_prefix = 'INVOICE_'
    Ingestion_file_type = IngestionFileType.InvoiceFile
    path_to_create_files = "/Users/maheshreddym/Documents/GitHub/Hackathon/Phantoms/Ingestion_txt_files_generated"

    def __init__(self,Env_Variables):
        self.Env_Variables=Env_Variables

    def Create_File(self):
        delimiter=GetSpecificDetails.Get_Delimiter(self.Env_Variables,Ingestion_file_type)
        timestamp = datetime.now().strftime("%Y_%m_%d-%I%M%S")
        try:
            enrollment_file = open(path_to_create_files + invoice_file_prefix + timestamp + '.txt', 'w+')
        except Exception as error:
            print("Error while reading file : {}".format(error))

        #call the builder which creates the file using delimiter enrollment_file