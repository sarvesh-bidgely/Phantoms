from Generate_Ingestion_Files.Ingestion_File_Type import IngestionFileType
from Generate_Ingestion_Files.IngestionFile_and_delimiterconfig_mapping import IngestionFileDelimiterConfigMapping
from Properties_Fetching.Get_Specific_Details import GetSpecificDetails

class EnrollmentFile:
    enrollment_file_prefix = 'USERENROLL_D_'
    def __init__(self):
        print (IngestionFileType.RawFile)

    # def Create_File(self,SpecificDetailsObj):
    #     self.SpecificDetailsObj

if __name__ == '__main__':
    p=IngestionFileDelimiterConfigMapping(IngestionFileType.EnrollmentFile)
    print(IngestionFileDelimiterConfigMapping(IngestionFileType.EnrollmentFile).GetFileMappingConfig())
