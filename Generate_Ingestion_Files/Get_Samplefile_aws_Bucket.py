from Generate_Ingestion_Files.Ingestion_File_Type import IngestionFileType

class FetchSampleAwsBucket:
    def __init__(self,bucket,filetype,prefix):
        self.BucketName=bucket
        self.FileType=filetype
        self.FilePrefix=prefix

    def GetFile(self):
        return("awsfile_name"+str(self.FileType)+self.FilePrefix)




