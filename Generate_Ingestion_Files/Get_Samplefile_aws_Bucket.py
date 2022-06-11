import logging
import boto3
import os

class FetchSampleAwsBucket:
    def __init__(self,bucket,prefix):
        self.log = logging.getLogger(self.__class__.__name__)
        self.BucketName=bucket
        self.FilePrefix=prefix

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
            filename=LatestFile.get("Key")
            self.log.info(f"The Latest File name = {filename}, Full Aws Details = {LatestFile}")
            print(filename,LatestFile)
            self.DowloadFile(filename)
            return str(filename)
        else:
            raise Exception(f"No files available with the file format {self.FilePrefix} to download from the S3 {self.BucketName}")

    def get_most_recent_s3_object(self):
        s3 = boto3.client('s3')
        paginator = s3.get_paginator( "list_objects_v2" )
        page_iterator = paginator.paginate(Bucket=self.BucketName, Prefix=self.FilePrefix)
        latest = None
        for page in page_iterator:
            if "Contents" in page:
                latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
                if latest is None or latest2['LastModified'] > latest['LastModified']:
                    latest = latest2
        return latest

if __name__ == '__main__':
    a=FetchSampleAwsBucket("bidgely-nspsmb-nonprodqa",'USERENROLL_D_')
    #a.MoveDowloadedFile('USERENROLL_D_202206101801_016756921518341772442.txt')
    a.GetFile()

# readonly-nonprod 189675173661 189675173661
# readonly-saml 857283459404
# bidgely-jp 224853341059
# bidgely-eu 967871724166
# bidgely-ca 076900401824