from os import listdir
from os.path import isfile, join
import json
import requests
import sys
import io
import subprocess
import logging
import os

class Ingestion:

    def __init__(self, filename,bucket,filepath):
        self.log = logging.getLogger(self.__class__.__name__)
        self.BucketName = 's3://' + bucket
        self.Ingestion_file_name = filename
        self.Ingestion_file_path = filepath

    def uploadtos3bucket(self):
        self.log.info(f"BucketName {self.BucketName} Ingestion FileName {self.Ingestion_file_name} Ingestion FilePath {self.Ingestion_file_path}")
        cmd='aws '+'s3 '+'cp '+self.Ingestion_file_path+" "+self.BucketName+" --metadata utility_file_name="+self.Ingestion_file_name
        try:
            os.system(cmd)
        except Exception as e:
            raise RuntimeError(f"command '{cmd}' return with error {e}")
