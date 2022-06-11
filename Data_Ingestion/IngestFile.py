from os import listdir
from os.path import isfile, join
import json
import requests
import sys
import io
import subprocess
import os

class Ingestion:

    def __init__(self, filename,bucket):
        self.BucketName = 's3://' + bucket
        self.Ingestion_file_name = filename

    def uploadtos3bucket(self):
        print('file_path', self.Ingestion_file_name)
        try:
            subprocess.call(['aws', 's3', 'cp', self.Ingestion_file_name, self.BucketName])
        except subprocess.CalledProcessError as e:
            raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
