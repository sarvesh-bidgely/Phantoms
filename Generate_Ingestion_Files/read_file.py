from os import listdir
from os.path import isfile, join
import json
import sys
import uuid
import string
import random

row_string = ""
path_to_read_files = '/Users/sarveshkulkarni/Sites/nsp_res_keys/USERENROLL_D_202205272120_01.txt'
path_to_write_files = '/Users/sarveshkulkarni/Sites/nsp_res_keys/USERENROLL_D_202205272120_01.txt'

enrollment_file = open(path_to_read_files, 'r')
row_data = [enrollment_file.readline()]
sorted_data = [item.replace("|", ",") for item in row_data]


sorted_data_list = sorted_data[0].split(",")[:-1]

def buildCustomerId(sorted_data_list,position):
    customerId = str(uuid.uuid1())
    sorted_data_list[position] = customerId[0:8]

def buildAccountId(sorted_data_list,position):
    accountId = str(uuid.uuid1())
    sorted_data_list[position] = accountId[0:8]

def buildPremiseId(sorted_data_list,position):
    premiseId = str(uuid.uuid1())
    sorted_data_list[position] = premiseId[0:5]

def buildEmail(sorted_data_list,position):
    sorted_data_list[position] = random_char(7)+"@gmail.com"

def random_char(char_num):
    return ''.join(random.choice(string.ascii_letters) for _ in range(char_num))


buildCustomerId(sorted_data_list,0)
buildAccountId(sorted_data_list,1)
buildPremiseId(sorted_data_list,2)
buildEmail(sorted_data_list,4)

for element in sorted_data_list:
    row_string = row_string + element + '|'

print(row_string)









