import enum
from Properties_Fetching import Get_Specific_Details

class GenerateEnrolementValues():
    enrolement_json = Get_Specific_Details.GetSpecificDetails()
    obj_dic = {
        "url": 'https://nspuatapi.bidgely.com',
        "pilotid": '40003',
        "access_token": '56b02db5-b83c-4c5c-b75d-3b6eaee03438'
    }
    columnNames = enrolement_json.Get_Data_columns(obj_dic)

    def __init__(self, Env_Variables):

    enrolement_file_values = []
    enrolement_file_values.append(buildCustomerId())
    enrolement_file_values.append(buildAccountId())


    def buildCustomerId(self):
        self.customerId = "12344"
        return self.customerId

    def buildAccountId(self):
        self.accountId = "12344"
        return self.accountId

    def buildPremiseId(self):
        self.premiseId = "12344"
        return self.premiseId




