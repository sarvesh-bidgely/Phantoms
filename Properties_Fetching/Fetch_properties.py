import logging

from Properties_Fetching import Fetch_Api_Response
from Properties_Fetching import Property_Types_Enum

class FetchProperties:
    def __init__(self,url,pilotid,access_token,property_type,uuid="",):
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.info(f"Inside the Constructor of FetchProperties")
        self.log.info(f"The parameters are url={url}, pilotid={pilotid}, access_token={access_token},uuid={uuid},property_type={property_type}")
        self.url=url
        self.pilotid=pilotid
        self.access_token=access_token
        self.property_type=property_type
        self.uuid=uuid

    def GetUrls(self):
        switcher = {
            Property_Types_Enum.PropertyTypes.UserDetails: self.url+"/v2.0/users/"+self.uuid,
            Property_Types_Enum.PropertyTypes.PilotConfigs: self.url+"/entities/pilot/"+self.pilotid+"/configs"
        }
        self.log.info(f"In the Function GetUrls and URl = {switcher.get(self.property_type,None)}")
        return switcher.get(self.property_type,None)

    def GetParams(self):
        switcher = {
            Property_Types_Enum.PropertyTypes.UserDetails: {"access_token":self.access_token},
            Property_Types_Enum.PropertyTypes.PilotConfigs: {"access_token":self.access_token}
        }
        self.log.info(f"In the Function GetParams and Params = {switcher.get(self.property_type,None)}")
        return switcher.get(self.property_type,None)

    def GetProperties(self):
        prop=Fetch_Api_Response.FetchApiResponse(self.GetUrls(), self.GetParams())
        res=prop.GetApiResponse()
        # print(res)
        # print(type(res))
        return res

# if __name__ == '__main__':
#     f=FetchProperties("http://naapi2.bidgely.com",str(10064),"595b8e91-3753-4826-9a46-bbd9d74b3f65",Property_Types_Enum.PropertyTypes.UserDetails,"6f587119-2859-42dd-af9d-9f361d5d9411")
#     f.GetProperties()



