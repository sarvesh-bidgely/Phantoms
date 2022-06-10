import logging
import json

from Properties_Fetching import Property_Types_Enum
from Fetch_properties import FetchProperties

class GetSpecificDetails:
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.info("Into the func GetSpecificDetails")

    def Get_Data_columns(self,Env_Variables):

        #Returning Associative Array
        result={}

        try:
            url=Env_Variables["url"]
            pilotid=Env_Variables["pilotid"]
            access_token=Env_Variables["access_token"]
            property_type=Property_Types_Enum.PropertyTypes.PilotConfigs
            uuid = Env_Variables.get("uuid", "")
            self.fetch_prop=FetchProperties(url, pilotid, access_token, property_type, uuid)
        except KeyError:
            raise KeyError(f"The keys passed in the dict {Env_Variables} is missing the expeceted keys")

        #Getting the configs of the pilot
        pilot_configs=self.fetch_prop.GetProperties()
        #print(pilot_configs)

        pilot_configs = pilot_configs.get("user_creation_launchpad")
        pilot_configs = json.loads(pilot_configs).get("kvs")
        for item in pilot_configs:
            k = item.get("key")
            temp = json.loads(item.get("val"))
            v = temp.get("fieldPosition")
            result[k]=v

        result=sorted(result, key=result.get)
        self.log.info(f"The o/p from the function Get_Data_columns of columns = {result}")
        return(result)

# if __name__ == '__main__':
#     d={"url":"http://naapi2.bidgely.com","pilotid":str(40002),"access_token":"595b8e91-3753-4826-9a46-bbd9d74b3f65"}
#     f=GetSpecificDetails()
#     print (f.Get_Data_columns(d))
