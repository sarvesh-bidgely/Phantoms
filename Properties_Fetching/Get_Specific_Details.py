import logging
import json

from Properties_Fetching import Property_Types_Enum
from Properties_Fetching import Fetch_properties
from Generate_Ingestion_Files.IngestionFile_and_delimiterconfig_mapping import IngestionFileDelimiterConfigMapping
from Generate_Ingestion_Files.Ingestion_File_Type import IngestionFileType

class GetSpecificDetails:
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.info("Into the func GetSpecificDetails")

    def Get_Pilot_Configs(self,Env_Variables):
        self.log.info(f"Into the Function Pilot_Configs for Env_Variables={Env_Variables}")
        try:
            url=Env_Variables["url"]
            pilotid=Env_Variables["pilotid"]
            access_token=Env_Variables["access_token"]
            property_type=Property_Types_Enum.PropertyTypes.PilotConfigs
            uuid = Env_Variables.get("uuid", "")
            self.fetch_prop=Fetch_properties.FetchProperties(url, pilotid, access_token, property_type, uuid)
        except KeyError:
            raise KeyError(f"The keys passed in the dict {Env_Variables} is missing the expeceted keys")

        #Getting the configs of the pilot
        pilot_configs=self.fetch_prop.GetProperties()
        return pilot_configs

    def Get_User_details(self,Env_Variables):
        self.log.info(f"Into the Function Get_User_details for Env_Variables={Env_Variables}")
        try:
            url=Env_Variables["url"]
            pilotid=Env_Variables["pilotid"]
            access_token=Env_Variables["access_token"]
            property_type=Property_Types_Enum.PropertyTypes.UserDetails
            uuid = Env_Variables["uuid"]
            self.fetch_prop=Fetch_properties.FetchProperties(url, pilotid, access_token, property_type, uuid)
        except KeyError:
            raise KeyError(f"The keys passed in the dict {Env_Variables} is missing the expeceted keys")

        user_details=self.fetch_prop.GetProperties().get("payload", None)
        return(user_details)

    def Get_Data_columns(self,Env_Variables):
        self.log.info(f"Into the Function Get_Data_columns for Env_Variables={Env_Variables}")
        #Returning Associative Array
        result={}

        pilot_configs = self.Get_Pilot_Configs(Env_Variables)
        pilot_configs = pilot_configs.get("user_creation_launchpad")
        pilot_configs = json.loads(pilot_configs).get("kvs")
        for item in pilot_configs:
            k = item.get("key")
            temp = json.loads(item.get("val"))
            v = temp.get("fieldPosition")
            if v==None:
                self.log.error(f"The column {k} doesnt have the Field position for the Env_Variables={Env_Variables}")
            else:
                result[k]=v

        result=sorted(result, key=result.get)
        self.log.info(f"The o/p from the function Get_Data_columns of columns = {result}")
        return(result)

    def Get_Delimiter(self,Env_Variables,File_Creation_type):
        self.log.info(f"Into the Function Get_Delimiter for Env_Variables={Env_Variables} and File_Creation_type={File_Creation_type}")
        pilot_configs=self.Get_Pilot_Configs(Env_Variables)

        IfiledelMap=IngestionFileDelimiterConfigMapping(File_Creation_type)
        delimiter_config=IfiledelMap.GetFileMappingConfig()
        self.log.info(f"The file type = {File_Creation_type} and config={delimiter_config}")
        pilot_configs = pilot_configs.get("launchpad_ingestion_configs")
        pilot_configs = json.loads(pilot_configs).get("kvs")

        for item in pilot_configs:
            if item.get("key").casefold()==delimiter_config.casefold():
                delimiter_config_val=str(item.get("val")).replace("\\","")
                self.log.info(f"The Delimiter={delimiter_config_val}")
                return delimiter_config_val

    def Get_S3BucketName(self,Env_Variables):
        self.log.info(f"Into the Function S3BucketName for Env_Variables={Env_Variables}")
        pilot_configs=self.Get_Pilot_Configs(Env_Variables)

        pilot_configs = pilot_configs.get("data_ingestion")
        #print(pilot_configs)
        pilot_configs = json.loads(pilot_configs).get("kvs")

        for item in pilot_configs:
            #print (item)
            if item.get("key").casefold()=="s3BucketName".casefold():
                s3BucketName=str(item.get("val"))
                self.log.info(f"The s3BucketName={s3BucketName}")
                return s3BucketName

# if __name__ == '__main__':
#     d={"url":"http://aurorauatapi.bidgely.com","pilotid":str(10058),"access_token":"56b02db5-b83c-4c5c-b75d-3b6eaee03438","uuid":"7c2b7a79-ec13-497b-9c4d-a3c5fe302264"}
#     f=GetSpecificDetails()
#     print (f.Get_Data_columns(d))
