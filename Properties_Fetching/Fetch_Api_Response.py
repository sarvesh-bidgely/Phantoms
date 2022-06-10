import logging
import requests

logging.basicConfig(filename="Logs.log",
                    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
                    filemode='w',level=0)

# Creating an object of the logging
logger = logging.getLogger()

class FetchApiResponse:
    def __init__(self, url, params):
        self.log = logging.getLogger(self.__class__.__name__)
        self.log.info(f"Inside the Constructor Function of FetchApiResponse and Args are url={url}, params={params}")
        self.link=url
        self.params=params

    def GetApiResponse(self):
        # sending get request and saving the response as response object
        r = requests.get(url=self.link, params=self.params)
        self.log.info(f"Fetching the api Response of {r.url}")
        #print(r.url, r.json())
        return r.json()
