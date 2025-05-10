import requests
from .IProducer import IProducer
import os
import json
from dotenv import load_dotenv

load_dotenv()

SPORTS_RADAR_API_KEYS = json.loads(os.environ['SPORTS_RADAR_API_KEYS'])

class LiveMatchProducer(IProducer):
    def __init__(self, sport_event):
        super().__init__()
        self.__sport_event = sport_event
    
    def _pub(self, val):
        super()._pub_with_topic(val, topic= "live-match")
        
    def main(self):
        idx = 0
        N = len(SPORTS_RADAR_API_KEYS)
        while (idx<N) :
            api_key_to_use = SPORTS_RADAR_API_KEYS[idx]
            headers = {
                'x-api-key': api_key_to_use  # Replace with your actual key
            }

            r = requests.get("https://api.sportradar.com/soccer-extended/trial/v4/stream/statistics/subscribe",
                params = {'format': 'json', 'sport_event_id': self.__sport_event},
                headers= headers,
                allow_redirects=False)

            if(r.status_code != 302): ## most likely the api key is used up for the day so we try using a new one
                idx+=1
                continue

            redirect_url = r.headers['Location']
            r = requests.get(redirect_url, stream=True)

            if(r.status_code != 200): ## most likely the api key is used up for the day so we try using a new one
                idx+=1
                continue

            for line in r.iter_lines():
                # filter out keep-alive new lines
                if line:
                    decoded_line = line.decode('utf-8')
                    self._pub(decoded_line)
