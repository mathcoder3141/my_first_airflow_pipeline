from dotenv import load_dotenv
import requests
import json
from datetime import datetime
import os

load_dotenv()

API_KEY = os.getenv('WEATHER_API_KEY')

def get_weather():
    parameters = {'q' : 'Dallas', 'appid':API_KEY}
    result = requests.get("http://api.openweathermap.org/data/2.5/weather?", params=parameters)

    if result.status_code == 200:
        json_data = result.json()
        file_name = str(datetime.now().date()) + '.json'
        tot_name = os.path.join(os.path.dirname(__file__), 'data', file_name)

        with open(file_name, 'w') as outputfile:
            json.dump(json_data, outputfile)

    else:
        print("Error in API call")

if __name__ == "__main__":
    get_weather()
