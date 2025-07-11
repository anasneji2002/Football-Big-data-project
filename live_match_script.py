import requests

api_key_to_use = "Fo8S4qJArRr7vRdFRbeTnuVzWERgEfl16xfLuZij"
headers = {
    'x-api-key': api_key_to_use  # Replace with your actual key
}

r = requests.get("https://api.sportradar.com/soccer-extended/trial/v4/stream/statistics/subscribe",
    params = {'format': 'json', 'sport_event_id': "sr:sport_event:55809543"},
    headers= headers,
    allow_redirects=False)

if(r.status_code != 302): ## most likely the api key is used up for the day so we try using a new one
    print(f"Error:{r.json()}")

redirect_url = r.headers['Location']
r = requests.get(redirect_url, stream=True)

if(r.status_code != 200): ## most likely the api key is used up for the day so we try using a new one
    print(f"Error:{r.json()}")

for line in r.iter_lines():
    # filter out keep-alive new lines
    if line:
        print(line)