import pandas as pd
import requests

url = "https://v2.jokeapi.dev/joke/Dark"
response = requests.get(url)
data = response.json()
df = pd.json_normalize(data)
print(df)