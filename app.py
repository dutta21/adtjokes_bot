import pandas as pd
import requests
import time

url = "https://v2.jokeapi.dev/joke/Dark"

output_file = "check.xlsx"
dfx=pd.DataFrame()
for i in range(6):
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data)
        dfx=pd.concat([df,dfx],ignore_index=True)
    else:
        print("Error:",response.status_code)
    
    time.sleep(10)

dfx.to_excel(output_file,header=True,index=False)
