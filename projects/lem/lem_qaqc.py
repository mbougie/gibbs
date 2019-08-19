import json
import pandas as pd
from pandas.io.json import json_normalize

indata='I:\\d_drive\\projects\\lem\\data\\deliverables\\version\\v3\\v3_2\\county.json'


with open(indata) as data_file:    
    d= json.load(data_file)  

df = json_normalize(d)
print (df["features"][0]["properties"])