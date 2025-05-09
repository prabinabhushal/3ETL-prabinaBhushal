
# Extract

import ijson
import pandas as pd

data = []
rate = []


with open('IgnoreFolder/MagnaCarePPO_In-Network.json', 'rb') as f:
    for item in ijson.items(f, 'in_network.item'):
        data.append(item)

with open('IgnoreFolder/MagnaCarePPO_In-Network.json', 'rb') as f1:
    for item1 in ijson.items(f1, 'provider_references.item'):
        rate.append(item1)

print(type(data))
print(type(rate))

df1=pd.DataFrame(data)
print(df1)

df2=pd.DataFrame(rate)
print(df2)



df1.to_json('IgnoreFolder/df1.json',orient='records',indent =4)
df2.to_json('IgnoreFolder/df2.json',orient='records',indent =4)


# To make parquetFile

df1.to_parquet('IgnoreFolder/df1.parquet')
df2.to_parquet('IgnoreFolder/df2.parquet')

