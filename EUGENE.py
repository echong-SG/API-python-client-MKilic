from safegraphql import client
import pandas as pd
import random
from safegraphql.types import __PATTERNS__

df_type = type(pd.DataFrame())
try:
    apiKey = open("apiKey.txt").readlines()[0]
except Exception as e:
    print("create file apiKey.txt and put your api key from https://shop.safegraph.com/api inside")
    raise e
    

sgql_client = client.HTTP_Client(apiKey)


arr = []
banned_keys = ["__header__", "__footer__"]
keys = [i for i in __PATTERNS__.keys()] 
for i in keys:
    arr += [i for i in __PATTERNS__[i] if i not in banned_keys]

placekeys = [
        "224-222@5vg-7gv-d7q", # "222-222@5qw-shj-7qz", "222-222@5s6-pyc-7qz", "zzy-222@5xc-k8q-zmk",
        "zzw-222@8fy-fjg-b8v", # (Disney World)
        "zzy-227@5sb-8cw-pjv", # (O'Hare Airport)
        "222-223@65y-rxx-djv", # (Walmart in Albany, NY)
        ] 

#sgql_client.date = {"date_range_start": "2021-01-05", "date_range_end": "2021-08-01"}
sgql_client.date = ["2021-08-05", "2021-08-12", "2021-08-19"]
sgql_client.patterns_version = "weekly"
df = sgql_client.batch_lookup(placekeys, columns=["safegraph_brand_ids", "date_range_start", "date_range_end", "visits_by_day"], return_type="pandas") 
sgql_client.save()
input('''columns=["safegraph_brand_ids", "date_range_start", "date_range_end", "visits_by_day"] saved to results.csv, next dataframe?''') 
df = sgql_client.batch_lookup(placekeys, columns="safegraph_weekly_patterns.*", return_type="pandas")
sgql_client.save()
input('''columns="safegraph_weekly_patterns.*" saved to results.csv, next dataframe?''')
df = sgql_client.batch_lookup(placekeys, columns="*", date="2021-01-01", patterns_version="weekly", return_type="pandas")
sgql_client.save()
input('''manual case [columns="*", date="2021-01-01", patterns_version="weekly"] saved to results.csv, next dataframe? next case is error case''')
columns = ['related_same_month_brand', 'visits_by_each_hour', "location_name", "longitude", "date_range_start"]
try:
    df = sgql_client.batch_lookup(placekeys, columns=columns, return_type="pandas")
    sgql_client.save()
except:
    pass
input('''columns=['related_same_month_brand', 'visits_by_each_hour', "location_name", "longitude", "date_range_start"] gave error, finished, go one more time to see the error''')
df = sgql_client.batch_lookup(placekeys, columns=columns, return_type="pandas")