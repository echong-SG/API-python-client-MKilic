from safegraphql import client
import pandas as pd
import random
from safegraphql.types import __PATTERNS__
import pprint
printy =  pprint.PrettyPrinter(indent=4).pprint

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


# misc. columns

def test_EUGENE_case():
    sgql_client.date = {"date_range_start": "2021-07-10", "date_range_end": "2021-08-01"}
    sgql_client.patterns_version = "weekly"
    cols = [
        'popularity_by_day',
        'related_same_month_brand',
        "latitude",
        # 'related_same_week_brand',
    ]
    cols = ["location_name"]
    sgql_client.lookup(
        'zzw-222@8fy-fjg-b8v', 
        columns = cols
    )
    sgql_client.save()
    input(f'''lookup: columns=[{cols}], Eugene test case, saved to csv''')


def test_lookup_by_name_each_case():
    sgql_client.date = ["2021-08-05", "2021-08-12", "2021-08-19"]
    cols = ["safegraph_brand_ids", "date_range_start", "date_range_end", "visits_by_day", "postal_code"]
    sgql_client.patterns_version = "weekly"
    # location_name + street_address + city + region + iso_country_code 
    df = sgql_client.lookup_by_name(
        location_name="Taco Bell", 
        street_address= "710 3rd St", 
        city= "San Francisco", 
        region="CA", 
        iso_country_code="US", columns=cols, return_type="pandas")
    print(df["postal_code"]) 
    input("""lookup_by_name: [location_name + street_address + city + region + iso_country_code] return_type="pandas")""")
    # location_name + street_address + postal_code + iso_country_code                                                     
    df = sgql_client.lookup_by_name(
        location_name="Taco Bell", 
        street_address= "710 3rd St", 
        postal_code= "94107", 
        region="CA", 
        iso_country_code="US", columns=cols + ["latitude", "longitude"], return_type="pandas")       
    print(df["latitude"], df["longitude"]) 
    input("""lookup_by_name: [location_name + street_address + postal_code + iso_country_code] return_type="pandas")""")
    # location_name + latitude + longitude + iso_country_code  
    df = sgql_client.lookup_by_name(
        location_name="Taco Bell", 
        latitude=37.778599,
        longitude=-122.39276,
        iso_country_code="US", columns=cols, return_type="pandas")       
    print(df["postal_code"]) 
    input("""lookup_by_name: [location_name + street_address + postal_code + iso_country_code] return_type="pandas")""")

test_lookup_by_name_each_case()                
# df = sgql_client.lookup(placekeys, columns=["safegraph_brand_ids", "date_range_start", "date_range_end", "visits_by_day"], return_type="pandas") 
# sgql_client.save()
# printy(df)
# input('''lookup: columns=["safegraph_brand_ids", "date_range_start", "date_range_end", "visits_by_day"] saved to results.csv, next dataframe?''') 
# df = sgql_client.lookup(placekeys, columns="safegraph_weekly_patterns.*", return_type="pandas")
# sgql_client.save()
# printy(df)
# input('''lookup: columns="safegraph_weekly_patterns.*" saved to results.csv, next dataframe?''')
df = sgql_client.search( brand = "starbucks", brand_id = None, naics_code = None, phone_number = None, street_address = None, city = None, region = None, postal_code = None, iso_country_code = None, 
        max_results=70, after_result_number=10, columns=["safegraph_core.*", "safegraph_weekly_patterns.*"], date=["2021-01-01", "2021-02-01"], patterns_version="weekly", return_type="pandas")
printy(df)
sgql_client.save()
input('''search: columns=["safegraph_core.*", "safegraph_weekly_patterns.*"], patterns_version="weekly", date="2021-01-01" case, saved to csv''')
df = sgql_client.lookup_by_name(
        location_name= "Taco Bell", 
        street_address= "710 3rd St", 
        city= "San Francisco", 
        region= "CA", 
        iso_country_code= "US",
        return_type="pandas",
        columns="*")
printy(df)
sgql_client.save()
input('''lookup_by_name: case saved to results.csv, next dataframe? next case is error case''')
df = sgql_client.lookup(placekeys, columns="*", date="2021-01-01", patterns_version="weekly", return_type="pandas")
sgql_client.save()
printy(df)
input('''manual case [columns="*", date="2021-01-01", patterns_version="weekly"] saved to results.csv, next dataframe? next case is error case''')
columns = ['related_same_month_brand', 'visits_by_each_hour', "location_name", "longitude", "date_range_start"]
try:
    df = sgql_client.lookup(placekeys, columns=columns, return_type="pandas")
    sgql_client.save()
except:
    pass
input('''columns=['related_same_month_brand', 'visits_by_each_hour', "location_name", "longitude", "date_range_start"] gave error, finished, go one more time to see the error''')
df = sgql_client.lookup(placekeys, columns=columns, return_type="pandas")