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

def test_safegraph_weekly_patterns():
    sgql_client.date = ["2021-08-05", "2021-08-12", "2021-08-19"]
    sgql_client.patterns_version = "weekly"
    df = sgql_client.batch_lookup(placekeys, columns=["safegraph_brand_ids", "date_range_start", "visits_by_day"], return_type="pandas") 
    assert type(df) == df_type
    df = sgql_client.batch_lookup(placekeys, columns="safegraph_weekly_patterns.*", return_type="pandas")
    assert type(df) == df_type
    df = sgql_client.batch_lookup(placekeys, columns="*", date="2021-01-01", patterns_version="weekly", return_type="pandas")
    assert type(df) == df_type
    columns = ['related_same_month_brand', 'visits_by_each_hour', "location_name", "longitude", "date_range_start"]
    try:
        df = sgql_client.batch_lookup(placekeys, columns=columns, return_type="pandas")
    except Exception as e:
        assert type(e) == ValueError


def test_search(): 
    # max_results = 150
    # # for loop len check
    # for i in reversed(range(1, max_results, 5)):
    #     test = sgql_client.search( brand = "starbucks", brand_id = None, naics_code = None, phone_number = None, street_address = None, city = None, region = None, postal_code = None, iso_country_code = None, 
    #     max_results=i, after_result_number=i, columns="safegraph_core.*", return_type="list")
    #     assert type(test) == list
    #     assert len(test) == i
    # assert type(sgql_client.search( brand = "starbucks", brand_id = None, naics_code = None, phone_number = None, street_address = None, city = None, region = None, postal_code = None, iso_country_code = None, 
    #     max_results=55, after_result_number=0, columns="safegraph_core.*", return_type="pandas")) == df_type
    # assert type(sgql_client.search( brand = "starbucks", brand_id = None, naics_code = None, phone_number = None, street_address = None, city = None, region = None, postal_code = None, iso_country_code = None, 
    #     max_results=55, after_result_number=5, columns="safegraph_core.*", return_type="pandas")) == df_type
    assert type(sgql_client.search( brand = "starbucks", brand_id = None, naics_code = None, phone_number = None, street_address = None, city = None, region = None, postal_code = None, iso_country_code = None, 
        max_results=70, after_result_number=10, columns="safegraph_core.*", return_type="pandas")) == df_type
    assert type(sgql_client.search( brand = "starbucks", brand_id = None, naics_code = None, phone_number = None, street_address = None, city = None, region = None, postal_code = None, iso_country_code = None, 
        max_results=55, after_result_number=15, columns="safegraph_core.*", return_type="list")) == list
    
    naics_code = "445120"
    assert type(sgql_client.search(columns = 'safegraph_core.*', naics_code = naics_code)) == df_type

    try:
        city = 'fsahfsadhfsadkjfsadjf'
        sgql_client.search(columns = ['safegraph_core.*'], city = city)
    except Exception as e:
        assert type(e) == client.safeGraphError

def test_search_pagination():
    city = "Philadelphia"
    brand = "Starbucks"

    brand_search_initial = sgql_client.search(columns = ['placekey'], brand = brand, city = city, max_results = 10, after_result_number = 0)
    brand_search_pagination1 = sgql_client.search(columns = ['placekey'], brand = brand, city = city, max_results = 25, after_result_number = 0)
    brand_search_pagination2 = sgql_client.search(columns = ['placekey'], brand = brand, city = city, max_results = 25, after_result_number = 10)
    assert (set(brand_search_initial.placekey) == set(brand_search_pagination1.placekey).difference(brand_search_pagination2.placekey))

def test_overlap():
    city = "Philadelphia"
    brand = "Starbucks"

    brand_search_initial = sgql_client.search(columns = ['placekey'], brand = brand, city = city, max_results = 10, after_result_number = 0)
    brand_search_pagination1 = sgql_client.search(columns = ['placekey'], brand = brand, city = city, max_results = 10, after_result_number = 10)
    for i in range(len(brand_search_initial)):
        assert brand_search_initial.loc[i].values[0] != brand_search_pagination1.loc[i].values[0]

def test_get_place_by_locatian_name_address():
    assert type(sgql_client.lookup_by_name(
        location_name= "Taco Bell", 
        street_address= "710 3rd St", 
        city= "San Francisco", 
        region= "CA", 
        iso_country_code= "US",
        return_type="pandas",
        columns="*")) == df_type
    assert type(sgql_client.lookup_by_name(
        location_name= "Taco Bell", 
        street_address= "710 3rd St", 
        city= "San Francisco", 
        region= "CA", 
        iso_country_code= "US",
        return_type="list",
        columns="*")) == list

def test_batch_lookup():  
    __dataset = ["safegraph_core.*", "safegraph_geometry.*", "safegraph_monthly_patterns.*"] # "safegraph_weekly_patterns.*"] # for dataset column functionality
    #import pdb;pdb.set_trace()
    assert type(sgql_client.batch_lookup(placekeys, columns="*", return_type="pandas")) == df_type
    assert type(sgql_client.batch_lookup(placekeys, columns=[__dataset[0]], return_type="pandas")) == df_type
    argv_ = []
    for i in range(random.randint(1, len(__dataset))):
        inside = random.choice(__dataset)
        if inside not in argv_:
            argv_.append(inside)
    assert type(sgql_client.batch_lookup(placekeys, columns=argv_, return_type="pandas")) == df_type
    argv_ = []
    for i in range(random.randint(1, len(__dataset))):
        inside = random.choice(__dataset)
        if inside not in argv_:
            argv_.append(inside)
    assert type(sgql_client.batch_lookup(placekeys, columns=argv_, return_type="pandas")) == df_type

    try:
        sgql_client.batch_lookup(placekeys, 
        columns=
            ["fakes", "fake2"] + 
            random.sample(arr,random.randint(1, len(arr))), 
        return_type="pandas")
    except Exception as e:
        assert(type(e) == ValueError)


    argv_ = []
    for i in range(random.randint(1, len(arr))):
        inside = random.choice(arr)
        if inside not in argv_:
            argv_.append(inside)
    try:
        assert type(sgql_client.batch_lookup(placekeys, columns=argv_, return_type="list")) == list
    except Exception as e:
        assert type(e) == ValueError
    argv_ = []
    for i in range(random.randint(1, len(arr))):
        inside = random.choice(arr)
        if inside not in argv_:
            argv_.append(inside)
    try:
        assert type(sgql_client.batch_lookup(placekeys, columns=argv_, return_type="pandas")) == df_type
    except Exception as e:
        assert type(e) == ValueError

    # assert type(sgql_client.batch_lookup(placekeys, columns=random.sample(arr,random.randint(1, len(arr))), return_type="list")) == list
    # assert type(sgql_client.batch_lookup(placekeys, columns=random.sample(arr,random.randint(1, len(arr))), return_type="pandas")) == df_type
    # assert type(sgql_client.batch_lookup(placekeys, columns=random.sample(arr,random.randint(1, len(arr))), return_type="list")) == list
    # assert type(sgql_client.batch_lookup(placekeys, columns=random.sample(arr,random.randint(1, len(arr))), return_type="pandas")) == df_type
    # assert type(sgql_client.batch_lookup(placekeys, columns=random.sample(arr,random.randint(1, len(arr))), return_type="list")) == list

def test_null_cases():
    null_check = [
        "placekey",
        "location_name",
        "top_category",
        "sub_category",
        "naics_code",
        "latitude",
        "longitude",
        "street_address",
        "city",
        "region",
        "postal_code",
        "iso_country_code",
        # "tracking_closed_since",
        # "geometry_type",
        # "polygon_wkt",
        # "polygon_class",
        # "is_synthetic",
        # "enclosed",
        # "date_range_start",
        # "date_range_end",
        # "raw_visit_counts",
        # "raw_visitor_counts",
        # "visits_by_day",
        # "visitor_home_cbgs",
        # "visitor_home_aggregation",
        # "visitor_daytime_cbgs",
        # "visitor_country_of_origin",
        # "distance_from_home",
        # "median_dwell",
        # "bucketed_dwell_times",
        # "related_same_day_brand",
        # "related_same_month_brand",
        # "popularity_by_hour",
        # "popularity_by_day",
        # "device_type",
    ] 
    df = sgql_client.batch_lookup(placekeys, columns="*", return_type="pandas")
    for i in null_check:
        assert(df[i].isnull().values.any() == False)
    for i in range(len(df)):
        # Check that the naics_code column is a string in any Pandas dataframe results.
        assert(type(df.loc[i]["naics_code"]) == str)

def test_save():
    # Read in the result of save() and make sure it matches the original dataframe.
    df = sgql_client.batch_lookup(placekeys, columns="*", return_type="pandas")
    path = "results.csv"
    sgql_client.save(path)
    saved_df = pd.read_csv(path)
    # if same shape means same values
    assert(df.shape == saved_df.shape)
