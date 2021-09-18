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

def test_combine_list():
    date = {"date_range_start": "2021-07-10", "date_range_end": "2021-08-01"}
    core = sgql_client.lookup(placekeys=placekeys, product="core", columns=["placekey", "brands", "naics_code"], return_type="list", date=date)
    sgql_client.save(path="core.csv")
    geometry = sgql_client.lookup(placekeys=placekeys, product="geometry", columns=["placekey", "location_name", "street_address", "city", "enclosed"], 
        return_type="list", date=date)
    sgql_client.save(path="geometry.csv")
    #monthly_patterns = sgql_client.lookup(placekeys=placekeys, product="monthly_patterns", columns=["placekey", "date_range_start", "date_range_end","raw_visit_counts", "raw_visitor_counts"], return_type="pandas")
    monthly_patterns = sgql_client.lookup(placekeys=placekeys, product="monthly_patterns", columns=["placekey", "date_range_start", "date_range_end","raw_visit_counts", "raw_visitor_counts"], 
        return_type="list", date=date)
    sgql_client.save(path="monthly_patterns.csv")
    arr_ = [ core, geometry, monthly_patterns ]
    inner_df = sgql_client.sg_merge(arr_, how="inner")
    sgql_client.save(path="inner_df.csv")
    outer_df = sgql_client.sg_merge(arr_, how="outer")
    sgql_client.save(path="outer_df.csv")
    columns = []
    for i in arr_:
        for j in i:
            columns += [k for k in j if k not in columns]
    assert len(columns) == len([i for i in inner_df[0]])
    assert len(columns) == len([i for i in outer_df[0]])
    # weekly pattterns error case
    weekly_patterns = sgql_client.lookup(placekeys=placekeys, product="weekly_patterns", columns=["placekey", "brands", "visitor_home_cbgs", "distance_from_home", "related_same_day_brand"], return_type="list", date=date)
    arr_ = [ core, geometry, monthly_patterns, weekly_patterns ]
    try:
        inner_df = sgql_client.sg_merge(arr_, how="inner")
        # outer_df = sgql_client.sg_merge(arr_, how="outer")
    except Exception as e:
        assert type(e) == TypeError

def test_combine_pandas():
    date = ["2021-07-10", "2021-08-01", "2020-05-11"]
    core = sgql_client.lookup(placekeys=placekeys, product="core", columns=["placekey", "brands", "naics_code"], 
        return_type="pandas", date=date)
    sgql_client.save(path="core.csv")
    geometry = sgql_client.lookup(placekeys=placekeys, product="geometry", columns=["placekey", "location_name", "street_address", "city", "enclosed"], 
        return_type="pandas", date=date)
    sgql_client.save(path="geometry.csv")
    #monthly_patterns = sgql_client.lookup(placekeys=placekeys, product="monthly_patterns", columns=["placekey", "date_range_start", "date_range_end","raw_visit_counts", "raw_visitor_counts"], return_type="pandas")
    monthly_patterns = sgql_client.lookup(placekeys=placekeys, product="monthly_patterns", columns=["placekey", "date_range_start", "date_range_end","raw_visit_counts", "raw_visitor_counts"], 
        return_type="pandas", date=date)
    sgql_client.save(path="monthly_patterns.csv")
    arr_ = [ core, geometry, monthly_patterns ]
    inner_df = sgql_client.sg_merge(arr_, how="inner")
    sgql_client.save(path="inner_df.csv")
    outer_df = sgql_client.sg_merge(arr_, how="outer")
    sgql_client.save(path="outer_df.csv")
    columns = []
    for i in arr_:
        columns += [i for i in i.columns if i not in columns]
    assert len(columns) == inner_df.shape[1]
    assert len(columns) == outer_df.shape[1]
    # weekly pattterns error case
    weekly_patterns = sgql_client.lookup(placekeys=placekeys, product="weekly_patterns", columns=["placekey", "brands", "visitor_home_cbgs", "distance_from_home", "related_same_day_brand"], 
        return_type="pandas", date=date)
    arr_ = [ core, geometry, monthly_patterns, weekly_patterns ]
    try:
        inner_df = sgql_client.sg_merge(arr_, how="inner")
        # outer_df = sgql_client.sg_merge(arr_, how="outer")
    except Exception as e:
        assert type(e) == TypeError

def test_get_place_by_locatian_name_address():
    date = ["2018-06-05", "2019-06-05", "2020-06-05", "2021-06-05"]
    # safegraph_core
    assert type(sgql_client.lookup_by_name(date=date, 
        product="core",
        location_name= "Taco Bell", 
        street_address= "710 3rd St", 
        city= "San Francisco", 
        region= "CA", 
        iso_country_code= "US",
        return_type="pandas",
        columns="*")) == df_type
    # safegraph_geometry
    assert type(sgql_client.lookup_by_name(date=date, 
        product="geometry",
        location_name= "Taco Bell", 
        street_address= "710 3rd St", 
        city= "San Francisco", 
        region= "CA", 
        iso_country_code= "US",
        return_type="list",
        columns="*")) == list
    # safegraph_weekly_patterns
    weekly = sgql_client.lookup_by_name(date=date, product="weekly_patterns", location_name= "Taco Bell", street_address= "710 3rd St", city= "San Francisco", region= "CA", iso_country_code= "US", return_type="pandas", columns=["related_same_week_brand", "related_same_day_brand", "bucketed_dwell_times"])
    type(weekly) == df_type

def test_search(): 
    date = ["2021-05-05", "2021-06-05", "2021-04-05"]
    # safegraph_core
    assert type(sgql_client.search( product="core", columns="*", brand = "starbucks", brand_id = None, naics_code = None, phone_number = None, street_address = None, city = None, region = None, postal_code = None, iso_country_code = None, 
        max_results=70, after_result_number=10, return_type="pandas", date=date)) == df_type
    # safegraph_geometry
    assert type(sgql_client.search( product="geometry", brand = "starbucks", columns=["location_name", "street_address", "city", "enclosed", "latitude", "iso_country_code"], brand_id = None, naics_code = None, phone_number = None, street_address = None, city = None, region = None, postal_code = None, iso_country_code = None, 
        max_results=55, after_result_number=15,  return_type="list", date=date)) == list
    # safegraph_weekly_patterns
    naics_code = "445120"
    assert type(sgql_client.search(product="weekly_patterns", columns =["median_dwell", "city", "brands", "visitor_home_cbgs", "distance_from_home", "related_same_day_brand"], naics_code = naics_code, date=date)) == df_type

    # safegraph_monthly_patterns
    try:
        city = 'fsahfsadhfsadkjfsadjf'
        sgql_client.search(product="monthly_patterns", columns = "*", city = city, date=date)
    except Exception as e:
        assert type(e) == client.safeGraphError

def test_lookup():  
    date = {"date_range_start": "2021-07-10", "date_range_end": "2021-08-01"}
    # safegraph_core
    assert type(sgql_client.lookup(placekeys=placekeys, product="core", columns="*", return_type="pandas", date=date)) == df_type
    core = sgql_client.lookup(placekeys=placekeys, product="core", columns=["placekey", "brands", "naics_code"], return_type="pandas", date=date)
    assert type(core) == df_type
    # safegraph_geometry
    assert type(sgql_client.lookup(placekeys=placekeys, product="geometry", columns="*", return_type="pandas", date=date)) == df_type
    geometry = sgql_client.lookup(placekeys=placekeys, product="geometry", columns=["location_name", "street_address", "city", "enclosed"], return_type="pandas", date=date)
    assert type(geometry) == df_type
    # safegraph_weekly_patterns
    assert type(sgql_client.lookup(placekeys=placekeys, product="weekly_patterns", columns="*", return_type="pandas", date=date)) == df_type
    weekly_patterns = sgql_client.lookup(placekeys=placekeys, product="weekly_patterns", columns=["brands", "visitor_home_cbgs", "distance_from_home", "related_same_day_brand"], return_type="pandas", date=date)
    assert type(weekly_patterns) == df_type
    # safegraph_monthly_patterns
    assert type(sgql_client.lookup(placekeys=placekeys, product="monthly_patterns", columns="*", return_type="pandas", date=date)) == df_type
    monthly_patterns = sgql_client.lookup(placekeys=placekeys, product="monthly_patterns", columns=["date_range_start", "date_range_end","raw_visit_counts", "raw_visitor_counts"], return_type="pandas", date=date)
    assert type(monthly_patterns) == df_type

    try:
        sgql_client.lookup(placekeys=placekeys, 
        product="core",
        columns=
            ["fakes", "fake2"] + 
            random.sample(arr,random.randint(1, len(arr))), 
        return_type="pandas", date=date)
    except Exception as e:
        assert(type(e) == ValueError)

def test_non_weekly_date_req():
    # weekly monthly
    # safegraph_core                
    date = {"date_range_start": "2021-07-10", "date_range_end": "2021-08-01"}                                                                                                                                                                                                                                                   
    assert type(sgql_client.lookup(placekeys=placekeys, product="weekly_patterns", columns="*", return_type="list", date=date)) == list                                                                                                                                                      
    core = sgql_client.lookup(placekeys=placekeys, product="weekly_patterns", columns="*", return_type="pandas", date=date)                                                                                                                                     
    assert type(core) == df_type                              
    # monthly_pattern                                                                                                                                                                                                                         
    assert type(sgql_client.lookup(placekeys=placekeys, product="monthly_patterns", columns="*", return_type="list", date=date)) == list                                                                                                                                                      
    core = sgql_client.lookup(placekeys=placekeys, product="monthly_patterns", columns="*", return_type="pandas", date=date)                                                                                                                                     
    assert type(core) == df_type     

    # NON weekly monthly
    # safegraph_core
    assert type(sgql_client.lookup(placekeys=placekeys, product="core", columns="*", return_type="pandas", date=date)) == df_type
    core = sgql_client.lookup(placekeys=placekeys, product="core", columns=["placekey", "brands", "naics_code"], return_type="pandas", date=date)
    assert type(core) == df_type
    # safegraph_geometry
    assert type(sgql_client.lookup(placekeys=placekeys, product="geometry", columns="*", return_type="pandas", date=date)) == df_type
    geometry = sgql_client.lookup(placekeys=placekeys, product="geometry", columns=["location_name", "street_address", "city", "enclosed"], return_type="pandas", date=date)
    assert type(geometry) == df_type
    # safegraph_weekly_patterns
    assert type(sgql_client.lookup(placekeys=placekeys, product="weekly_patterns", columns="*", return_type="pandas", date=date)) == df_type
    weekly_patterns = sgql_client.lookup(placekeys=placekeys, product="weekly_patterns", columns=["brands", "visitor_home_cbgs", "distance_from_home", "related_same_day_brand"], return_type="pandas", date=date)
    assert type(weekly_patterns) == df_type
    # safegraph_monthly_patterns
    assert type(sgql_client.lookup(placekeys=placekeys, product="monthly_patterns", columns="*", return_type="pandas", date=date)) == df_type
    monthly_patterns = sgql_client.lookup(placekeys=placekeys, product="monthly_patterns", columns=["date_range_start", "date_range_end","raw_visit_counts", "raw_visitor_counts"], return_type="pandas", date=date)
    assert type(monthly_patterns) == df_type



# def test_safegraph_weekly_patterns():
#     sgql_client.date = ["2021-08-05", "2021-08-12", "2021-08-19"]
#     sgql_client.patterns_version = "weekly"
#     df = sgql_client.lookup(placekeys, product="core", columns=["safegraph_brand_ids", "date_range_start", "visits_by_day"], return_type="pandas") 
#     assert type(df) == df_type
#     df = sgql_client.lookup(placekeys, product="geometry", columns="safegraph_weekly_patterns.*", return_type="pandas")
#     assert type(df) == df_type
#     df = sgql_client.lookup(placekeys, product="weekly_patterns", columns="*", date="2021-01-01", patterns_version="weekly", return_type="pandas")
#     assert type(df) == df_type
#     columns = ['related_same_month_brand', 'visits_by_each_hour', "location_name", "longitude", "date_range_start"]
#     try:
#         df = sgql_client.lookup(placekeys, columns=columns, return_type="pandas")
#     except Exception as e:
#         assert type(e) == ValueError


# def test_search_pagination():
#     city = "Philadelphia"
#     brand = "Starbucks"

#     brand_search_initial = sgql_client.search(columns = ['placekey'], brand = brand, city = city, max_results = 10, after_result_number = 0)
#     brand_search_pagination1 = sgql_client.search(columns = ['placekey'], brand = brand, city = city, max_results = 25, after_result_number = 0)
#     brand_search_pagination2 = sgql_client.search(columns = ['placekey'], brand = brand, city = city, max_results = 25, after_result_number = 10)
#     assert (set(brand_search_initial.placekey) == set(brand_search_pagination1.placekey).difference(brand_search_pagination2.placekey))

# def test_overlap():
#     city = "Philadelphia"
#     brand = "Starbucks"

#     brand_search_initial = sgql_client.search(columns = ['placekey'], brand = brand, city = city, max_results = 10, after_result_number = 0)
#     brand_search_pagination1 = sgql_client.search(columns = ['placekey'], brand = brand, city = city, max_results = 10, after_result_number = 10)
#     for i in range(len(brand_search_initial)):
#         assert brand_search_initial.loc[i].values[0] != brand_search_pagination1.loc[i].values[0]

# def test_null_cases():
#     null_check = [
#         "placekey",
#         "location_name",
#         "top_category",
#         "sub_category",
#         "naics_code",
#         "latitude",
#         "longitude",
#         "street_address",
#         "city",
#         "region",
#         "postal_code",
#         "iso_country_code",
#         # "tracking_closed_since",
#         # "geometry_type",
#         # "polygon_wkt",
#         # "polygon_class",
#         # "is_synthetic",
#         # "enclosed",
#         # "date_range_start",
#         # "date_range_end",
#         # "raw_visit_counts",
#         # "raw_visitor_counts",
#         # "visits_by_day",
#         # "visitor_home_cbgs",
#         # "visitor_home_aggregation",
#         # "visitor_daytime_cbgs",
#         # "visitor_country_of_origin",
#         # "distance_from_home",
#         # "median_dwell",
#         # "bucketed_dwell_times",
#         # "related_same_day_brand",
#         # "related_same_month_brand",
#         # "popularity_by_hour",
#         # "popularity_by_day",
#         # "device_type",
#     ] 
#     df = sgql_client.lookup(placekeys, columns="*", return_type="pandas")
#     for i in null_check:
#         assert(df[i].isnull().values.any() == False)
#     for i in range(len(df)):
#         # Check that the naics_code column is a string in any Pandas dataframe results.
#         assert(type(df.loc[i]["naics_code"]) == str)

# def test_save():
#     # Read in the result of save() and make sure it matches the original dataframe.
#     df = sgql_client.lookup(placekeys, columns="*", return_type="pandas")
#     path = "results.csv"
#     sgql_client.save(path)
#     saved_df = pd.read_csv(path)
#     # if same shape means same values
#     assert(df.shape == saved_df.shape)
