from safegraph_ql import client
import pandas as pd
import random

df_type = type(pd.DataFrame())
apiKey = open("apiKey.txt").readlines()[0]
safe_graph = client.HTTP_Client(apiKey)

arr = []
banned_keys = ["__header__", "__footer__"]
keys = [i for i in safe_graph.__pattern__.keys()] 
for i in keys:
    arr += [i for i in safe_graph.__pattern__[i] if i not in banned_keys]

placekeys = [
        "224-222@5vg-7gv-d7q", # "222-222@5qw-shj-7qz", "222-222@5s6-pyc-7qz", "zzy-222@5xc-k8q-zmk",
        "zzw-222@8fy-fjg-b8v", # (Disney World)
        "zzy-227@5sb-8cw-pjv", # (O'Hare Airport)
        "222-223@65y-rxx-djv", # (Walmart in Albany, NY)
        ] 

def test_places():  
    assert type(safe_graph.places(placekeys, columns=[random.choice(arr) for i in range(random.randint(1, len(arr)))], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, columns=[random.choice(arr) for i in range(random.randint(1, len(arr)))], return_type="list")) == list
    assert type(safe_graph.places(placekeys, columns=[random.choice(arr) for i in range(random.randint(1, len(arr)))], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, columns=[random.choice(arr) for i in range(random.randint(1, len(arr)))], return_type="list")) == list
    assert type(safe_graph.places(placekeys, columns=[random.choice(arr) for i in range(random.randint(1, len(arr)))], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, columns=[random.choice(arr) for i in range(random.randint(1, len(arr)))], return_type="list")) == list
    assert type(safe_graph.places(placekeys, columns=[random.choice(arr) for i in range(random.randint(1, len(arr)))], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, columns=[random.choice(arr) for i in range(random.randint(1, len(arr)))], return_type="list")) == list
    
# def test_jsonPairDataFrame():  
#     df = safe_graph.places(placekeys, columns=[random.choice(arr) for i in range(random.randint(1, len(arr)))], return_type="pandas")
#     # pure_data = df[["device_type", "placekey"]].dropna().values.tolist()
#     # pd.DataFrame(list(chain.from_iterable(df["device_type"].dropna().to_list())))

def test_get_place_by_locatian_name_address():
    assert type(safe_graph.place_by_name(
        location_name= "Taco Bell", 
        street_address= "710 3rd St", 
        city= "San Francisco", 
        region= "CA", 
        iso_country_code= "US")) == df_type

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
        "tracking_closed_since",
        "geometry_type",
        "polygon_wkt",
        "polygon_class",
        "is_synthetic",
        "enclosed",
        "date_range_start",
        "date_range_end",
        "raw_visit_counts",
        "raw_visitor_counts",
        "visits_by_day",
        "visitor_home_cbgs",
        "visitor_home_aggregation",
        "visitor_daytime_cbgs",
        "visitor_country_of_origin",
        "distance_from_home",
        "median_dwell",
        "bucketed_dwell_times",
        "related_same_day_brand",
        "related_same_month_brand",
        "popularity_by_hour",
        "popularity_by_day",
        "device_type",
    ] 
    df = safe_graph.places(placekeys, columns=["*"], return_type="pandas")
    for i in null_check:
        assert(df[i].isnull().values.any() == False)
    for i in range(len(df)):
        # Check that the naics_code column is a string in any Pandas dataframe results.
        assert(type(df.loc[i]["naics_code"]) == str)

def test_save():
    # Read in the result of save() and make sure it matches the original dataframe.
    df = safe_graph.places(placekeys, columns=["*"], return_type="pandas")
    path = "results.csv"
    safe_graph.save(path)
    saved_df = pd.read_csv(path)
    # if same shape means same values
    assert(df.shape == saved_df.shape)
