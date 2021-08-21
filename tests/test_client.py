from safegraph_ql import client
import pandas as pd

df_type = type(pd.DataFrame())
safe_graph = client.HTTP_Client("x8TUW4IV3hYC1L4Xav56nChUWwtBisRY")

def test_place():
    # [random.choice(list(safe_graph.__pattern__.values())) for i in range(random.randint(10)) if i not in ["__header__", "__footer__"]]
    placekey = "224-222@5vg-7gv-d7q"
    assert type(safe_graph.place(placekey, columns=["*"], return_type="pandas")) == df_type
    assert type(safe_graph.place(placekey, columns=["*"], return_type="list")) == list

    assert type(safe_graph.place(placekey, columns=["*"], return_type="pandas")) == df_type
    assert type(safe_graph.place(placekey, columns=["*"], return_type="list")) == list
    assert type(safe_graph.place(placekey, columns=["*"], return_type="pandas")) == df_type
    assert type(safe_graph.place(placekey, columns=["*"], return_type="list")) == list
    assert type(safe_graph.place(placekey, columns=["*"], return_type="pandas")) == df_type
    assert type(safe_graph.place(placekey, columns=["*"], return_type="list")) == list

    assert type(safe_graph.place(placekey, columns=["*"], return_type="pandas")) == df_type
    assert type(safe_graph.place(placekey, columns=["*"], return_type="list")) == list

    assert type(safe_graph.place(placekey, columns=["*"], return_type="pandas")) == df_type
    assert type(safe_graph.place(placekey, columns=["*"], return_type="list")) == list
    
    assert type(safe_graph.place(placekey, columns=["*"], return_type="list")) == list

def test_places():
    placekeys = ["224-222@5vg-7gv-d7q", "222-222@5qw-shj-7qz", "222-222@5s6-pyc-7qz", "zzy-222@5xc-k8q-zmk"]   
    assert type(safe_graph.places(placekeys, columns=["*"], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, columns=["*"], return_type="list")) == list
    
    assert type(safe_graph.places(placekeys, columns=["*"], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, columns=["*"], return_type="list")) == list
    assert type(safe_graph.places(placekeys, columns=["*"], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, columns=["*"], return_type="list")) == list
    assert type(safe_graph.places(placekeys, columns=["*"], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, columns=["*"], return_type="list")) == list

    assert type(safe_graph.places(placekeys, columns=["*"], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, columns=["*"], return_type="list")) == list

    assert type(safe_graph.places(placekeys, columns=["*"], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, columns=["*"], return_type="list")) == list
    
    assert type(safe_graph.places(placekeys, columns=["*"], return_type="list")) == list

def test_jsonPairDataFrame():
    placekeys = ["224-222@5vg-7gv-d7q", "222-222@5qw-shj-7qz", "222-222@5s6-pyc-7qz", "zzy-222@5xc-k8q-zmk"]   
    df = safe_graph.places(placekeys, columns=["*"], return_type="pandas")
    pure_data = df[["device_type", "placekey"]].dropna().values.tolist()
    # pd.DataFrame(list(chain.from_iterable(df["device_type"].dropna().to_list())))

def test_get_place_by_locatian_name_address():
    assert type(safe_graph.place_by_name(
        location_name= "Taco Bell", 
        street_address= "710 3rd St", 
        city= "San Francisco", 
        region= "CA", 
        iso_country_code= "US")) == df_type