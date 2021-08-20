from safegraph_ql import client
import pandas as pd

df_type = type(pd.DataFrame())
safe_graph = client.HTTP_Client("x8TUW4IV3hYC1L4Xav56nChUWwtBisRY")

def test_place():
    placekey = "224-222@5vg-7gv-d7q"
    assert type(safe_graph.place(placekey, dataset= ["*"], return_type="pandas")) == df_type
    assert type(safe_graph.place(placekey, dataset= ["*"], return_type="list")) == list

    assert type(safe_graph.place(placekey, dataset=["safegraph_core"], return_type="pandas")) == df_type
    assert type(safe_graph.place(placekey, dataset=["safegraph_core"], return_type="list")) == list
    assert type(safe_graph.place(placekey, dataset=["safegraph_geometry"], return_type="pandas")) == df_type
    assert type(safe_graph.place(placekey, dataset=["safegraph_geometry"], return_type="list")) == list
    assert type(safe_graph.place(placekey, dataset=["safegraph_patterns"], return_type="pandas")) == df_type
    assert type(safe_graph.place(placekey, dataset=["safegraph_patterns"], return_type="list")) == list

    assert type(safe_graph.place(placekey, dataset=["safegraph_core", "safegraph_patterns"], return_type="pandas")) == df_type
    assert type(safe_graph.place(placekey, dataset=["safegraph_core", "safegraph_geometry"], return_type="list")) == list

    assert type(safe_graph.place(placekey, dataset=["safegraph_geometry", "safegraph_core"], return_type="pandas")) == df_type
    assert type(safe_graph.place(placekey, dataset=["safegraph_geometry", "safegraph_patterns"], return_type="list")) == list
    
    assert type(safe_graph.place(placekey, dataset=["safegraph_core", "safegraph_geometry", "safegraph_patterns"], return_type="list")) == list

def test_places():
    placekeys = ["224-222@5vg-7gv-d7q", "222-222@5qw-shj-7qz", "222-222@5s6-pyc-7qz", "zzy-222@5xc-k8q-zmk"]
    assert type(safe_graph.places(placekeys, dataset= ["*"], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, dataset= ["*"], return_type="list")) == list
    
    assert type(safe_graph.places(placekeys, dataset=["safegraph_core"], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, dataset=["safegraph_core"], return_type="list")) == list
    assert type(safe_graph.places(placekeys, dataset=["safegraph_geometry"], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, dataset=["safegraph_geometry"], return_type="list")) == list
    assert type(safe_graph.places(placekeys, dataset=["safegraph_patterns"], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, dataset=["safegraph_patterns"], return_type="list")) == list

    assert type(safe_graph.places(placekeys, dataset=["safegraph_core", "safegraph_patterns"], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, dataset=["safegraph_core", "safegraph_geometry"], return_type="list")) == list

    assert type(safe_graph.places(placekeys, dataset=["safegraph_geometry", "safegraph_core"], return_type="pandas")) == df_type
    assert type(safe_graph.places(placekeys, dataset=["safegraph_geometry", "safegraph_patterns"], return_type="list")) == list
    
    assert type(safe_graph.places(placekeys, dataset=["safegraph_core", "safegraph_geometry", "safegraph_patterns"], return_type="list")) == list

def test_get_place_by_locatian_name_address():
    assert type(safe_graph.place_by_name(
        location_name= "Taco Bell", 
        street_address= "710 3rd St", 
        city= "San Francisco", 
        region= "CA", 
        iso_country_code= "US")) == df_type