============
safegraphQL
============
.. image:: https://github.com/datamade/census/workflows/Python%20package/badge.svg

API of https://api.safegraph.com/v1/graphql

Installation
============
::

    pip install safegraphQL

Usage
=====
First, get yourself a `SafeGraph API key <https://shop.safegraph.com/api>`_.

::

    from safegraphql import client
    sgql_client = client.HTTP_Client("MY_API_KEY")
    placekeys = [
        "zzw-222@8fy-fjg-b8v", # (Disney World)
        "zzy-227@5sb-8cw-pjv", # (O'Hare Airport)
        "222-223@65y-rxx-djv", # (Walmart in Albany, NY)
        ] 
    # returns pandas dataframe
    df = sgql_client.places(placekeys, columns="*")

Datasets
========
* CORE PLACES

Reliable POI For Accuracy In Your Data Models.
With global coverage for any brand you request, our Core dataset provides high quality data for the POI you care about.

* GEOMETRY

Machine Generated, Human Verified Building Footprint Polygons.
SafeGraph Geometry allows data leaders to understand POI footprints with spatial hierarchy metadata.

* PATTERNS

Understand Consumer Behavior With Verified Foot Traffic Data.
Aggregate foot fall data to either specific POIs or to Census Block Groups (CBGs) with SafeGraph Patterns.

Examples
========
::

    from safegraphql import client
    sgql_client = client.HTTP_Client("MY_API_KEY")
    placekeys = [
        "zzw-222@8fy-fjg-b8v", # (Disney World)
        "zzy-227@5sb-8cw-pjv", # (O'Hare Airport)
        "222-223@65y-rxx-djv", # (Walmart in Albany, NY)
        ]
    ##########
    # search #
    ##########
    df = sgql_client.search( brand = "starbucks", brand_id = None, 
        naics_code = None, phone_number = None, street_address = None, 
        city = None, region = None, postal_code = None, iso_country_code = None, columns="safegraph_core.*")
    ##################
    # lookup_by_name #
    ##################
    df_2 = sgql_client.lookup_by_name(
        location_name= "Taco Bell", 
        street_address= "710 3rd St", 
        city= "San Francisco", 
        region= "CA", 
        iso_country_code= "US",
        return_type="pandas",
        columns="*")
    ################
    # lookup #
    ################
    df_3 = sgql_client.lookup(placekeys, columns="*")
    ########
    # save #
    ########
    # saves last pulled data as results.csv on default settings
    sgql_client.save(path="__default__", return_type="__default__")
