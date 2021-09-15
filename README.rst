============
safegraphQL
============
A Python library for `SafeGraph data <https://docs.safegraph.com/docs/about-safegraph>`_ through `SafeGraph's GraphQL API <https://api.safegraph.com/v1/graphql>`_. 

Please file issues on this repository for bugs or feature requests specific to this Python client. For bugs or feature requests related to the SafeGraph API itself, please contact [...]

Datasets
========
**`Core Places <https://docs.safegraph.com/docs/core-places>`_:** Base information such as location name, category, and brand association for points of interest (POIs) where consumers spend time or business operations take place. Available for ~9.9MM POI including permanently closed POIs.
**[Geometry](https://docs.safegraph.com/docs/geometry-data)**: POI footprints with spatial hierarchy metadata depicting when child polygons are contained by parents or when two tenants share the same polygon. Available for ~9.2MM POI (Geometry metadata not provided for closed POIs).
**[Patterns](https://docs.safegraph.com/docs/monthly-patterns)**: Place, traffic, and demographic aggregations that answer: how often people visit, how long they stay, where they came from, where else they go, and more. Available for ~4.5MM POI in weekly and monthly versions. *Historical data dating back to January 2018 is available via the API for the weekly version of Patterns only.*

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
