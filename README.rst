======
safegraph_ql
======
.. image:: https://github.com/datamade/census/workflows/Python%20package/badge.svg

API of https://api.safegraph.com/v1/graphql

::

    pip install safegraph_ql

Usage
=====

First, get yourself a `SafeGraph API key <https://shop.safegraph.com/api>`_.

::

    from safegraph_ql import client
    graph = client.HTTP_Client("MY_API_KEY")
    placekeys = [
        "zzw-222@8fy-fjg-b8v", # (Disney World)
        "zzy-227@5sb-8cw-pjv", # (O'Hare Airport)
        "222-223@65y-rxx-djv", # (Walmart in Albany, NY)
        ] 
    df = graph.places(placekeys, columns="*")

Datasets
=====
* CORE PLACES
Reliable POI For Accuracy In Your Data Models\n
With global coverage for any brand you request, our Core dataset provides high quality data for the POI you care about.

* GEOMETRY
Machine Generated, Human Verified Building Footprint Polygons
SafeGraph Geometry allows data leaders to understand POI footprints with spatial hierarchy metadata.

* PATTERNS
Understand Consumer Behavior With Verified Foot Traffic Data
Aggregate foot fall data to either specific POIs or to Census Block Groups (CBGs) with SafeGraph Patterns.

Examples
========
* places
* search
* place_by_name
