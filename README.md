# safegraphQL

A Python library for accessing [SafeGraph data](https://docs.safegraph.com/docs/about-safegraph) through [SafeGraph's GraphQL API](https://docs.safegraph.com/reference#places-api-overview-new). 

Please see the [SafeGraph API documentation](https://docs.safegraph.com/reference) for further information on GraphQL, available datasets, query types, use cases, and FAQs.

Please file issues on this repository for bugs or feature requests specific to this Python client. For bugs or feature requests related to the SafeGraph API itself, please contact [...]

# Datasets

**[Core Places](https://docs.safegraph.com/docs/core-places):** Base information such as location name, category, and brand association for points of interest (POIs) where consumers spend time or business operations take place. Available for ~9.9MM POI including permanently closed POIs.

**[Geometry](https://docs.safegraph.com/docs/geometry-data)**: POI footprints with spatial hierarchy metadata depicting when child polygons are contained by parents or when two tenants share the same polygon. Available for ~9.2MM POI (Geometry metadata not provided for closed POIs).

**[Patterns](https://docs.safegraph.com/docs/monthly-patterns)**: Place, traffic, and demographic aggregations that answer: how often people visit, how long they stay, where they came from, where else they go, and more. Available for ~4.5MM POI in weekly and monthly versions. _Historical data dating back to January 2018 is available via the API for the weekly version of Patterns only. For the monthly version of Patterns, only the most recent month is available via the API._

# Installation

```python
pip install safegraphQL
```

# Usage

## Requirements

Get an API key from the [SafeGraph Shop](https://shop.safegraph.com/api) and instantiate the client.

```python
from safegraphql import client
sgql_client = client.HTTP_Client("MY_API_KEY")
```

Use the `sgql_client` object to make requests!

## Outputs

By default, query functions in safegraphQL return pandas DataFrames. See the `return_type` parameter below for how to return a JSON response object instead.

## `lookup()`

### One Placekey

Query all Core Places columns for a single Placekey.

```python
pk = 'zzw-222@8fy-fjg-b8v' # Disney World
core = sgql_client.lookup(product = 'core', placekeys = pk, columns = '*')

core
```

|    | placekey            | parent_placekey     | location_name       | safegraph_brand_ids                           | brands                                                                                                  | top_category                                                           | sub_category                         |   naics_code |   latitude |   longitude | street_address            | city   | region   |   postal_code | iso_country_code   | phone_number   | open_hours                                                                                                                                                                                             | category_tags   | opened_on   | closed_on   | tracking_closed_since   | geometry_type   |
|---:|:--------------------|:--------------------|:--------------------|:----------------------------------------------|:--------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------|:-------------------------------------|-------------:|-----------:|------------:|:--------------------------|:-------|:---------|--------------:|:-------------------|:---------------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:----------------|:------------|:------------|:------------------------|:----------------|
|  0 | 222-223@65y-rxx-djv | 224-225@65y-rxx-dgk | Walmart Supercenter | ['SG_BRAND_04a8ca7bf49e7ecb4a32451676e929f0'] | [{'brand_id': 'SG_BRAND_04a8ca7bf49e7ecb4a32451676e929f0', 'brand_name': 'Walmart Supercenter Canada'}] | General Merchandise Stores, including Warehouse Clubs and Supercenters | All Other General Merchandise Stores |       452319 |    42.6947 |     -73.847 | 141 Washington Avenue Ext | Albany | NY       |         12205 | US                 |                | { "Mon": [["5:00", "23:00"]], "Tue": [["5:00", "23:00"]], "Wed": [["5:00", "23:00"]], "Thu": [["5:00", "23:00"]], "Fri": [["5:00", "23:00"]], "Sat": [["5:00", "23:00"]], "Sun": [["5:00", "23:00"]] } | []              |             |             | 2019-07-01              | POLYGON         |

You can do the same for Geometry and Monthly Patterns.

```python
geo = sgql_client.lookup(product = 'geometry', placekeys = pk, columns = '*')
patterns = sgql_client.lookup(product = 'monthly_patterns', placekeys = pk, columns = '*')
```

Query the most recent Weekly Patterns data

```python
watterns = sgql_client.lookup(product = 'weekly_patterns', placekeys = pk, columns = '*')
```

Query an arbitrary set of columns from a dataset.

```python
# requested columns must all come from the same dataset
cols = [
    'placekey',
    'location_name',
    'street_address',
    'city',
    'region',
    'brands',
    'top_category',
    'sub_category',
    'naics_code'
]

sgql_client.lookup(product = 'core', placekeys = pk, columns = cols)
```

|    | placekey            | location_name       | brands                                                                                                  | top_category                                                           | sub_category                         |   naics_code | street_address            | city   | region   |
|---:|:--------------------|:--------------------|:--------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------------|:-------------------------------------|-------------:|:--------------------------|:-------|:---------|
|  0 | 222-223@65y-rxx-djv | Walmart Supercenter | [{'brand_id': 'SG_BRAND_04a8ca7bf49e7ecb4a32451676e929f0', 'brand_name': 'Walmart Supercenter Canada'}] | General Merchandise Stores, including Warehouse Clubs and Supercenters | All Other General Merchandise Stores |       452319 | 141 Washington Avenue Ext | Albany | NY       |

### Multiple Placekeys

You can perform any of the previous queries on a set of multiple Placekeys.

```python
pks = [
    'zzw-222@8fy-fjg-b8v', # Disney World 
    'zzw-222@5z6-3h9-tsq'  # LAX
]

sgql_client.lookup(
    product = 'core',
    placekeys = pks, 
    columns = cols
)
```

|    | placekey            | location_name                     | brands   | top_category                              | sub_category              |   naics_code | street_address           | city       | region   |
|---:|:--------------------|:----------------------------------|:---------|:------------------------------------------|:--------------------------|-------------:|:-------------------------|:-----------|:---------|
|  0 | zzw-222@5z6-3h9-tsq | Los Angeles International Airport | []       | Support Activities for Air Transportation | Other Airport Operations  |       488119 | 1 World Way              | El Segundo | CA       |
|  1 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort          | []       | Amusement Parks and Arcades               | Amusement and Theme Parks |       713110 | Walt Disney World Resort | Orlando    | FL       |

### `return_type`

By default, query functions in safegraphQL return pandas DataFrames. By setting `return_type = 'list'`, you can return the JSON response object instead.

```python
# core columns only
sgql_client.lookup(product = 'core', placekeys = pk, columns = '*', return_type = 'list')

---

[{'placekey': 'zzw-222@8fy-fjg-b8v',
  'parent_placekey': None,
  'location_name': 'Walt Disney World Resort',
  'safegraph_brand_ids': [],
  'brands': [],
  'top_category': 'Amusement Parks and Arcades',
  'sub_category': 'Amusement and Theme Parks',
  'naics_code': 713110,
  'latitude': 28.388228,
  'longitude': -81.567304,
  'street_address': 'Walt Disney World Resort',
  'city': 'Orlando',
  'region': 'FL',
  'postal_code': '32830',
  'iso_country_code': 'US',
  'phone_number': None,
  'open_hours': '{ "Mon": [["8:00", "23:00"]], "Tue": [["8:00", "23:00"]], "Wed": [["8:00", "23:00"]], "Thu": [["8:00", "23:00"]], "Fri": [["8:00", "23:00"]], "Sat": [["8:00", "23:00"]], "Sun": [["8:00", "23:00"]] }',
  'category_tags': [],
  'opened_on': None,
  'closed_on': None,
  'tracking_closed_since': '2019-07-01',
  'geometry_type': 'POLYGON'}]
```

### `save()`

Export the most recently queried result. If the previous result had been a pandas DataFrame, the saved file will be a .csv. If the result had been the JSON response object, the saved file will be a .json. The default path for the exported file will be `results.{csv/json}`.

```python
# saved file will be results.csv
sgql_client.lookup(product = 'core', placekeys = pk, columns = '*')
sgql_client.save()

# saved file will be results.json
sgql_client.lookup(product = 'core', placekeys = pk, columns = '*', return_type = 'list')
sgql_client.save()

# saved file will be safegraph_data.csv
sgql_client.lookup(product = 'core', placekeys = pk, columns = '*')
sgql_client.save(path = 'safegraph_data.csv')
```

## `sg_merge()`

Merge safegraphQL query results with `sg_merge()`.

```python
core = sgql_client.lookup(product = 'core', placekeys = pks, columns = ['placekey', 'location_name', 'naics_code', 'top_category', 'sub_category'])
geo = sgql_client.lookup(product = 'geometry', placekeys = pks, columns = ['placekey', 'polygon_class', 'enclosed'])

merge_set = [core, geo]

merged = sgql_client.sg_merge(datasets = merge_set)
```

|    | placekey            | location_name                     | top_category                              | sub_category              |   naics_code | polygon_class   | enclosed   |
|---:|:--------------------|:----------------------------------|:------------------------------------------|:--------------------------|-------------:|:----------------|:-----------|
|  0 | zzw-222@5z6-3h9-tsq | Los Angeles International Airport | Support Activities for Air Transportation | Other Airport Operations  |       488119 | OWNED_POLYGON   | False      |
|  1 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort          | Amusement Parks and Arcades               | Amusement and Theme Parks |       713110 | OWNED_POLYGON   | False      |

!! ADD SECTION ON INNER JOIN HERE ONCE ISSUE FOR NO LONGER SHOWING NULL PATTERNS ROWS HAS BEEN RESOLVED !!

`sg_merge()` works for JSON response objects as well.

```python
core = sgql_client.lookup(product = 'core', placekeys = pks, columns = ['placekey', 'location_name', 'naics_code', 'top_category', 'sub_category'], return_type = 'list')
geo = sgql_client.lookup(product = 'geometry', placekeys = pks, columns = ['placekey', 'polygon_class', 'enclosed'], return_type = 'list')

merge_set = [core, geo]

merged = sgql_client.sg_merge(datasets = merge_set)

---

[{'placekey': 'zzw-222@5z6-3h9-tsq',
  'location_name': 'Los Angeles International Airport',
  'top_category': 'Support Activities for Air Transportation',
  'sub_category': 'Other Airport Operations',
  'naics_code': 488119,
  'polygon_class': 'OWNED_POLYGON',
  'enclosed': False},
 {'placekey': 'zzw-222@8fy-fjg-b8v',
  'location_name': 'Walt Disney World Resort',
  'top_category': 'Amusement Parks and Arcades',
  'sub_category': 'Amusement and Theme Parks',
  'naics_code': 713110,
  'polygon_class': 'OWNED_POLYGON',
  'enclosed': False}]
  ```

## Historical Weekly Patterns

Use `lookup()` to query Weekly Patterns data for a Placekey from a particular date (`YYYY-MM-DD` format).

```python
date = '2019-06-15'

sgql_client.lookup(
    product = 'weekly_patterns', 
    placekeys = pk, 
    date = date, 
    columns = ['placekey', 'location_name', 'date_range_start', 'date_range_end', 'raw_visit_counts']
)
```
|    | placekey            | location_name            | date_range_start          | date_range_end            |   raw_visit_counts |
|---:|:--------------------|:-------------------------|:--------------------------|:--------------------------|-------------------:|
|  0 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | 2019-06-10T00:00:00-04:00 | 2019-06-17T00:00:00-04:00 |             242530 |

Pass a list of dates to query multiple Weekly Patterns releases. Note that if two dates fall within the same release (e.g. `2019-06-15` and `2019-06-16` below), the data for the relevant week will only be returned once.

```python
# notice the dates list contains 4 elements, but only 3 rows of data are returned
dates = ['2019-06-15', '2019-06-16', '2021-05-23', '2018-10-23']

sgql_client.lookup(
    product = 'weekly_patterns', 
    placekeys = pk, 
    date = dates, 
    columns = ['placekey', 'location_name', 'date_range_start', 'date_range_end', 'raw_visit_counts']
)
```

|    | placekey            | location_name            | date_range_start          | date_range_end            |   raw_visit_counts |
|---:|:--------------------|:-------------------------|:--------------------------|:--------------------------|-------------------:|
|  0 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | 2018-10-22T00:00:00-04:00 | 2018-10-29T00:00:00-04:00 |             169884 |
|  1 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | 2019-06-10T00:00:00-04:00 | 2019-06-17T00:00:00-04:00 |             242530 |
|  2 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | 2021-05-17T00:00:00-04:00 | 2021-05-24T00:00:00-04:00 |             323187 |

Pass a Python dictionary with `date_range_start` and `date_range_end` key/value pairs to query a range of Weekly Patterns releases.

```python
dates = {'date_range_start': '2019-04-10', 'date_range_end': '2019-06-05'}

sgql_client.lookup(
    product = 'weekly_patterns', 
    placekeys = pk, 
    date = dates, 
    columns = ['placekey', 'location_name', 'date_range_start', 'date_range_end', 'raw_visit_counts']
)
```

|    | placekey            | location_name            | date_range_start          | date_range_end            |   raw_visit_counts |
|---:|:--------------------|:-------------------------|:--------------------------|:--------------------------|-------------------:|
|  0 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | 2019-04-15T00:00:00-04:00 | 2019-04-22T00:00:00-04:00 |             249559 |
|  1 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | 2019-04-22T00:00:00-04:00 | 2019-04-29T00:00:00-04:00 |             248989 |
|  2 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | 2019-04-29T00:00:00-04:00 | 2019-05-06T00:00:00-04:00 |             263878 |
|  3 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | 2019-05-06T00:00:00-04:00 | 2019-05-13T00:00:00-04:00 |             247846 |
|  4 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | 2019-05-13T00:00:00-04:00 | 2019-05-20T00:00:00-04:00 |             223901 |
|  5 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | 2019-05-20T00:00:00-04:00 | 2019-05-27T00:00:00-04:00 |             212718 |
|  6 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | 2019-05-27T00:00:00-04:00 | 2019-06-03T00:00:00-04:00 |             236622 |
|  7 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | 2019-06-03T00:00:00-04:00 | 2019-06-10T00:00:00-04:00 |             239621 |

And combine the results with Core Places and Geometry using `sg_merge()`.

```python
dates = {'date_range_start': '2019-04-10', 'date_range_end': '2019-06-05'}

watterns = sgql_client.lookup(
    product = 'weekly_patterns', 
    placekeys = pk, 
    date = dates, 
    columns = ['placekey', 'location_name', 'date_range_start', 'date_range_end', 'raw_visit_counts']
)

core = sgql_client.lookup(product = 'core', placekeys = pk, columns = ['placekey', 'location_name', 'naics_code', 'top_category', 'sub_category'])
geo = sgql_client.lookup(product = 'geometry', placekeys = pk, columns = ['placekey', 'polygon_class', 'enclosed'])

merged = sgql_client.sg_merge(datasets = [core, geo, watterns])
```

|    | placekey            | location_name            | top_category                | sub_category              |   naics_code | polygon_class   | enclosed   | date_range_start          | date_range_end            |   raw_visit_counts |
|---:|:--------------------|:-------------------------|:----------------------------|:--------------------------|-------------:|:----------------|:-----------|:--------------------------|:--------------------------|-------------------:|
|  0 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | Amusement Parks and Arcades | Amusement and Theme Parks |       713110 | OWNED_POLYGON   | False      | 2019-04-15T00:00:00-04:00 | 2019-04-22T00:00:00-04:00 |             249559 |
|  1 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | Amusement Parks and Arcades | Amusement and Theme Parks |       713110 | OWNED_POLYGON   | False      | 2019-04-22T00:00:00-04:00 | 2019-04-29T00:00:00-04:00 |             248989 |
|  2 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | Amusement Parks and Arcades | Amusement and Theme Parks |       713110 | OWNED_POLYGON   | False      | 2019-04-29T00:00:00-04:00 | 2019-05-06T00:00:00-04:00 |             263878 |
|  3 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | Amusement Parks and Arcades | Amusement and Theme Parks |       713110 | OWNED_POLYGON   | False      | 2019-05-06T00:00:00-04:00 | 2019-05-13T00:00:00-04:00 |             247846 |
|  4 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | Amusement Parks and Arcades | Amusement and Theme Parks |       713110 | OWNED_POLYGON   | False      | 2019-05-13T00:00:00-04:00 | 2019-05-20T00:00:00-04:00 |             223901 |
|  5 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | Amusement Parks and Arcades | Amusement and Theme Parks |       713110 | OWNED_POLYGON   | False      | 2019-05-20T00:00:00-04:00 | 2019-05-27T00:00:00-04:00 |             212718 |
|  6 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | Amusement Parks and Arcades | Amusement and Theme Parks |       713110 | OWNED_POLYGON   | False      | 2019-05-27T00:00:00-04:00 | 2019-06-03T00:00:00-04:00 |             236622 |
|  7 | zzw-222@8fy-fjg-b8v | Walt Disney World Resort | Amusement Parks and Arcades | Amusement and Theme Parks |       713110 | OWNED_POLYGON   | False      | 2019-06-03T00:00:00-04:00 | 2019-06-10T00:00:00-04:00 |             239621 |

## `lookup_by_name()`

If you don't know a location's Placekey, you can look it up by name. Note that you should use this for looking up a _particular_ location, but if you are searching for _more than one_ relevant location, you should use the `search()` function described below.

**Note:** When querying by location & address, it's necessary to have at least the following combination of fields to return a result:

    location_name + street_address + city + region + iso_country_code
    location_name + street_address + postal_code + iso_country_code
    location_name + latitude + longitude + iso_country_code

```python
location_name = "Taco Bell"
street_address = "710 3rd St"
city = "San Francisco"
region = "CA"
iso_country_code = "US"

sgql_client.lookup_by_name(
    product = 'core',
    location_name = location_name,
    street_address = street_address,
    city = city,
    region = region,
    iso_country_code = iso_country_code,
    columns = ['placekey', 'location_name', 'street_address', 'city', 'region', 'postal_code', 'iso_country_code', 'latitude', 'longitude']
)
```

|    | placekey            | location_name   |   latitude |   longitude | street_address   | city          | region   |   postal_code | iso_country_code   |
|---:|:--------------------|:----------------|-----------:|------------:|:-----------------|:--------------|:---------|--------------:|:-------------------|
|  0 | 224-222@5vg-7gv-d7q | Taco Bell       |    37.7786 |    -122.393 | 710 3rd St       | San Francisco | CA       |         94107 | US                 |

## Search

You can search for SafeGraph POI by a variety of attributes, as described [here](https://docs.safegraph.com/reference#search).

Search by a single criterion, such as any convenience store POI in the SafeGraph dataset (`naics_code == 445120`). By default, `search()` returns only the first 20 results.

```python
naics_code = 445120

search_result = sgql_client.search(product = 'core', columns = ['placekey', 'location_name', 'street_address', 'city', 'region', 'iso_country_code'], naics_code = naics_code)
```

|    | placekey            | location_name          | street_address                | city         | region     | iso_country_code   |
|---:|:--------------------|:-----------------------|:------------------------------|:-------------|:-----------|:-------------------|
|  0 | zzw-223@646-9rk-nqz | Cash & Dash 7          | 701 Highway 701 N             | Loris        | SC         | US                 |
|  1 | 222-222@63r-tqr-zj9 | 7-Eleven               | 8708 Liberia Ave              | Manassas     | VA         | US                 |
|  2 | zzy-222@4hf-pq3-w6k | Londis                 | 18 & 22 & 26 Winster Mews,    | Gamesley     | Derbyshire | GB                 |
|  3 | 222-223@63v-c97-hnq | Circle K               | 1608 East Ave                 | Akron        | OH         | US                 |
|  4 | zzw-222@8dj-n5s-2hq | 7-Eleven               | 13150 S US Highway 41         | Gibsonton    | FL         | US                 |
|  5 | 224-222@66b-2d2-rhq | Depanneur 7 Jours      | 6024 Avenue De Darlington     | Montreal     | QC         | CA                 |
|  6 | 222-222@5pc-4d2-8n5 | Kwik Trip              | 1549 Madison Ave              | Mankato      | MN         | US                 |
|  7 | 22c-222@5z5-3r9-8jv | 7-Eleven               | 5000 Wilshire Blvd            | Los Angeles  | CA         | US                 |
|  8 | 223-223@5z5-qcd-wc5 | 7-Eleven               | 6401 Mission Gorge Rd         | San Diego    | CA         | US                 |
|  9 | zzw-223@5r8-2cq-nbk | Casey's General Stores | 2604 N Range Line Rd          | Joplin       | MO         | US                 |
| 10 | zzw-223@8gn-kc9-5mk | Circle K               | 101 N Gilmer Ave              | Lanett       | AL         | US                 |
| 11 | zzw-222@5q9-b99-vcq | Circle K               | 7530 Village Square Dr        | Castle Pines | CO         | US                 |
| 12 | zzy-225@3x7-z8z-qj9 | Circle K               | 100 Twelfth Avenue South West | Slave Lake   | AB         | CA                 |
| 13 | 223-223@8sx-zcv-grk | Circle K               | 901 Voss Ave                  | Odem         | TX         | US                 |
| 14 | 224-222@5wb-sdq-r8v | Circle K               | 5301 W Canal Dr               | Kennewick    | WA         | US                 |
| 15 | zzw-226@64h-vj9-mrk | 21st Street Deli       | 222 W 21st St Ste J           | Norfolk      | VA         | US                 |
| 16 | 22k-222@627-wdk-z9f | Victory Meat Center    | 8506 Bay Pkwy                 | Brooklyn     | NY         | US                 |
| 17 | zzy-222@5pm-6rj-4n5 | Quick Mart             | 129 E Hill St                 | Waynesboro   | TN         | US                 |
| 18 | 224-222@3wz-4kr-rc5 | 7-Eleven               | 1704 61st Street South East   | Calgary      | AB         | CA                 |
| 19 | 223-222@5pb-b7m-5s5 | Casey's General Stores | 907 13th St N                 | Humboldt     | IA         | US                 |

Search by multiple criteria, such as Sheetz locations in Pennsylvania.

```python
brand = 'Sheetz'
region = 'PA'

search_result = sgql_client.search(product = 'core', columns = ['placekey', 'location_name', 'street_address', 'city', 'region', 'iso_country_code'], brand = brand, region = region)

search_result.head()
```

|    | placekey            | location_name   | street_address        | city                | region   | iso_country_code   |
|---:|:--------------------|:----------------|:----------------------|:--------------------|:---------|:-------------------|
|  0 | 225-222@63p-wtm-8qf | Sheetz          | 24578 Route 35 N      | Mifflintown         | PA       | US                 |
|  1 | 224-222@63p-d8d-dgk | Sheetz          | 330 Westminster Dr    | Kenmar              | PA       | US                 |
|  2 | 223-222@63s-x95-c89 | Sheetz          | 420 N Baltimore Ave   | Mount Holly Springs | PA       | US                 |
|  3 | 223-222@63d-3y3-3wk | Sheetz          | 4701 William Penn Hwy | Murrysville         | PA       | US                 |
|  4 | 227-222@63p-tv5-brk | Sheetz          | 8711 Woodbury Pike    | East Freedom        | PA       | US                 |

`search()` works for Geometry, Monthly Patterns, and Weekly Patterns as well.

```python
brand = 'Sheetz'
region = 'PA'
date = '2021-07-04'

search_result = sgql_client.search(product = 'weekly_patterns', columns = ['placekey', 'location_name', 'raw_visit_counts'], date = date, brand = brand, region = region)
```

|    | placekey            | location_name   |   raw_visit_counts |
|---:|:--------------------|:----------------|-------------------:|
|  0 | 225-222@63p-wtm-8qf | Sheetz          |                338 |
|  1 | 224-222@63p-d8d-dgk | Sheetz          |                619 |
|  2 | 223-222@63s-x95-c89 | Sheetz          |                241 |
|  3 | 223-222@63d-3y3-3wk | Sheetz          |                705 |
|  4 | 227-222@63p-tv5-brk | Sheetz          |                564 |

Change the `max_results` parameter to request more than the default 20 results.

```python
brand = 'Sheetz'
region = 'PA'
max_results = 200

search_result = sgql_client.search(product = 'core', columns = ['placekey', 'location_name', 'street_address', 'city', 'region', 'iso_country_code'], brand = brand, region = region, max_results = max_results)
```

|    | placekey            | location_name   | street_address     | city             | region   | iso_country_code   |
|---:|:--------------------|:----------------|:-------------------|:-----------------|:---------|:-------------------|
|  0 | 225-222@63p-wtm-8qf | Sheetz          | 24578 Route 35 N   | Mifflintown      | PA       | US                 |
|  1 | 222-222@63p-bjm-xnq | Sheetz          | 270 Route 61 S     | Schuylkill Haven | PA       | US                 |
|  2 | 228-222@63t-p3s-zzz | Sheetz          | 107 Franklin St    | Slippery Rock    | PA       | US                 |
|  3 | zzw-222@63s-xr7-49z | Sheetz          | 6054 Carlisle Pike | Mechanicsburg    | PA       | US                 |
|  4 | zzw-222@63s-9nq-9zz | Sheetz          | 3200 Cape Horn Rd  | Red Lion         | PA       | US                 |
| ... |  |           |  |  |        |                  |
| 195 | zzw-222@63n-xgm-zpv | Sheetz          | 7775 N Route 220 Hwy  | Linden        | PA       | US                 |
| 196 | 222-222@63s-xgf-cyv | Sheetz          | 5201 Simpson Ferry Rd | Mechanicsburg | PA       | US                 |
| 197 | 222-222@63p-8qd-fcq | Sheetz          | 1550 State Rd         | Duncannon     | PA       | US                 |
| 198 | 226-222@63s-xqc-ty9 | Sheetz          | 1720 Harrisburg Pike  | Carlisle      | PA       | US                 |
| 199 | 227-222@63d-77y-dgk | Sheetz          | 1297 Washington Pike  | Bridgeville   | PA       | US                 |

Change the `after_result_number` parameter if you want to skip the first few results. For example, maybe you already searched for the first 2 Sheetz results in PA, and you're interested in the results after that.

```python
brand = 'Sheetz'
region = 'PA'
max_results = 200
after_result_number = 2

search_result = sgql_client.search(product = 'core', columns = ['placekey', 'location_name', 'street_address', 'city', 'region', 'iso_country_code'], brand = brand, region = region, max_results = max_results, after_result_number = after_result_number)
```
