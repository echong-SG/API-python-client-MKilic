WM__PATTERNS__ = ["safegraph_weekly_patterns.*"] # , "safegraph_monthly_patterns.*"]
DATASET = ["safegraph_core", "safegraph_geometry", "safegraph_monthly_patterns", "safegraph_weekly_patterns",]
INNER_DATASET = ["safegraph_core.*", "safegraph_geometry.*", "safegraph_monthly_patterns.*", "safegraph_weekly_patterns.*",] # for dataset column functionality
__PATTERNS__ = {
    "safegraph_core": { 
        "__header__": "safegraph_core {",
        # "placekey" : "placekey",
        "parent_placekey" : "parent_placekey",
        "location_name" : "location_name",
        "safegraph_brand_ids" : "safegraph_brand_ids",
        "brands" : """
            brands {
                brand_id: brand_id,
                brand_name: brand_name,
        }""",
        "top_category" : "top_category",
        "sub_category" : "sub_category",
        "naics_code" : "naics_code",
        "latitude" : "latitude",
        "longitude" : "longitude",
        "street_address" : "street_address",
        "city" : "city",
        "region" : "region",
        "postal_code" : "postal_code",
        "iso_country_code" : "iso_country_code",
        "phone_number" : "phone_number",
        "open_hours" : "open_hours",
        "category_tags" : "category_tags",
        "opened_on" : "opened_on",
        "closed_on" : "closed_on",
        "tracking_closed_since" : "tracking_closed_since",
        "geometry_type" : "geometry_type",
        "__footer__": "}"
    },
    "safegraph_geometry": {
        "__header__": "safegraph_geometry {",
        # "placekey": "placekey",
        "parent_placekey": "parent_placekey",
        "location_name": "location_name",
        "brands" : """
            brands {
                brand_id: brand_id,
                brand_name: brand_name,
        }""",
        "latitude": "latitude",
        "longitude": "longitude",
        "street_address": "street_address",
        "city": "city",
        "region": "region",
        "postal_code": "postal_code",
        "iso_country_code": "iso_country_code",
        "polygon_wkt": "polygon_wkt",
        "polygon_class": "polygon_class",
        "includes_parking_lot": "includes_parking_lot",
        "is_synthetic": "is_synthetic",
        "enclosed": "enclosed",
        "__footer__": "}"
    },
    "safegraph_monthly_patterns": {
        "__header__": "safegraph_monthly_patterns {",#_DATE_BLOCK_ {",
        # "placekey": "placekey",
        "parent_placekey": "parent_placekey",
        "location_name": "location_name",
        "street_address": "street_address",
        "city": "city",
        "region": "region",
        "postal_code": "postal_code",
        "safegraph_brand_ids": "safegraph_brand_ids",
        "brands" : """
            brands {
                brand_id: brand_id,
                brand_name: brand_name,
        }""",
        "date_range_start": "date_range_start",
        "date_range_end": "date_range_end",
        "raw_visit_counts": "raw_visit_counts",
        "raw_visitor_counts": "raw_visitor_counts",
        "visits_by_day": "visits_by_day",
        "poi_cbg": "poi_cbg",
        "visitor_home_cbgs": """
            visitor_home_cbgs {
                key: key
                value: value
            }""",
        "visitor_home_aggregation": """
            visitor_home_aggregation {
                key: key
                value: value
        }""",
        "visitor_daytime_cbgs": """
            visitor_daytime_cbgs {
                key: key
                value: value
        }""",
        "visitor_country_of_origin": """
            visitor_country_of_origin {
                key: key
                value: value
        }""",
        "distance_from_home": "distance_from_home",
        "median_dwell": "median_dwell",
        "bucketed_dwell_times": """
            bucketed_dwell_times {
                key: key
                value: value
        }""",
        "related_same_day_brand": """
            related_same_day_brand {
                key: key
                value: value
        }""",
        "related_same_month_brand": """
            related_same_month_brand {
                key: key
                value: value
        }""",
        "popularity_by_hour": "popularity_by_hour",
        "popularity_by_day": """
            popularity_by_day {
                key: key
                value: value
        }""",
        "device_type": """
            device_type {
                key: key
                value: value
        }""",
        "__footer__": "}"
    },
    "safegraph_weekly_patterns": {
        "__header__": "safegraph_weekly_patterns _DATE_BLOCK_ {",
        # "placekey": "placekey",
        "parent_placekey": "parent_placekey",
        "location_name": "location_name",
        "street_address": "street_address",
        "city": "city",
        "region": "region",
        "postal_code": "postal_code",
        "iso_country_code": "iso_country_code",
        "safegraph_brand_ids": "safegraph_brand_ids",
        "brands": "brands",
        "date_range_start": "date_range_start",
        "date_range_end": "date_range_end",
        "raw_visit_counts": "raw_visit_counts",
        "raw_visitor_counts": "raw_visitor_counts",
        "visits_by_day": "visits_by_day",
        "visits_by_each_hour": "visits_by_each_hour",
        "poi_cbg": "poi_cbg",
        "visitor_home_cbgs": """
            visitor_home_cbgs {
                key
                value
        }""",
        "visitor_home_aggregation": """
            visitor_home_aggregation {
                key
                value
        }""",
        "visitor_daytime_cbgs": """
            visitor_daytime_cbgs {
                key
                value
        }""",
        "visitor_country_of_origin": """
            visitor_country_of_origin {
                key
                value
        }""",
        "distance_from_home": "distance_from_home",
        "median_dwell": "median_dwell",
        "bucketed_dwell_times": """
            bucketed_dwell_times {
                key: key
                value: value
        }""",
        "related_same_day_brand": """
            related_same_day_brand {
                key
                value
        }""",
        "related_same_week_brand": """
            related_same_week_brand {
                key
                value
        }""",
        # "device_type": """
        #     device_type {
        #         key: key
        #         value: value
        # }""",
        "__footer__": "}"
    },
}
__VALUE_TYPES__ = {
    "naics_code": int,
    "latitude": float,
    "longitude": float,
}