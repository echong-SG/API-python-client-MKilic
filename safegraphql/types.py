WM__PATTERNS__ = ["safegraph_weekly_patterns.*"] # , "safegraph_monthly_patterns.*"]
DATASET = ["safegraph_core", "safegraph_geometry", "safegraph_monthly_patterns", "safegraph_weekly_patterns",]
INNER_DATASET = ["safegraph_core.*", "safegraph_geometry.*", "safegraph_monthly_patterns.*", "safegraph_weekly_patterns.*",] # for dataset column functionality
__PATTERNS__ = {
    "safegraph_core": { 
        "__header__": "safegraph_core {\n",
        # "placekey": "\t\tplacekey\n",
        "parent_placekey": "\t\tparent_placekey\n",
        "location_name": "\t\tlocation_name\n",
        "safegraph_brand_ids": "\t\tsafegraph_brand_ids\n",
        "brands": """\t\tbrands """,
        "top_category": "\t\ttop_category\n",
        "sub_category": "\t\tsub_category\n",
        "naics_code": "\t\tnaics_code\n",
        "latitude": "\t\tlatitude\n",
        "longitude": "\t\tlongitude\n",
        "street_address": "\t\tstreet_address\n",
        "city": "\t\tcity\n",
        "region": "\t\tregion\n",
        "postal_code": "\t\tpostal_code\n",
        "iso_country_code": "\t\tiso_country_code\n",
        "phone_number": "\t\tphone_number\n",
        "open_hours": "\t\topen_hours\n",
        "category_tags": "\t\tcategory_tags\n",
        "opened_on": "\t\topened_on\n",
        "closed_on": "\t\tclosed_on\n",
        "tracking_closed_since": "\t\ttracking_closed_since\n",
        "geometry_type": "\t\tgeometry_type\n",
        "__footer__": "\t\t}"
    },
    "safegraph_geometry": {
        "__header__": "safegraph_geometry {\n",
        # "placekey": "\t\tplacekey\n",
        "parent_placekey": "\t\tparent_placekey\n",
        "location_name": "\t\tlocation_name\n",
        "safegraph_brand_ids": "\t\tsafegraph_brand_ids\n",
        "brands": """\t\tbrands """,
        "latitude": "\t\tlatitude\n",
        "longitude": "\t\tlongitude\n",
        "street_address": "\t\tstreet_address\n",
        "city": "\t\tcity\n",
        "region": "\t\tregion\n",
        "postal_code": "\t\tpostal_code\n",
        "iso_country_code": "\t\tiso_country_code\n",
        "polygon_wkt": "\t\tpolygon_wkt\n",
        "polygon_class": "\t\tpolygon_class\n",
        "includes_parking_lot": "\t\tincludes_parking_lot\n",
        "is_synthetic": "\t\tis_synthetic\n",
        "enclosed": "\t\tenclosed\n",
        "__footer__": "\t\t}"
    },
    "safegraph_monthly_patterns": {
        "__header__": "safegraph_monthly_patterns {\n",#_DATE_BLOCK_ {\n",
        # "placekey": "\t\tplacekey\n",
        "parent_placekey": "\t\tparent_placekey\n",
        "location_name": "\t\tlocation_name\n",
        "street_address": "\t\tstreet_address\n",
        "city": "\t\tcity\n",
        "region": "\t\tregion\n",
        "postal_code": "\t\tpostal_code\n",
        "safegraph_brand_ids": "\t\tsafegraph_brand_ids\n",
        "brands": """\t\tbrands """,
        "date_range_start": "\t\tdate_range_start\n",
        "date_range_end": "\t\tdate_range_end\n",
        "raw_visit_counts": "\t\traw_visit_counts\n",
        "raw_visitor_counts": "\t\traw_visitor_counts\n",
        "visits_by_day": "\t\tvisits_by_day\n",
        "poi_cbg": "\t\tpoi_cbg\n",
        "visitor_home_cbgs": "\t\tvisitor_home_cbgs\n",
        "visitor_home_aggregation": "\t\tvisitor_home_aggregation\n",
        "visitor_daytime_cbgs": "\t\tvisitor_daytime_cbgs\n",
        "visitor_country_of_origin": "\t\tvisitor_country_of_origin\n",
        "distance_from_home": "\t\tdistance_from_home\n",
        "median_dwell": "\t\tmedian_dwell\n",
        "bucketed_dwell_times": "\t\tbucketed_dwell_times\n",
        "related_same_day_brand": "\t\trelated_same_day_brand\n",
        "related_same_month_brand": "\t\trelated_same_month_brand\n",
        "popularity_by_hour": "\t\tpopularity_by_hour\n",
        "popularity_by_day": "\t\tpopularity_by_day\n",
        "device_type": "\t\tdevice_type\n",
        "__footer__": "\t\t}"
    },
    "safegraph_weekly_patterns": {
        "__header__": "safegraph_weekly_patterns _DATE_BLOCK_ {\n",
        # "placekey": "\t\tplacekey\n",
        "parent_placekey": "\t\tparent_placekey\n",
        "location_name": "\t\tlocation_name\n",
        "street_address": "\t\tstreet_address\n",
        "city": "\t\tcity\n",
        "region": "\t\tregion\n",
        "postal_code": "\t\tpostal_code\n",
        "iso_country_code": "\t\tiso_country_code\n",
        "safegraph_brand_ids": "\t\tsafegraph_brand_ids\n",
        "brands": "\t\tbrands\n",
        "date_range_start": "\t\tdate_range_start\n",
        "date_range_end": "\t\tdate_range_end\n",
        "raw_visit_counts": "\t\traw_visit_counts\n",
        "raw_visitor_counts": "\t\traw_visitor_counts\n",
        "visits_by_day": "\t\tvisits_by_day\n",
        "visits_by_each_hour": "\t\tvisits_by_each_hour\n",
        "poi_cbg": "\t\tpoi_cbg\n",
        "visitor_home_cbgs": "\t\tvisitor_home_cbgs\n",
        "visitor_home_aggregation": "\t\tvisitor_home_aggregation\n",
        "visitor_daytime_cbgs": "\t\tvisitor_daytime_cbgs\n",
        "visitor_country_of_origin": "\t\tvisitor_country_of_origin\n",
        "distance_from_home": "\t\tdistance_from_home\n",
        "median_dwell": "\t\tmedian_dwell\n",
        "bucketed_dwell_times": "\t\tbucketed_dwell_times\n",
        "related_same_day_brand": "\t\trelated_same_day_brand\n",
        "related_same_week_brand": "\t\trelated_same_week_brand\n",
        "device_type": "\t\tdevice_type\n",
        "__footer__": "\t\t}"
    },
}
__VALUE_TYPES__ = {
    "naics_code": int,
    "latitude": float,
    "longitude": float,
}