import pandas as pd
import pprint
printy =  pprint.PrettyPrinter(indent=4).pprint
from gql import gql
from gql import Client as gql_Client
from gql.transport.requests import RequestsHTTPTransport

class safeGraphError(Exception):
    pass

class HTTP_Client:
    def __init__(self, apikey, max_tries=3):
        self.df = pd.DataFrame()
        self.lst = [] 
        self.url = 'https://api.safegraph.com/v1/graphql'
        self.apikey = apikey
        self.headers = {'Content-Type': 'application/json', 'apikey': apikey}
        self.transport = RequestsHTTPTransport(
            url=self.url, 
            verify=True, 
            retries=max_tries,
            headers=self.headers,
        )
        self.client = gql_Client(transport=self.transport, fetch_schema_from_transport=True)
        self.dataset = ["safegraph_core", "safegraph_geometry", "safegraph_patterns"]
        self.__pattern__ = {
            "safegraph_core": { 
                "__header__": "safegraph_core {",
                "placekey" : "placekey",
                "parent_placekey" : "parent_placekey",
                "location_name" : "location_name",
                "safegraph_brand_ids" : "safegraph_brand_ids",
                "brands" : """
                    brands {
                        brand_id: brand_id,
                        brand_name: brand_name,
                    }
                """,
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
                "placekey": "placekey",
                "parent_placekey": "parent_placekey",
                "location_name": "location_name",
                "brands" : """
                    brands {
                        brand_id: brand_id,
                        brand_name: brand_name,
                    }
                """,
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
                "building_height": "building_height",
                "enclosed": "enclosed",
                "__footer__": "}"
            },
            "safegraph_patterns": {
                "__header__": "safegraph_patterns {",
                "placekey": "placekey",
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
                    }
                """,
                "date_range_start": "date_range_start",
                "date_range_end": "date_range_end",
                "raw_visit_counts": "raw_visit_counts",
                "raw_visitor_counts": "raw_visitor_counts",
                "visits_by_day": "visits_by_day",
                "visitor_home_cbgs": """
                    visitor_home_cbgs {
                        key: key
                        value: value
                        }
                """,
                "visitor_home_aggregation": """
                    visitor_home_aggregation {
                        key: key
                        value: value
                    }
                """,
                "visitor_daytime_cbgs": """
                    visitor_daytime_cbgs {
                        key: key
                        value: value
                    }
                """,
                "visitor_country_of_origin": """
                    visitor_country_of_origin {
                        key: key
                        value: value
                    }
                """,
                "distance_from_home": "distance_from_home",
                "median_dwell": "median_dwell",
                "bucketed_dwell_times": """
                    bucketed_dwell_times {
                        key: key
                        value: value
                    }
                """,
                "related_same_day_brand": """
                    related_same_day_brand {
                        key: key
                        value: value
                    }
                """,
                "related_same_month_brand": """
                    related_same_month_brand {
                        key: key
                        value: value
                    }
                """,
                "popularity_by_hour": "popularity_by_hour",
                "popularity_by_day": """
                    popularity_by_day {
                        key: key
                        value: value
                    }
                """,
                "device_type": """
                    device_type {
                        key: key
                        value: value
                    }
                """,
                "__footer__": "}"
            }
        }
        self.value_types = {
            "naics_code": str,
        }

    def __str__(self):
        return f"api url: {self.url} | apikey: {self.apikey}"

    def __change_value_types_pandas(self):
        for key, val in self.value_types.items():
            try:
                self.df = self.df.astype({key: val}) # errors='ignore')
            except KeyError:
                pass

    def __change_value_types_lst(self):
        # TODO
        # self.lst
        pass

    def save(self, path="results.csv"):
        """
            :param str path:                location of csv file e.g: "results.csv"
            saves last pulled datafame as a csv file to given location
            if path is not given saves to current location as results.csv
        """
        self.df.to_csv(path_or_buf=path, index=False)

    def __dataset__(self, columns):
        query = ""
        data_type = []
        if type(columns) != list:
            raise ValueError("*** columns argumnet has to be a list")
        if columns[0] == "*":
            for i in self.__pattern__:
                for j in self.__pattern__[i]:
                    query += self.__pattern__[i][j] + " "
            data_type = self.dataset
        else:
            for i in self.dataset:
                if columns == "*":
                    for j in self.__pattern__[i]:
                        query += self.__pattern__[i][j] + " "
                    data_type.append(i)
                else:
                    available_columns = [j for j in self.__pattern__[i] if j in columns]
                    if len(available_columns) > 0:
                        data_type.append(i)
                        query += self.__pattern__[i]["__header__"] + " "
                        for j in available_columns:
                            query += self.__pattern__[i][j] + " "
                        query += self.__pattern__[i]["__footer__"] + " "
        if query == "":
            raise ValueError(f"*** Bad column assignment, check your paramaters: {columns}")
        return query, data_type

    def __adjustments(self, data_frame):
        self.lst = data_frame
        self.__change_value_types_lst()
        self.df = pd.DataFrame.from_dict(data_frame)
        self.__change_value_types_pandas()

    def places(self, placekeys, return_type="pandas", columns=["*"]):
        """
            :param list placekeys:          Unique Placekey ID/IDs inside an array
                                            [ a single placekey string or a list of placekeys are both acceptable ]
            :param str return_type:         Desired return type ether "pandas" or "list"
            :param list columns:            ["*"] for all or desired column in dataframe
            :return:                        The data of given placekeys in return_type
            :rtype:                         pandas.DataFrame or dict
        """
        params = {"placekeys": placekeys}
        dataset, data_type = self.__dataset__(columns)
        query = gql(
            f"""query($placekeys: [Placekey!]) {{
                places(placekeys: $placekeys) {{
                    placekey
                {dataset}
                }}
            }}"""
        ) 
        result = self.client.execute(query, variable_values=params)
        data_frame = []
        for place in result['places']:
            dict_ = {}
            for j in data_type:
                dict_.update(place[j])
            data_frame.append(dict_)

        # adjustments
        self.__adjustments(data_frame)

        if return_type == "pandas":
            return self.df
        elif return_type == "list":
            return self.lst
        else:
            raise safeGraphError(f'return_type "{return_type}" does not exist')

    def place_by_name(self, location_name, street_address, city, region, iso_country_code, return_type="pandas"):
        """
            :param str location_name:       location_name of the desidred place
            :param str street_address:      street_address of the desidred place
            :param str city:                city of the desidred place
            :param str region:              region of the desidred place
            :param str iso_country_code:    iso_country_code of the desidred place
            :param str return_type:         Desired return type ether "pandas" or "list"
            :return:                        The data of given placekey in return_type
            :rtype:                         pandas.DataFrame or dict
            XXX EXAMPLE 
                :raises ValueError:         if placekey is not found in database
        """
        query = gql(
            f"""query {{
                place(query: {{
                        location_name: "{location_name}", 
                        street_address: "{street_address}", 
                        city: "{city}", 
                        region: "{region}", 
                        iso_country_code: "{iso_country_code}"
                    }}) {{ 
                    placekey 
                    safegraph_core {{
                        location_name
                        street_address
                        postal_code
                        phone_number
                        category_tags
                    }}
                }}
            }}"""
        ) 
        result = self.client.execute(query)
        if return_type == "pandas":
            df = pd.DataFrame.from_dict(result['place']['safegraph_core'], orient="index")
            self.df = df
            return df
        if return_type == "list":
            return result
        else:
            raise safeGraphError(f'return_type "{return_type}" does not exist')

# print("\n\tSINGLE PULL FROM PLACEKEY\n")
# print(get_place_by_placekey(query_pk = "222-222@5qw-shj-7qz"))
# print("\n\tSINGLE PULL FROM location_name, street_address, city, region, iso_country_code\n")
# print(get_place_by_locatian_name_address(
#   location_name= "Taco Bell", 
#   street_address= "710 3rd St", 
#   city= "San Francisco", 
#   region= "CA", 
#   iso_country_code= "US"))
# placekeys = ["224-222@5vg-7gv-d7q", "222-222@5qw-shj-7qz", "222-222@5s6-pyc-7qz", "zzy-222@5xc-k8q-zmk"]
# print(f"\n\tMULTIPLE PULL FROM PLACEKEYS: {placekeys}\n")
# print(get_places_by_placekeys(placekeys))