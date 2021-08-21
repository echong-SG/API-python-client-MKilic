import pandas as pd
import pprint
printy =  pprint.PrettyPrinter(indent=4).pprint
from gql import gql
from gql import Client as gql_Client
from gql.transport.requests import RequestsHTTPTransport

class safeGraphError(Exception):
    pass

class HTTP_Client:
    def __init__(self, apikey):
        self.url = 'https://api.safegraph.com/v1/graphql'
        self.apikey = apikey # "x8TUW4IV3hYC1L4Xav56nChUWwtBisRY"
        self.headers = {'Content-Type': 'application/json', 'apikey': apikey}
        self.transport = RequestsHTTPTransport(
            url=self.url, 
            verify=True, 
            retries=3,
            headers=self.headers,
        )
        self.client = gql_Client(transport=self.transport, fetch_schema_from_transport=True)
        self.dataset = ["safegraph_core", "safegraph_geometry", "safegraph_patterns"]
        self.__pattern__ = {
            "safegraph_core": { 
                "__header__": "safegraph_core {",
                "location_name": "location_name", 
                "top_category": "top_category", 
                "street_address": "street_address", 
                "city": "city", 
                "region": "region", 
                "postal_code": "postal_code", 
                "latitude": "latitude", 
                "longitude": "longitude", 
                "iso_country_code": "iso_country_code", 
                "__footer__": "}"
            },
            "safegraph_geometry": {
                "__header__": "safegraph_geometry {",
                "location_name": "location_name", 
                "street_address": "street_address", 
                "city": "city", 
                "region": "region", 
                "postal_code": "postal_code", 
                "latitude": "latitude", 
                "longitude": "longitude", 
                "polygon_wkt": "polygon_wkt", 
                "__footer__": "}"
            },
            "safegraph_patterns": {
                "__header__": "safegraph_patterns {",
                "date_range_start": "date_range_start",
                "date_range_end": "date_range_end",
                "median_dwell": "median_dwell",
                "bucketed_dwell_times": "bucketed_dwell_times { key value }",
                "popularity_by_day": "popularity_by_day { key value }",
                "visits_by_day": "visits_by_day",
                "poi_cbg": "poi_cbg",
                "__footer__": "}"
            }
        }

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

    def place(self, placekey, return_type="pandas", columns=["*"]):
        """
            :param str placekey:            Unique Placekey ID
            :param str return_type:         Desired return type ether "pandas" or "list"
            :param list columns:            ["*"] for all or desired column in dataframe
            :return:                        The data of given placekey in return_type
            :rtype:                         pandas.DataFrame or dict
        """
        params = {"placekey": placekey}
        dataset, data_type = self.__dataset__(columns)
        query = gql(
            f"""query($placekey: Placekey!) {{
                place(placekey: $placekey) {{
                        placekey 
                    {dataset}
                }}
            }}"""
        ) 
        result = self.client.execute(query, variable_values=params)
        data_frame = []
        dict_ = {}
        for j in data_type:
            dict_.update(result['place'][j])
        data_frame.append(dict_)

        if return_type == "pandas":
            df = pd.DataFrame.from_dict(data_frame)
            return df
        if return_type == "list":
            return data_frame
        else:
            raise safeGraphError(f'return_type "{return_type}" does not exist')

    def places(self, placekeys, return_type="pandas", columns=["*"]):
        """
            :param list placekeys:          Unique Placekey ID inside an array
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
            
        if return_type == "pandas":
            df = pd.DataFrame(data_frame)
            return df
        if return_type == "list":
            return data_frame
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