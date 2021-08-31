import pandas as pd
import json
from gql import gql
from gql import Client as gql_Client
from gql.transport.requests import RequestsHTTPTransport
from .types import __VALUE_TYPES__, DATASET, INNER_DATASET, __PATTERNS__
### DEBUGGER
# import pprint
# printy =  pprint.PrettyPrinter(indent=4).pprint
##

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
        self.return_type = "pandas"

    def __str__(self):
        return f"api url: {self.url} | apikey: {self.apikey}"

    def __change_value_types_pandas(self):
        for key, val in __VALUE_TYPES__.items():
            try:
                self.df = self.df.astype({key: val}) # errors='ignore')
            except KeyError:
                pass

    def __change_value_types_lst(self):
        for key, val in __VALUE_TYPES__.items():
            if not key in self.lst[0].keys():
                # if not in first element of list not in other pairs
                continue
            for lst in self.lst:
                lst[key] = val(lst[key])

    def save(self, path="__default__", return_type="__default__"):
        """
            :param str path:                location of the file e.g: "results.csv"
                saves as a .json file if return_type was "list" 
                saves as a .csv file if return_type was "pandas"
                if path is not given saves to current location as results.csv or results.json
            :param str return_type:          return type of the saved format by default last return format
        """
        if return_type == "__default__":
            return_type = self.return_type
        if self.return_type == "pandas":
            if path != "__default__":
                self.df.to_csv(path_or_buf=path, index=False)
            else:
               self.df.to_csv(path_or_buf="results.csv", index=False) 
        elif self.return_type == "list":
            if path != "__default__":
                with open(path, 'w') as json_file:
                    json.dump(self.lst, json_file, indent=4)
            else:
                with open("results.json", 'w') as json_file:
                    json.dump(self.lst, json_file, indent=4)


    def __column_check_raise(self, columns):
        dict_ = {el:0 for el in columns}
        for i in __PATTERNS__:
            for j in columns:
                if j not in __PATTERNS__[i].keys():
                    dict_[j]+=1
        invalid_values = [i for i in dict_ if dict_[i] >= len(__PATTERNS__.keys())]
        if len(invalid_values) > 0:
            raise ValueError(f'''
                Invalid column name(s): "{'", "'.join(invalid_values)}"
            ''')

    def __dataset__(self, columns):
        query = ""
        data_type = []
        data_pull = [i.rstrip(".*") for i in columns if i in INNER_DATASET]
        if columns == "*":
            # if all data from all datasets wanted
            for i in __PATTERNS__:
                for j in __PATTERNS__[i]:
                    query += __PATTERNS__[i][j] + " "
            data_type = DATASET
        elif columns == "safegraph_core.*":
            # if all data from safegraph_core
            for j in __PATTERNS__["safegraph_core"]:
                query += __PATTERNS__["safegraph_core"][j] + " "
            data_type = ["safegraph_core"]
        elif columns == "safegraph_geometry.*":
            # if all data from safegraph_geometry
            for j in __PATTERNS__["safegraph_geometry"]:
                query += __PATTERNS__['safegraph_geometry'][j] + " "
            data_type = ["safegraph_geometry"]
        elif columns == "safegraph_patterns.*":
            # if all data from safegraph_patterns
            for j in __PATTERNS__["safegraph_patterns"]:
                query += __PATTERNS__['safegraph_patterns'][j] + " "
            data_type = ["safegraph_patterns"]
        elif type(columns) != list:
            raise ValueError("""*** columns argument must to be a list or one of the following string: 
                *, safegraph_core.*, safegraph_geometry.*, safegraph_patterns.*
            """)
        elif len(data_pull) > 0:
            # if spesific dataset(s) wanted
            for i in data_pull:
                for j in __PATTERNS__[i]:
                    query += __PATTERNS__[i][j] + " "
                data_type.append(i)
        else:
            self.__column_check_raise(columns)
            # if spesific column(s) wanted
            for i in DATASET:
                available_columns = [j for j in __PATTERNS__[i] if j in columns]
                if len(available_columns) > 0:
                    data_type.append(i)
                    query += __PATTERNS__[i]["__header__"] + " "
                    for j in available_columns:
                        query += __PATTERNS__[i][j] + " "
                    query += __PATTERNS__[i]["__footer__"] + " "
        return query, data_type

    def __adjustments(self, data_frame):
        self.lst = data_frame
        self.__change_value_types_lst()
        self.df = pd.DataFrame.from_dict(data_frame)
        self.__change_value_types_pandas()

    def places(self, placekeys, columns, return_type="pandas"):
        """
            :param list placekeys:          Unique Placekey ID/IDs inside an array
                                            [ a single placekey string or a list of placekeys are both acceptable ]
            :param str return_type:         Desired return type ether "pandas" or "list"
            :param list/str columns:            * as string for all or desired column(s) in a [list]
            :return:                        The data of given placekeys in return_type
            :rtype:                         pandas.DataFrame or dict
        """
        self.return_type = return_type
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

        if self.return_type == "pandas":
            return self.df
        elif self.return_type == "list":
            return self.lst
        else:
            raise safeGraphError(f'return_type "{return_type}" does not exist')

    def place_by_name(self, location_name, street_address, city, region, iso_country_code, columns, return_type="pandas"):
        """
            :param str location_name:       location_name of the desidred place
            :param str street_address:      street_address of the desidred place
            :param str city:                city of the desidred place
            :param str region:              region of the desidred place
            :param str iso_country_code:    iso_country_code of the desidred place
            :param list/str columns:        * as string for all or desired column(s) in a [list]
            :param str return_type:         Desired return type ether "pandas" or "list"
                                                default -> "pandas"
            :return:                        The data of given placekey in return_type
            :rtype:                         pandas.DataFrame or dict
        """
        self.return_type = return_type
        params = {
            "location_name": location_name, 
            "street_address": street_address, 
            "city": city, 
            "region": region, 
            "iso_country_code": iso_country_code
        }
        dataset, data_type = self.__dataset__(columns)
        query = gql(
            f"""query ($location_name: String!, $street_address: String!, $city: String!, $region: String!, $iso_country_code: String!) {{
                place(query: {{
                        location_name: $location_name, 
                        street_address: $street_address, 
                        city: $city, 
                        region: $region, 
                        iso_country_code: $iso_country_code
                    }}) {{ 
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

        # adjustments
        self.__adjustments(data_frame)

        if self.return_type == "pandas":
            return self.df
        elif self.return_type == "list":
            return self.lst
        else:
            raise safeGraphError(f'return_type "{return_type}" does not exist')

    def __chunks__(self, lst, n):
     """Yield successive n-sized chunks from lst."""
     for i in range(0, len(lst), n):
         yield lst[i:i + n]

    def search(self, columns, 
        brand = None, brand_id = None, naics_code = None, 
        # address with following sub-fields
        phone_number = None, street_address = None, city = None, region = None, postal_code = None, iso_country_code = None,
        max_results=20,
        after_result_number=0,
        return_type="pandas"):
        """
            :param str brand:               brand for searching query
            :param str brand_id:            brand_id for searching query
            :param str naics_code:          naics_code for searching query
            :param str phone_number:        phone_number for searching query
            :param str street_address:      street_address of the desidred place
            :param str city:                city of the desidred place
            :param str region:              region of the desidred place
            :param str postal_code:         postal_code of the desidred place
            :param str iso_country_code:    iso_country_code of the desidred place
            :param list/str columns:        * as string for all or desired column(s) in a [list]
            :param str return_type:         Desired return type ether "pandas" or "list"
                                                default -> "pandas"
            :return:                        The data of given placekey in return_type
            :rtype:                         pandas.DataFrame or dict

        """
        self.return_type = return_type
        dataset, data_type = self.__dataset__(columns)
        params = f"""
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("brand", brand)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("brand_id", brand_id)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("naics_code", naics_code)}
"""
        address = f""" address: {{
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("phone_number", phone_number)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("street_address", street_address)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("city", city)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("region", region)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("postal_code", postal_code)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("iso_country_code", iso_country_code)}
}}"""
        if address != ' address: {\n\n\n\n\n\n\n}':
            params+=address
        after = 0
        output = []
        chunks = self.__chunks__([i for i in range(max_results)], 20)
        for i in chunks:
            first = len(i)
            query = gql(
                f"""query {{
                    search(first: {first} after: {after+after_result_number} filter: {{
                        {params}
                        }}) {{ 
                        placekey 
                        {dataset}
                    }}
                }}"""
            )
            result = self.client.execute(query)
            output+=result['search']
            after += first
                        # print(f"{max_results=},{len(output)=}")
                        # arr = []
                        # for i in output: 
                        #     if i['placekey'] not in arr:
                        #         arr.append(i['placekey'])
        data_frame = []
        for out in output:
            dict_ = {}
            for j in data_type:
                dict_.update(out[j])
            dict_['placekey'] = out["placekey"]
            data_frame.append(dict_)

        # adjustments
        self.__adjustments(data_frame)

        if self.return_type == "pandas":
            return self.df
        elif self.return_type == "list":
            return self.lst
        else:
            raise safeGraphError(f'return_type "{return_type}" does not exist')
