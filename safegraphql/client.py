import pandas as pd
import json
from gql import gql
from gql import Client as gql_Client
from gql.transport.requests import RequestsHTTPTransport
from .types import __VALUE_TYPES__, DATASET, INNER_DATASET, __PATTERNS__, WM__PATTERNS__
from requests.exceptions import *  
from datetime import datetime, timedelta
from time import sleep
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
        self.max_results = 20
        self.errors = []
        self._date = [datetime.now().strftime("%Y-%m-%d")]
        self.patterns_version = "monthly"
        self._date_min_ = datetime.strptime("2018-01-01", '%Y-%m-%d')

    def __str__(self):
        return f"api url: {self.url} | apikey: {self.apikey}"

    @property
    def date(self):
        return self._date

    @date.setter
    def date(self, value):
        self._round_date(value)

    def _round_date(self, value):
        self._date = []
        if type(value) == list:
            for i in range(len(value)):
                d1 = datetime.strptime(value[i], '%Y-%m-%d')
                start_of_week = d1 - timedelta(days=d1.weekday())
                if start_of_week < self._date_min_:
                    raise safeGraphError(f'''*** DateTime {d1} cannot be smaller than {self._date_min_}''')
                self._date.append(start_of_week.strftime("%Y-%m-%d"))
            self._date = list(set(self._date)) # drops duplicates
            self._date.sort()
        elif type(value) == str:
            d1 = datetime.strptime(value, '%Y-%m-%d')
            start_of_week = d1 - timedelta(days=d1.weekday())
            if start_of_week < self._date_min_:
                raise safeGraphError(f'''*** DateTime {d1} cannot be smaller than {self._date_min_}''')
            self._date = [start_of_week.strftime("%Y-%m-%d")]
        elif type(value) == dict:
            d1 = datetime.strptime(value['date_range_start'], "%Y-%m-%d")
            d2 = datetime.strptime(value['date_range_end'], "%Y-%m-%d")
            start_of_week1 = d1 - timedelta(days=d1.weekday())
            if start_of_week1 < self._date_min_:
                raise safeGraphError(f'''*** DateTime {d1} cannot be smaller than {self._date_min_}''')
            start_of_week2 = d2 - timedelta(days=d2.weekday())
            week_loop = int(abs((start_of_week1 - start_of_week2).days / 7))  
            for i in range(1, week_loop+1):
                self._date.append((start_of_week1 + timedelta(days=7*i)).strftime("%Y-%m-%d"))
        else:
            # XXX
            # better explain
            raise safeGraphError(f'''*** Unrecognized DateTime! {value}\n must be either list/str/dict''')

    def __change_value_types_pandas(self):
        for key, val in __VALUE_TYPES__.items():
            try:
                self.df = self.df.astype({key: val}) # errors='ignore')
            except KeyError:
                pass

    def __change_value_types_lst(self):
        for key, val in __VALUE_TYPES__.items():
            # XXX
            # because of weekly patterns not useing need testing
            # if not key in self.lst[0].keys():
            #     # if not in first element of list not in other pairs
            #     continue
            for lst in self.lst:
                try:
                    lst[key] = val(lst[key])
                except KeyError:
                    pass

    def save(self, path="__default__", return_type="__default__"):
        """
            :param str path:                 (optional) location of the file e.g: "results.csv"
                saves as a .json file if return_type was "list" 
                saves as a .csv file if return_type was "pandas"
                if path is not given saves to current location as results.csv or results.json
            :param str return_type:          (optional) pandas or list 
                return type of the saved format by default last return format
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

        invalid_values = []
        monthly_only = set(__PATTERNS__['safegraph_monthly_patterns'].keys()).difference(__PATTERNS__['safegraph_weekly_patterns'])
        monthly_only_arr = [el for el in columns if el in monthly_only]
        weekly_only = set(__PATTERNS__['safegraph_weekly_patterns'].keys()).difference(__PATTERNS__['safegraph_monthly_patterns'])
        weekly_only_arr = [el for el in columns if el in weekly_only]
        if len(monthly_only_arr) > 0 and len(weekly_only_arr) > 0:
            invalid_values = [f"\n\t{i} is only in safegraph_monthly_patterns" for i in monthly_only_arr] + [f"\n\t{i} is only in safegraph_weekly_patterns" for i in weekly_only_arr]

        if len(invalid_values) > 1:
            raise ValueError(f'''
    Invalid column name(s): {' and '.join(invalid_values)}
    Those cannot be used in a single search
            ''')


    def __dataset(self, columns):
        w_run_ = 1 # for weekly patterns
        query = ""
        data_type = []
        data_pull = [i.rstrip(".*") for i in columns if i in INNER_DATASET]
        if columns == "*":
            # if all data from all datasets wanted
            if self.patterns_version == "weekly":
                __PATTERNS__arr = ["safegraph_weekly_patterns", "safegraph_core", "safegraph_geometry"]
            else:
                __PATTERNS__arr = ["safegraph_monthly_patterns", "safegraph_core", "safegraph_geometry"]
            for i in __PATTERNS__arr:
                for j in __PATTERNS__[i]:
                    query += __PATTERNS__[i][j] + " "
            data_type = __PATTERNS__arr
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
        elif columns == "safegraph_monthly_patterns.*":
            # if all data from safegraph_monthly_patterns
            for j in __PATTERNS__["safegraph_monthly_patterns"]:
                query += __PATTERNS__['safegraph_monthly_patterns'][j] + " "
            data_type = ["safegraph_monthly_patterns"]
        elif columns == "safegraph_weekly_patterns.*":
            # if all data from safegraph_weekly_patterns
            for j in __PATTERNS__["safegraph_weekly_patterns"]:
                query += __PATTERNS__['safegraph_weekly_patterns'][j] + " "
            data_type = ["safegraph_weekly_patterns"]
        elif type(columns) != list:
            raise ValueError("""*** columns argument must to be a list or one of the following string: 
                * , safegraph_core.* , safegraph_geometry.* , safegraph_monthly_patterns.* , safegraph_weekly_patterns.*
            """)
        elif len(data_pull) > 0:
            # if spesific dataset(s) wanted
            if "safegraph_monthly_patterns" in data_pull and "safegraph_weekly_patterns.*" in data_pull:
                raise ValueError("""*** Please select columns from only one version of Patterns - weekly or monthly""")
            for i in data_pull:
                for j in __PATTERNS__[i]:
                    query += __PATTERNS__[i][j] + " "
                data_type.append(i)
        else:
            # if spesific column(s) wanted
            self.__column_check_raise(columns)
            for i in DATASET:
                if i == "safegraph_monthly_patterns" and self.patterns_version == "weekly":
                    continue
                elif i == "safegraph_weekly_patterns" and self.patterns_version == "montly":
                    continue
                available_columns = [j for j in __PATTERNS__[i] if j in columns]
                if len(available_columns) > 0:
                    data_type.append(i)
                    query += __PATTERNS__[i]["__header__"] + " "
                    for j in available_columns:
                        query += __PATTERNS__[i][j] + " "
                    query += __PATTERNS__[i]["__footer__"] + " "
                columns = [i for i in columns if i not in available_columns]
        return query, data_type

    def __dataset_WM(self, columns):
        data_pull = [i.rstrip(".*") for i in columns if i.rstrip(".*") in WM__PATTERNS__]
        data_type = []
        query = ""
        # if columns == "safegraph_monthly_patterns.*" or columns == "*" and self.patterns_version == "monthly":
        #     # if all data from safegraph_monthly_patterns
        #     for j in __PATTERNS__["safegraph_monthly_patterns"]:
        #         query += __PATTERNS__['safegraph_monthly_patterns'][j] + " "
        #     data_type = ["safegraph_monthly_patterns"]
        if self.patterns_version == "monthly":
            return "", False
        else: # self.patterns_version == "weekly"
            if columns == "safegraph_weekly_patterns.*" or columns == "*":
                # if all data from safegraph_weekly_patterns
                for j in __PATTERNS__["safegraph_weekly_patterns"]:
                    query += __PATTERNS__['safegraph_weekly_patterns'][j] + " "
                data_type = ["safegraph_weekly_patterns"]
            elif len(data_pull) > 0:
                for i in data_pull:
                    for j in __PATTERNS__[i]:
                        query += __PATTERNS__[i][j] + " "
                    data_type.append(i)
            else:
                for i in WM__PATTERNS__:
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

    def __lengthCheck__(self, data_frame):
        if len(data_frame) < 1:
            raise safeGraphError("Your search returned no results.")

    def __error_check(self, after_result_number=None):
        if len(self.errors) > 0:
            print(f'results {" and ".join([i for i in self.errors])} failed and must be re-queried')
        if after_result_number != None:
            print(after_result_number)
        self.errors = []

    def __chunks(self):
        """Yield successive n-sized chunks from self.max_results."""
        lst = [i for i in range(self.max_results)]
        n = 20
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def _date_setter(self, patterns_version, date):
        if date != "__default__" and patterns_version != "__default__":
            self.patterns_version = patterns_version
            self.date = date
        elif patterns_version != "__default__" and date == "__default__":
            raise safeGraphError('''*** date of HTTP_Client has to be set
                >>> from safegraphql import client
                >>> sgql_client = client.HTTP_Client("MY_API_KEY")
                >>> sgql_client.patterns_version="weekly"
                >>> sgql_client.date = ["2021-08-05", "2021-08-12", "2021-08-19"]
                >>> sgql_client.date = "2021-08-05"
                >>> sgql_client.date = {"date_range_start": "2021-01-05", "date_range_end": "2021-08-01"}
                >>> df = sgql_client.batch_lookup(placekeys, columns="*")
                # or
                >>> df = sgql_client.batch_lookup(placekeys, columns="*", date="2021-08-05", patterns_version="weekly")
            ''')
        elif date != "__default__" and patterns_version == "__default__":
            raise safeGraphError('''*** patterns_version of HTTP_Client has to be set to weekly in order to make date spesific calculations
                >>> from safegraphql import client
                >>> sgql_client = client.HTTP_Client("MY_API_KEY")
                >>> sgql_client.patterns_version="weekly"
                >>> sgql_client.date = ["2021-08-05", "2021-08-12", "2021-08-19"]
                >>> sgql_client.date = "2021-08-05"
                >>> sgql_client.date = {"date_range_start": "2021-01-05", "date_range_end": "2021-08-01"}
                >>> df = sgql_client.batch_lookup(placekeys, columns="*")
                # or
                >>> df = sgql_client.batch_lookup(placekeys, columns="*", date="2021-08-05", patterns_version="weekly")
            ''')


    def batch_lookup(self, placekeys, columns, date="__default__", patterns_version="__default__", return_type="pandas"):
        """
            :param list placekeys:          Unique Placekey ID/IDs inside an array
                [ a single placekey string or a list of placekeys are both acceptable ]
            :param str return_type:         (optional) pandas or list
                default -> pandas
            :param columns:                 list or str 
                "*" as string for all or desired column(s) in a [list]
            :return:                        The data of given placekeys in return_type
            :rtype:                         pandas.DataFrame or dict
        """
        self._date_setter(patterns_version, date)
        self.return_type = return_type
        params = {"placekeys": placekeys}
        # save non weekly and monthly pattern first then the rest
        first_run = 1 # for the first pull, pull all data the rest only weekly
        data_frame = []
        print(f"\n\n\n\tbatch_lookup: {columns=},{date=},{patterns_version=},{return_type=}\n\n\n")
        for i in self._date:
            print("\n\t "+i+"\n")
            if first_run:
                dataset, data_type = self.__dataset(columns)
                dataset = dataset.replace("_DATE_", f'"{i}"') 
                first_run = 0 
            else:
                dataset, data_type = self.__dataset_WM(columns)
                if dataset == "":
                    continue
                dataset = dataset.replace("_DATE_", f'"{i}"')
            print(dataset+"\n")
            query = gql(
                f"""query($placekeys: [Placekey!]) {{
                    batch_lookup(placekeys: $placekeys) {{
                        placekey
                    {dataset}
                    }}
                }}"""
            ) 
            result = self.client.execute(query, variable_values=params)
            for place in result['batch_lookup']:
                dict_ = {}
                for j in data_type:
                    try:
                        dict_.update(place[j])
                    except TypeError:
                        # 'safegraph_weekly_patterns': None
                        pass
                data_frame.append(dict_)

        # adjustments
        # self.__lengthCheck__(data_frame) # not working in this function
        self.__adjustments(data_frame)

        if self.return_type == "pandas":
            return self.df
        elif self.return_type == "list":
            return self.lst
        else:
            raise safeGraphError(f'return_type "{return_type}" does not exist')

    def lookup_by_name(self, columns,
            location_name:str=None, 
            street_address:str=None, 
            city:str=None, 
            region:str=None,
            iso_country_code:str=None, 
            postal_code:str=None,
            latitude:float=None, longitude:float=None, 
            date="__default__", patterns_version="__default__", return_type="pandas"):
        """
            :param str location_name:       location_name of the desidred lookup
            :param str street_address:      street_address of the desidred lookup
            :param str city:                city of the desidred lookup
            :param str region:              region of the desidred lookup
            :param str iso_country_code:    iso_country_code of the desidred lookup
            :param columns:                 list or str 
                "*" as string for all or desired column(s) in a [list]
            :param str return_type:         (optional) pandas or list
                default -> pandas
            :return:                        The data of given placekey in return_type
            :rtype:                         pandas.DataFrame or dict
        """
        if location_name != None and street_address != None and city != None and region != None and iso_country_code != None:
            pass
        elif location_name != None and street_address != None and postal_code != None and iso_country_code != None:
            pass
        elif location_name != None and latitude != None and longitude != None and iso_country_code != None:
            pass
        else:
            # XXX
            # TODO
            # better explain
            raise safeGraphError('''*** Not enough parameter to fill query
When querying by location & address, it's necessary to have at least the following combination of fields to return a result:
    location_name + street_address + city + region + iso_country_code
    location_name + street_address + postal_code + iso_country_code
    location_name + latitude + longitude + iso_country_code
            ''')

        self.return_type = return_type
        self._date_setter(patterns_version, date)
        params = f"""
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("location_name", location_name)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("street_address", street_address)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("city", city)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("region", region)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("iso_country_code", iso_country_code)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("postal_code", postal_code)}
{(lambda x,y: f' {x}: {float(y)} ' if y!=None else "")("latitude", latitude)}
{(lambda x,y: f' {x}: {float(y)} ' if y!=None else "")("longitude", longitude)}
"""
        # params = {
        #     "location_name": location_name, 
        #     "street_address": street_address, 
        #     "city": city, 
        #     "region": region, 
        #     "iso_country_code": iso_country_code
        # }
        first_run = 1
        data_frame = []
        print(f"\n\n\n\tlookup_by_name: {columns=},{date=},{patterns_version=},{return_type=}\n\n\n")
        for i in self._date:
            print("\n\t"+i+"\n")
            if first_run:
                dataset, data_type = self.__dataset(columns)
                dataset = dataset.replace("_DATE_", f'"{i}"') 
                first_run = 0 
            else:
                dataset, data_type = self.__dataset_WM(columns)
                if dataset == "":
                    continue
                dataset = dataset.replace("_DATE_", f'"{i}"')
            print(dataset+"\n")
            query = gql(
                f"""query {{
                    lookup(query: {{
                        {params}
                        }}) {{ 
                        placekey 
                        {dataset}
                    }}
                }}"""
            )
            result = self.client.execute(query)
            dict_ = {}
            for j in data_type:
                try:
                    dict_.update(result['lookup'][j])
                except TypeError:
                    # 'safegraph_weekly_patterns': None
                    pass
            data_frame.append(dict_)

        # adjustments
        # self.__lengthCheck__(data_frame) # not working in this function
        self.__adjustments(data_frame)

        if self.return_type == "pandas":
            return self.df
        elif self.return_type == "list":
            return self.lst
        else:
            raise safeGraphError(f'return_type "{return_type}" does not exist')

    def search(self, columns,
        brand = None, brand_id = None, naics_code = None, phone_number = None,
        # address with following sub-fields
        street_address = None, city = None, region = None, postal_code = None, iso_country_code = None,
        max_results=20,
        after_result_number=0,
        date="__default__", patterns_version="__default__",
        return_type="pandas"):
        """
            :param columns:                 list or str 
                "*" as string for all or desired column(s) in a [list]
            :param str brand:               brand for searching query
            :param str brand_id:            brand_id for searching query
            :param str naics_code:          naics_code for searching query
            :param str phone_number:        phone_number for searching query
            :param str street_address:      street_address of the desidred place
            :param str city:                city of the desidred place
            :param str region:              region of the desidred place
            :param str postal_code:         postal_code of the desidred place
            :param str iso_country_code:    iso_country_code of the desidred place
            :param int max_results:         (optional) how many result required
                default -> 20
            :param str return_type:         (optional) pandas or list
                default -> pandas
            :return:                        The data of given placekey in return_type
            :rtype:                         pandas.DataFrame or dict
        """                               ############ 
        #################################################        |```|  /\   |````|
        self.max_results = max_results    ##################     |\``  / _\  |    |
        self.return_type = return_type    ###################    | \  /    \ |____|__
        # self.errors = []                #################
        #################################################
                                          ############
        self._date_setter(patterns_version, date)
        # dataset, data_type = self.__dataset(columns)
        # dataset = dataset.replace("_DATE_" , f'''"{self._date[0]}"''')  
        params = f"""
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("brand", brand)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("brand_id", brand_id)}
{(lambda x,y: f' {x}: {int(y)} ' if y!=None else "")("naics_code", naics_code)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("phone_number", phone_number)}
"""
        address = f""" address: {{
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
        chunks = self.__chunks()
        data_frame = []
        print(f"\n\n\n\tsearch: {columns=},{date=},{patterns_version=},{return_type=}\n\n\n")
        for chu in chunks:
            first = len(chu)
            first_run = 1 # for the first pull, pull all data the rest only weekly
            data_frame = []
            for i in self._date:
                print("\n\t "+i+"\n")
                if first_run:
                    dataset, data_type = self.__dataset(columns)
                    dataset = dataset.replace("_DATE_", f'"{i}"') 
                    first_run = 0 
                else:
                    dataset, data_type = self.__dataset_WM(columns)
                    if dataset == "":
                        continue
                    dataset = dataset.replace("_DATE_", f'"{i}"')
                print(dataset+"\n")
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
                # query = gql(
                #     f"""query($placekeys: [Placekey!]) {{
                #         batch_lookup(placekeys: $placekeys) {{
                #             placekey
                #         {dataset}
                #         }}
                #     }}"""
                # ) 
                try:
                    result = self.client.execute(query)
                except Exception as e:
                    print("\n\n\n\t*** ERROR ***\n\n\n")
                    print(e)
                    #if type(e) == ConnectionError:
                    result = {'search': []}
                    self.errors.append(f"{after+after_result_number}-{first+after+after_result_number}")
                output+=result['search']
                after += first
            for out in output:
                dict_ = {}
                for j in data_type:
                    try:
                        dict_.update(out[j])
                    except TypeError:
                        # 'safegraph_weekly_patterns': None
                        pass
                dict_['placekey'] = out["placekey"]
                data_frame.append(dict_)

        self.__lengthCheck__(data_frame)
        self.__adjustments(data_frame)
        self.__error_check(after_result_number)

        if self.return_type == "pandas":
            return self.df
        elif self.return_type == "list":
            return self.lst
        else:
            raise safeGraphError(f'return_type "{return_type}" does not exist')

    def search_within_radius(self,):
        # * Argument `keyset_placekey` was added to `Query.search_within_radius` field
        query = gql(
            f"""query {{
                search_within_radius(first: {first} after: {after+after_result_number} filter: {{
                    {params}
                    }}) {{ 
                    placekey 
                    {dataset}
                }}
            }}"""
        )

    def fuzzy_search_by_city(self,):
        #  * Argument `limit` was added to `Query.fuzzy_search_by_city` field
        query = gql(
            f"""query {{
                fuzzy_search_by_city(first: {first} after: {after+after_result_number} filter: {{
                    {params}
                    }}) {{ 
                    placekey 
                    {dataset}
                }}
            }}"""
        )

