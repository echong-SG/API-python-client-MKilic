import pandas as pd
import json, re, asyncio, sys
from gql import gql
from gql import Client as gql_Client
from gql.transport.aiohttp import AIOHTTPTransport
from .types import __VALUE_TYPES__, DATASET, INNER_DATASET, __PATTERNS__, WM__PATTERNS__
from gql.transport.exceptions import *
from datetime import datetime, timedelta
from time import sleep
if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
import nest_asyncio
nest_asyncio.apply()

class safeGraphError(Exception):
    pass

class HTTP_Client:
    def __init__(self, apikey, max_tries=3):
        self.ct = 0
        self.df = pd.DataFrame()
        self.lst = [] 
        self.url = 'https://api.safegraph.com/v1/graphql'
        self.apikey = apikey
        self.headers = {'Content-Type': 'application/json', 'apikey': apikey}
        self.return_type = "pandas"
        self.max_results = 20
        self.errors = []
        self._date = [datetime.now().strftime("%Y-%m-%d")]
        self._date_min_ = datetime.strptime("2018-01-01", '%Y-%m-%d')

    def __str__(self):
        return f"api url: {self.url} | apikey: {self.apikey}"

    @property
    def date(self):
        return self._date

    @date.setter
    def date(self, value):
        if value == "__default__":
            return
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
            raise safeGraphError(f'''*** Unrecognized DateTime! {value}\n must be either one list/str/dict''')

    async def main_runner(self, query):
        self.ct+=1
        transport = AIOHTTPTransport(url=self.url, headers=self.headers)
        async with gql_Client(
            transport=transport, fetch_schema_from_transport=True,
        ) as session:
            return await session.execute(query)

    def __change_value_types_pandas(self):
        for key, val in __VALUE_TYPES__.items():
            try:
                self.df = self.df.astype({key: val}, errors='ignore')
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


    def __dataset(self, product, columns):
        w_run_ = 1 # for weekly patterns
        query = ""
        data_type = []
        try:
            cols = columns.copy()
        except AttributeError:
            cols = columns # cols is *
        if product == "safegraph_core.*":
            # if all data from safegraph_core
            pattern_pick = __PATTERNS__["safegraph_core"]
            available_columns = (lambda x: [j for j in pattern_pick if j not in ["__header__", "__footer__"]] if x=="*" else [j for j in pattern_pick if j in cols] )(cols)#[j for j in pattern_pick if j in columns]
            if type(cols) != str:
                # XXX
                # TEST
                try:
                    cols.remove("placekey")
                except ValueError:
                    pass
                if len(available_columns) < len(cols):
                    errors = [i for i in cols if i not in pattern_pick]
                    raise safeGraphError(f"""*** [{",".join(errors)}] not available for {product}, use another query""")
            if len(available_columns) > 0:
                query += pattern_pick["__header__"] + " "
                for j in available_columns:
                    query += pattern_pick[j] + " "
                query += pattern_pick["__footer__"] + " "
                # query += __PATTERNS__["safegraph_core"][j] + " "
            else:
                raise safeGraphError("*** picked column(s) not available")
            data_type = ["safegraph_core"]
        elif product == "safegraph_geometry.*":
            # if all data from safegraph_geometry
            pattern_pick = __PATTERNS__["safegraph_geometry"]
            available_columns = (lambda x: [j for j in pattern_pick if j not in ["__header__", "__footer__"]] if x=="*" else [j for j in pattern_pick if j in cols] )(cols)#[j for j in pattern_pick if j in columns]
            if type(cols) != str:
                try:
                    cols.remove("placekey")
                except ValueError:
                    pass
                if len(available_columns) < len(cols):
                    errors = [i for i in cols if i not in pattern_pick]
                    raise safeGraphError(f"""*** [{",".join(errors)}] not available for {product}, use another query""")
            if len(available_columns) > 0:
                query += pattern_pick["__header__"] + " "
                for j in available_columns:
                    query += pattern_pick[j] + " "
                query += pattern_pick["__footer__"] + " "
                # query += __PATTERNS__['safegraph_geometry'][j] + " "
            else:
                raise safeGraphError("*** picked column(s) not available")
            data_type = ["safegraph_geometry"]
        elif product == "safegraph_monthly_patterns.*":
            # if all data from safegraph_monthly_patterns
            pattern_pick = __PATTERNS__["safegraph_monthly_patterns"]
            available_columns = (lambda x: [j for j in pattern_pick if j not in ["__header__", "__footer__"]] if x=="*" else [j for j in pattern_pick if j in cols] )(cols)#[j for j in pattern_pick if j in columns]
            if type(cols) != str:
                try:
                    cols.remove("placekey")
                except ValueError:
                    pass
                if len(available_columns) < len(cols):
                    errors = [i for i in cols if i not in pattern_pick]
                    raise safeGraphError(f"""*** [{",".join(errors)}] not available for {product}, use another query""")
            if len(available_columns) > 0:
                query += pattern_pick["__header__"] + " "
                for j in available_columns:
                    query += pattern_pick[j] + " "
                query += pattern_pick["__footer__"] + " "
                # query += __PATTERNS__['safegraph_monthly_patterns'][j] + " "
            else:
                raise safeGraphError("*** picked column(s) not available")
            data_type = ["safegraph_monthly_patterns"]
        elif product == "safegraph_weekly_patterns.*":
            # if all data from safegraph_weekly_patterns
            pattern_pick = __PATTERNS__["safegraph_weekly_patterns"]
            available_columns = (lambda x: [j for j in pattern_pick if j not in ["__header__", "__footer__"]] if x=="*" else [j for j in pattern_pick if j in cols] )(cols)#[j for j in pattern_pick if j in columns]
            if type(cols) != str:
                try:
                    cols.remove("placekey")
                except ValueError:
                    pass
                if len(available_columns) < len(cols):
                    errors = [i for i in cols if i not in pattern_pick]
                    raise safeGraphError(f"""*** [{",".join(errors)}] not available for {product}, use another query""")
            if len(available_columns) > 0:
                query += pattern_pick["__header__"] + " "
                for j in available_columns:
                    query += pattern_pick[j] + " "
                query += pattern_pick["__footer__"] + " "
                # query += __PATTERNS__['safegraph_weekly_patterns'][j] + " "
            else:
                raise safeGraphError("*** picked column(s) not available")
            data_type = ["safegraph_weekly_patterns"]
        else:
            raise ValueError("""*** product argument must be one of the following string: 
                core, geometry, monthly_patterns, weekly_patterns
            """)
        return query, data_type

    def __dataset_WM(self, product, columns):
        data_type = []
        query = ""
        product = product.rstrip(".*")
        pattern_pick = __PATTERNS__[product]
        available_columns = (lambda x: [j for j in pattern_pick if j not in ["__header__", "__header_dateless__",  "__footer__"]] if x=="*" else [j for j in pattern_pick if j in columns] )(columns)
        if product == 'safegraph_weekly_patterns' and self.date == [datetime.now().strftime("%Y-%m-%d")]:
            query += pattern_pick["__header_dateless__"] + " "
        else:
            query += pattern_pick["__header__"] + " "
        for j in available_columns:
            query += pattern_pick[j] + " "
        query += pattern_pick["__footer__"] + " "
        data_type = [product]
        return query, data_type

    def __adjustments(self, data_frame):
        self.lst = data_frame
        self.__change_value_types_lst()
        self.__none_found_delete()
        self.df = pd.DataFrame.from_dict(self.lst)
        self.__change_value_types_pandas()

    def __none_found_delete(self):
        """If element of a column from query's value is null, deletes that column
                and keeps the rest"""
        for l in reversed(range(len(self.lst))):
            count = 0
            for v in self.lst[l].keys():
                if self.lst[l][v] == None:
                    count+=1
                    continue
            if count >= len(self.lst[l])-1:
                self.lst.pop(l)

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
        try:
            lst = [i for i in range(self.max_results)]
        except TypeError:
            raise safeGraphError("|max_results argument|*** TypeError: 'str' object cannot be interpreted as an integer ")
        n = 500
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def __date_setter(self, date):
        if date != "__default__":
            self.date = date
        elif date == "__default__":
            self.date = [datetime.now().strftime("%Y-%m-%d")]

    def __prettyfy(self, __query__) -> None:
        __pick__ = False # picks the next value without additional operation; for first check
        pre = 0
        str_ = ""
        __split__ = re.sub(r'\s+', " ", __query__).split(" ");
        print(re.sub(r'\n+', '\n', __query__))

    def save(self, path="__default__", return_type="__default__"):
        """
            :param str path:                  (optional) location of the file e.g: "results.csv"
                saves as a .json file if return_type was "list" 
                saves as a .csv file if return_type was "pandas"
                if path is not given saves to current location as results.csv or results.json
            :param str return_type:          (optional) pandas or list 
                return type of the saved format by default last return format
        """
        if return_type != "__default__":
            self.return_type = return_type
        if self.return_type == "pandas":
            if path != "__default__":
                self.df.to_csv(path_or_buf=path, index=False)
            else:
               self.df.to_csv(path_or_buf="results.csv", index=False) 
        elif self.return_type == "list":
            if path != "__default__":
                json_file = open(path, 'w')
                json.dump(self.lst, json_file, indent=4)
                json_file.close()
            else:
                json_file = open("results.json", 'w')
                json.dump(self.lst, json_file, indent=4)
                json_file.close()
        else:
            raise safeGraphError("*** unknown return_type should be a string, either pandas or list")

    def sg_merge(self, datasets:list, how:str="outer", return_type:str="pandas"):
        """
            :param list datasets:           a list of dataframes or jsons
            :param str how:                 (optional) join style either outer or inner       
            :param str return_type:         (optional) return type either "pandas" or "list"; default "pandas"       
            :return:                        The data of given return_type
            :rtype:                         pandas.DataFrame or list
        """
        df = datasets[0]
        data_type = type(df)
        if data_type == pd.DataFrame:
            df_cols = list(df.columns)
            df_cols.remove('placekey')
            for i in datasets[1:]:
                if type(i) != data_type:
                    raise safeGraphError(f"*** each datasets' type must be the same: {data_type}, cannot be {type(i)}")
                try:
                    drop_cols = [col for col in i.columns if col in df_cols]
                    i = i.drop(drop_cols, axis=1)
                    df = df.merge(i, how=how)
                except pd.errors.MergeError as e:
                    print(e)
                except TypeError:
                    print("*** weekly_patterns/monthly_patterns cannot be merged for the current page: TOFIX")
            self.__adjustments(df.to_dict("records"))
        elif data_type == list:
            # change arr's values otherwise
            r_dataset = []
            for i in range(len(datasets)):
                if type(datasets[i]) != data_type:
                    raise safeGraphError(f"*** each datasets' type must be the same:{data_type}, cannot be {type(i)}")
                r_dataset.append(pd.DataFrame.from_dict(datasets[i]))
            return self.sg_merge(r_dataset, how=how, return_type="list")


            for i in datasets[1:]:
                df = df + i
            self.__adjustments(df)  
        if return_type == "pandas":      
            return df
        else:
            return df.to_dict("records")

    def lookup(self, product:str, placekeys:list, columns, date='__default__', preview_query:bool=False, return_type:str="pandas"):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        if type(placekeys) == str:
            raise "placekeys cannot be string: list required"
        results = loop.run_until_complete(self.lookup_(product, placekeys, columns, date, preview_query, return_type, cache=True, results=[]))
        if preview_query:
            return
        arr = [self.main_runner(query) for query in results]
        results = asyncio.gather(*arr, return_exceptions=True)
        report = loop.run_until_complete(results)
        ret = loop.run_until_complete(self.lookup_(product, placekeys, columns, date, preview_query, return_type, cache=False, results=report))
        loop.close()
        return ret

    async def lookup_(self, product:str, placekeys:list, columns, date='__default__', preview_query:bool=False, return_type:str="pandas", cache=True, results=[]):
        """
            :param str product:             spesific product to pull data from either one core/geometry/weekly_patterns/monthly_patterns
            :param list placekeys:          Unique Placekey ID/IDs inside an array
                [ a single placekey string or a list of placekeys are both acceptable ]
            :param str return_type:         (optional) pandas or list
                default -> pandas
            :param columns:                 list or str 
                "*" as string for all or desired column(s) in a [list]
            :param date:                    (optional) "YYYY-MM-DD", if not given using today's date
                list    => ["YYYY-MM-DD", "YYYY-MM-DD", "YYYY-MM-DD"]
                string  => "YYYY-MM-DD"
                dict    => {'date_range_start': "YYYY-MM-DD", 'date_range_end': "YYYY-MM-DD"}
            :return:                        The data of given placekeys in return_type
            :rtype:                         pandas.DataFrame or dict
        """
        self.__date_setter(date)
        self.return_type = return_type
        product = f"safegraph_{product}.*"
        params = {"placekeys": placekeys}
        # save non weekly and monthly pattern first then the rest
        first_run = 1 # for the first pull, pull all data the rest only weekly
        data_frame = []
        count = 0
        for i in self.date:
            if first_run:
                dataset, data_type = self.__dataset(product, columns)
                first_run = 0 
            elif product not in WM__PATTERNS__:
                continue
            else:
                dataset, data_type = self.__dataset_WM(product, columns)
                if dataset == "":
                    continue
            if date == '__default__':
                dataset = dataset.replace("_DATE_BLOCK_", "")
            else:
                dataset = dataset.replace("_DATE_BLOCK_", f'(date: "{i}" )')
            __query__ = f"""query {{
 batch_lookup(placekeys: {str(placekeys).replace("'", '"')}) {{
    placekey
    {dataset}
    }}
}}"""
            if preview_query:
                self.__prettyfy(__query__)
                return
            if cache:
                query = gql(__query__)
                results.append(query)
                continue
            #################################################################
            # async with gql_Client(
            #     transport=self.transport, fetch_schema_from_transport=True,
            # ) as session:
            #     result = await session.execute(query, variable_values=params)
            #################################################################
            try:
                result = results[count] # result = await asyncio.wait_for(self.client.execute_async(query), timeout=30) # variable_values=params
                count+=1
            except TransportServerError:
                print("*** TransportServerError => self.date:", self.date)
                self.__adjustments(data_frame)
                if self.return_type == "pandas":
                    return self.df
                elif self.return_type == "list":
                    return self.lst
                else:
                    raise safeGraphError(f'return_type "{return_type}" does not exist')

            for place in result['batch_lookup']:
                dict_ = {}
                for j in data_type:
                    print(place)
                    try:
                        dict_.update(place[j])
                        dict_['placekey'] = (place['placekey'])
                    except (TypeError, KeyError):
                        import pdb;pdb.set_trace()
                        # 'safegraph_weekly_patterns': None
                        pass
                data_frame.append(dict_)
        if cache:
            return results
        self.__adjustments(data_frame)
        if self.return_type == "pandas":
            return self.df
        elif self.return_type == "list":
            return self.lst
        else:
            raise safeGraphError(f'return_type "{return_type}" does not exist')

    def lookup_by_name(self,product,columns,location_name=None,street_address=None,city=None,region=None,iso_country_code=None,postal_code=None,latitude=None,longitude=None,date='__default__',preview_query:bool=False,return_type='pandas'):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(self.lookup_by_name_(product,columns,location_name,street_address,city,region,iso_country_code,postal_code,latitude,longitude,date,preview_query,return_type, cache=True, results=[]))
        if preview_query:
            return
        arr = [self.main_runner(query) for query in results]
        results = asyncio.gather(*arr, return_exceptions=True)
        report = loop.run_until_complete(results)
        ret = loop.run_until_complete(self.lookup_by_name_(product,columns,location_name,street_address,city,region,iso_country_code,postal_code,latitude,longitude,date,preview_query,return_type, cache=False, results=report))
        loop.close()
        return ret

    async def lookup_by_name_(self, product:str, columns:list,
            location_name:str=None, 
            street_address:str=None, 
            city:str=None, 
            region:str=None,
            iso_country_code:str=None, 
            postal_code:str=None,
            latitude:float=None, 
            longitude:float=None, 
            date='__default__',
            preview_query:bool=False,
            return_type:str="pandas", cache=True, results=[]):
        """
            :param str product:             spesific product to pull data from either one core/geometry/weekly_patterns/monthly_patterns
            :param columns:                 list or str 
                "*" as string for all or desired column(s) in a list
            :param date:                    (optional) "YYYY-MM-DD", if not given using today's date
                list    => ["YYYY-MM-DD", "YYYY-MM-DD", "YYYY-MM-DD"]
                string  => "YYYY-MM-DD"
                dict    => {'date_range_start': "YYYY-MM-DD", 'date_range_end': "YYYY-MM-DD"}
            :param str location_name:       (optional) location_name of the desidred lookup
            :param str street_address:      (optional) street_address of the desidred lookup
            :param str city:                (optional) city of the desidred lookup
            :param str region:              (optional) region of the desidred lookup
            :param str iso_country_code:    (optional) iso_country_code of the desidred lookup
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
        self.__date_setter(date)
        self.return_type = return_type
        product = f"safegraph_{product}.*"
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
        first_run = 1
        data_frame = []
        count = 0
        for i in self.date:
            if first_run:
                dataset, data_type = self.__dataset(product, columns)
                first_run = 0 
            elif product not in WM__PATTERNS__:
                continue
            else:
                dataset, data_type = self.__dataset_WM(product, columns)
                if dataset == "":
                    continue
            if date == '__default__':
                dataset = dataset.replace("_DATE_BLOCK_", "")
            else:
                dataset = dataset.replace("_DATE_BLOCK_", f'(date: "{i}" )')
            __query__ = f"""query {{
 lookup(query: {{{params}
 }}) {{ 
        placekey 
        {dataset}
    }}
}}"""
            if preview_query:
                self.__prettyfy(__query__)  
                return
            if cache:
                query = gql(__query__)
                results.append(query)
                continue
            #################################################################
            # async with gql_Client(
            #     transport=self.transport, fetch_schema_from_transport=True,
            # ) as session:
            #     result = await session.execute(query)
            #################################################################
            try:
                result = results[count] # result = await asyncio.wait_for(self.client.execute_async(query), timeout=30)
                count+=1
            except TransportServerError:
                self.__adjustments(data_frame)
                if self.return_type == "pandas":
                    return self.df
                elif self.return_type == "list":
                    return self.lst
                else:
                    raise safeGraphError(f'return_type "{return_type}" does not exist')
            dict_ = {}
            for j in data_type:
                try:
                    dict_.update(result['lookup'][j])
                except (TypeError, KeyError):
                    # 'safegraph_weekly_patterns': None
                    pass
            try:
                dict_["placekey"] = result['lookup']['placekey']
            except TypeError:
                raise safeGraphError("*** query didnt match with an item")
            data_frame.append(dict_)
        if cache:
            return results
        self.__adjustments(data_frame)
        if self.return_type == "pandas":
            return self.df
        elif self.return_type == "list":
            return self.lst
        else:
            raise safeGraphError(f'return_type "{return_type}" does not exist')

    def search(self, product, columns, date='__default__',brand:str=None,brand_id:str=None,naics_code:int=None, phone_number:str=None,location_name:str=None, street_address:str=None, city:str=None, region:str=None, postal_code:str=None, iso_country_code:str=None,max_results:int=20,after_result_number:int=0,preview_query:bool=False,return_type:str="pandas"):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results = loop.run_until_complete(self.search_(product,columns,date,brand,brand_id,naics_code,phone_number,location_name,street_address,city,region,postal_code,iso_country_code,max_results,after_result_number,preview_query,return_type, cache=True, results=[]))
        if preview_query:
            return
        arr = [self.main_runner(query) for query in results]
        results = asyncio.gather(*arr, return_exceptions=True)
        report = loop.run_until_complete(results)
        return loop.run_until_complete(self.search_(product,columns,date,brand,brand_id,naics_code,phone_number,location_name,street_address,city,region,postal_code,iso_country_code,max_results,after_result_number,preview_query,return_type, cache=False, results=report))

    async def search_(self, product, columns, date='__default__',
        # params
        brand:str=None, brand_id:str=None, naics_code:int=None, phone_number:str=None,
        # address with following sub-fields
        location_name:str=None, street_address:str=None, city:str=None, region:str=None, postal_code:str=None, iso_country_code:str=None,
        max_results:int=20,
        after_result_number:int=0,
        preview_query:bool=False,
        return_type:str="pandas", cache=True, results=[]):
        """
            :param str product:             spesific product to pull data from either one core/geometry/weekly_patterns/monthly_patterns
            :param columns:                 list or str 
                "*" as string for all or desired column(s) in a list
            :param date:                    (optional) "YYYY-MM-DD", if not given using today's date
                list    => ["YYYY-MM-DD", "YYYY-MM-DD", "YYYY-MM-DD"]
                string  => "YYYY-MM-DD"
                dict    => {'date_range_start': "YYYY-MM-DD", 'date_range_end': "YYYY-MM-DD"}
            :param str brand:               brand for searching query
            :param str brand_id:            brand_id for searching query
            :param int naics_code:          naics_code for searching query
            :param str phone_number:        phone_number for searching query
            :param str location_name:       location_name for searching query
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
        """
        ############
        self.return_type = return_type
        self.__date_setter(date)
        ############
        self.max_results = max_results
        product = f"safegraph_{product}.*"
        params = f"""
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("brand", brand)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("brand_id", brand_id)}
{(lambda x,y: f' {x}: {int(y)} ' if y!=None else "")("naics_code", naics_code)}
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("phone_number", phone_number)}
"""
        address = f""" address: {{
{(lambda x,y: f' {x}: "{y}" ' if y!=None else "")("location_name", location_name)}
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
        arr = []
        _max_after = False
        count = 0
        for chu in chunks:
            if _max_after:
                break
            first = len(chu)
            first_run = 1 # for the first pull, pull all data the rest only weekly
            for i in self.date:
                if first_run:
                    dataset, data_type = self.__dataset(product, columns)
                    first_run = 0 
                elif product not in WM__PATTERNS__:
                    continue
                else:
                    dataset, data_type = self.__dataset_WM(product, columns)
                    if dataset == "":
                        continue
                if date == '__default__':
                    dataset = dataset.replace("_DATE_BLOCK_", "")
                else:
                    dataset = dataset.replace("_DATE_BLOCK_", f'(date: "{i}" )')
                __query__ = f"""query {{
 search(first: {first} after: {after+after_result_number} filter: {{{params}   
 }}) {{ 
        placekey 
        {dataset}
    }}
}}"""
                if preview_query:
                    self.__prettyfy(__query__) 
                    return
                if cache:
                    query = gql(__query__)
                    results.append(query)
                    continue
                try:
                    result = results[count]#await self.client.execute_async(query)
                    count+=1
                    result['search'][0]['placekey']
                except IndexError:
                    self.errors.append(f"{after+after_result_number}-{first+after+after_result_number}")
                    self.__lengthCheck__(data_frame)
                    self.__adjustments(data_frame)
                    self.__error_check(after_result_number)
                    if self.return_type == "pandas":
                        return self.df
                    elif self.return_type == "list":
                        return self.lst
                    else:
                        raise safeGraphError(f'return_type "{return_type}" does not exist')
                except TransportServerError:
                    print("*** TransportServerError")
                    self.errors.append(f"{after+after_result_number}-{first+after+after_result_number}")
                    self.__lengthCheck__(data_frame)
                    self.__adjustments(data_frame)
                    self.__error_check(after_result_number)
                    print(self.return_type)
                    if self.return_type == "pandas":
                        return self.df
                    elif self.return_type == "list":
                        return self.lst
                    else:
                        raise safeGraphError(f'return_type "{return_type}" does not exist')
                except Exception as e:
                    try:
                        if result.errors[0]['message'].startswith("Maximum 'after' value exceeded"):
                            _max_after = True
                            break
                    except:
                        pass
                    for err in e.errors:
                        if err["message"] == "Maximum 'after' value exceeded, use 'last_seen_placekey' to get next page. (max_offset=5000)":
                            # {'message': "Maximum 'after' value exceeded, use 'last_seen_placekey' to get next page. (max_offset=5000)", 'path': ['search'], 'locations': [{'line': 2, 'column': 3}]}
                            _max_after = True
                            break
                try:
                    output+=result['search']
                except UnboundLocalError:
                    break
                after += first
            for out in output:
                dict_ = {}
                for j in data_type:
                    try:
                        dict_.update(out[j])
                        dict_['placekey'] = (out['placekey'])
                    except (TypeError, KeyError):
                        pass
                data_frame.append(dict_)
        if cache:
            return results
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

    def get_patterns_supplemental(self, file:str, # home_panel_summary, normalization_stats, and visit_panel_summary
        region:str, iso_country_code:str, date="__default__", 
        max_hps_results:str='all', # This is only relevant when file = 'home_panel_summary'
        after_hps_result:int=0, # This is only relevant when file = 'home_panel_summary'
        preview_query:bool=False,):
        pass

