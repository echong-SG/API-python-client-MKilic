# import requests
# import json
# import pandas as pd
# import pprint
# printy =  pprint.PrettyPrinter(indent=4).pprint

# url = 'https://api.safegraph.com/v1/graphql'
# apikey = "x8TUW4IV3hYC1L4Xav56nChUWwtBisRY"
# headers = {'Content-Type': 'application/json', 'apikey': apikey}
# payload = '{"query":"query {\n  place(placekey: \"225-222@5vg-7gs-t9z\") {\n\t\tsafegraph_core {\n\t\t\tlocation_name\n\t\t\ttop_category\n\t\t\tstreet_address\n\t\t\tcity\n\t\t\tregion\n\t\t\tlatitude\n\t\t\tlongitude\n\t\t}\n\t}\n}\n"}' 
# # payload = '{"query":"query {
# # 	\n  place(placekey: \"225-222@5vg-7gs-t9z\") {
# # 		\n\t\tsafegraph_core {\n\t\t\t
# # 			location_name\n\t\t\t
# # 			top_category\n\t\t\t
# # 			street_address\n\t\t\t
# # 			city\n\t\t\t
# # 			region\n\t\t\t
# # 			latitude\n\t\t\t
# # 			longitude\n\t\t
# # 		}\n\t
# # 	}\n
# # }\n"
# #}' 
# query = """query {
# 	place(placekey: "225-222@5vg-7gs-t9z") {
# 		safegraph_core {
# 			location_name
# 			top_category
# 			street_address
# 			city
# 			region
# 			latitude
# 			longitude
# 		}
# 	}
# }""" 
# # response = requests.get(url, headers=headers, data=payload)
# response = requests.get(url, headers=headers, data={'query':query})
# json_data = json.loads(response.text)
# import pdb;pdb.set_trace()


# import requests
# query = f"""query {{
# 	place(placekey: "{query_pk}") {{
# 				placekey 
# 		safegraph_core {{
# 			location_name
# 			top_category
# 			street_address
# 			city
# 			region
# 			latitude
# 			longitude
# 		}}
# 	}}
# }}"""
# # response = requests.post(url, headers=headers, json={'query': query})
# # json_data = json.loads(response.text)
# # df_data = json_data['data']['place']['safegraph_core']
# # df = pd.DataFrame.from_dict(df_data, orient="index")


#         query = gql(
#             """query($placekeys: [Placekey!]) {
#                 places(placekeys: $placekeys) {
#                     placekey
#                 safegraph_core {
#                     location_name
#                     top_category
#                     street_address
#                     city
#                     region
#                     postal_code
#                     latitude
#                     longitude
#                     iso_country_code
#                 }
#                 safegraph_geometry{
#                     location_name
#                     top_category
#                     street_address
#                     city
#                     region
#                     postal_code
#                     latitude
#                     longitude
#                     polygon_wkt
#                 }
#                 safegraph_patterns {
#                     date_range_start
#                     date_range_end
#                     median_dwell
#                     bucketed_dwell_times {
#                         key
#                         value
#                     }
#                     popularity_by_day {
#                         key
#                         value
#                     }
#                     visits_by_day
#                     poi_cbg
#                 }
#               }
#             }"""
#         ) 


#     def place(self, placekey, return_type="pandas", columns=["*"]):
#         """
#             :param str placekey:            Unique Placekey ID
#             :param str return_type:         Desired return type ether "pandas" or "list"
#             :param list columns:            ["*"] for all or desired column in dataframe
#             :return:                        The data of given placekey in return_type
#             :rtype:                         pandas.DataFrame or dict
#         """
#         params = {"placekey": placekey}
#         dataset, data_type = self.__dataset__(columns)
#         query = gql(
#             f"""query($placekey: Placekey!) {{
#                 place(placekey: $placekey) {{
#                         placekey 
#                     {dataset}
#                 }}
#             }}"""
#         ) 
#         result = self.client.execute(query, variable_values=params)
#         data_frame = []
#         dict_ = {}
#         for j in data_type:
#             dict_.update(result['place'][j])
#         data_frame.append(dict_)

#         # adjustments
#         self.__adjustments(data_frame)

#         if return_type == "pandas":
#             # df = pd.DataFrame.from_dict(data_frame)
#             # self.df = df
#             # self.__change_value_type_pandas()
#             return self.df
#         elif return_type == "list":
#             # self.lst = data_frame
#             return self.lst
#         else:
#             raise safeGraphError(f'return_type "{return_type}" does not exist')


#         #import pdb;pdb.set_trace()
#         #pass
#         """
#             query {
#               search(filter: {
#                 naics_code: 445120
#               }) {
#                 placekey
#                 safegraph_core {
#                   location_name
#                   street_address
#                   city
#                   region
#                   iso_country_code
#                 }
#               }
#             }
#         """

#         # Staggered Search
#         """
#             query {
#               search(first: 15 after: 20 filter: {
#                 brand: "starbucks"
#                 }) {
#                 placekey
                
#                 safegraph_core {
#                   location_name
#                   street_address
#                   city
#                   region
#                   iso_country_code
#                   latitude
#                   longitude
#                   brands {
#                     brand_id
#                     brand_name
#                   }
#                 }
#                 safegraph_patterns {
#                   date_range_start
#                   date_range_end
#                   raw_visit_counts
#                 }
#               }
#             }
#         """
#         # Search by Multiple Attributes
#         """
#             query {
#               search(filter: {
#                 brand: "Starbucks"
#                 address:{
#                   city: "San Francisco"
#                 }
#               }) {
#                 placekey
#                 safegraph_core {
#                   location_name
#                   street_address
#                   city
#                   region
#                   iso_country_code
#                 }
#               }
#             }
#         """
#         # Search for Multiple Values Per Attribute Using Variables
#         # https://docs.safegraph.com/reference/places-api-examples#section-search-for-multiple-values-per-attribute-using-variables
#         """
#             query SearchByRegionAndNaics($region: String! $naics: Int!){
#               search(filter: { 
#                 naics_code: $naics
#                 address: {
#                   region: $region
#                 }
#               })
#             {
#                 placekey
#                 safegraph_core{
#                   location_name
#                   top_category
#                   sub_category
#                   naics_code
#                 }
#                 safegraph_geometry{
#                   street_address
#                   region
#                   postal_code
#                     polygon_wkt
#                 }
#               }
#             }
#         """