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

	def place(self, placekey, return_type="pandas"):
		"""
			:param str placekey: 			Unique Placekey ID
			:param str return_type:			Desired return type ether "pandas" or "dict"
			:return:						The informations of given placekey
			:rtype:							pandas.DataFrame or dict
			XXX EXAMPLE 
				:raises ValueError: 		if placekey is not found in database
		"""
		query = gql(
			"""query($placekey: Placekey!) {
				place(placekey: $placekey) {
						placekey 
					safegraph_core {
						location_name
						top_category
						street_address
						city
						region
						latitude
						longitude
						iso_country_code
					}
				}
			}"""
		) 
		params = {"placekey": placekey}
		result = self.client.execute(query, variable_values=params)
		if return_type == "pandas":
			df = pd.DataFrame.from_dict(result['place']['safegraph_core'], orient="index")
			return df
		if return_type == "dict":
			return result['place']['safegraph_core']
		else:
			raise safeGraphError("return_type does not exist")

	def places(self, placekeys, return_type="pandas"):
		"""
			:param list placekeys: 			Unique Placekey ID inside an array
			:param str return_type:			Desired return type ether "pandas" or "dict"
			:return:						The informations of given placekeys
			:rtype:							pandas.DataFrame or dict
		"""
		query = gql(
			"""query($placekeys: [Placekey!]) {
				places(placekeys: $placekeys) {
			    	placekey
			    safegraph_core {
					location_name
					top_category
					street_address
					city
					region
					latitude
					longitude
					iso_country_code
			    }
			  }
			}"""
		) 
		params = {"placekeys": placekeys}
		result = self.client.execute(query, variable_values=params)
		result = [i['safegraph_core'] for i in result['places']]
		if return_type == "pandas":
			df = pd.DataFrame(result)
			return df
		if return_type == "dict":
			return result
		else:
			raise safeGraphError("return_type does not exist")

	def place_by_name(self, location_name, street_address, city, region, iso_country_code, return_type="pandas"):
		"""
			:param str location_name: 		location_name of the desidred place
			:param str street_address: 		street_address of the desidred place
			:param str city: 				city of the desidred place
			:param str region: 				region of the desidred place
			:param str iso_country_code: 	iso_country_code of the desidred place
			:param str return_type:			Desired return type ether "pandas" or "dict"
			:return:						The informations of given placekey
			:rtype:							pandas.DataFrame or dict
			XXX EXAMPLE 
				:raises ValueError: 		if placekey is not found in database
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
		if return_type == "dict":
			return result
		else:
			raise safeGraphError("return_type does not exist")

# print("\n\tSINGLE PULL FROM PLACEKEY\n")
# print(get_place_by_placekey(query_pk = "222-222@5qw-shj-7qz"))
# print("\n\tSINGLE PULL FROM location_name, street_address, city, region, iso_country_code\n")
# print(get_place_by_locatian_name_address(
# 	location_name= "Taco Bell", 
# 	street_address= "710 3rd St", 
# 	city= "San Francisco", 
# 	region= "CA", 
# 	iso_country_code= "US"))
# placekeys = ["224-222@5vg-7gv-d7q", "222-222@5qw-shj-7qz", "222-222@5s6-pyc-7qz", "zzy-222@5xc-k8q-zmk"]
# print(f"\n\tMULTIPLE PULL FROM PLACEKEYS: {placekeys}\n")
# print(get_places_by_placekeys(placekeys))