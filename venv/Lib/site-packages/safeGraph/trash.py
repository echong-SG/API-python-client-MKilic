import requests
import json
import pandas as pd
import pprint
printy =  pprint.PrettyPrinter(indent=4).pprint

url = 'https://api.safegraph.com/v1/graphql'
apikey = "x8TUW4IV3hYC1L4Xav56nChUWwtBisRY"
headers = {'Content-Type': 'application/json', 'apikey': apikey}
payload = '{"query":"query {\n  place(placekey: \"225-222@5vg-7gs-t9z\") {\n\t\tsafegraph_core {\n\t\t\tlocation_name\n\t\t\ttop_category\n\t\t\tstreet_address\n\t\t\tcity\n\t\t\tregion\n\t\t\tlatitude\n\t\t\tlongitude\n\t\t}\n\t}\n}\n"}' 
# payload = '{"query":"query {
# 	\n  place(placekey: \"225-222@5vg-7gs-t9z\") {
# 		\n\t\tsafegraph_core {\n\t\t\t
# 			location_name\n\t\t\t
# 			top_category\n\t\t\t
# 			street_address\n\t\t\t
# 			city\n\t\t\t
# 			region\n\t\t\t
# 			latitude\n\t\t\t
# 			longitude\n\t\t
# 		}\n\t
# 	}\n
# }\n"
#}' 
query = """query {
	place(placekey: "225-222@5vg-7gs-t9z") {
		safegraph_core {
			location_name
			top_category
			street_address
			city
			region
			latitude
			longitude
		}
	}
}""" 
# response = requests.get(url, headers=headers, data=payload)
response = requests.get(url, headers=headers, data={'query':query})
json_data = json.loads(response.text)
import pdb;pdb.set_trace()


import requests
query = f"""query {{
	place(placekey: "{query_pk}") {{
				placekey 
		safegraph_core {{
			location_name
			top_category
			street_address
			city
			region
			latitude
			longitude
		}}
	}}
}}"""
# response = requests.post(url, headers=headers, json={'query': query})
# json_data = json.loads(response.text)
# df_data = json_data['data']['place']['safegraph_core']
# df = pd.DataFrame.from_dict(df_data, orient="index")