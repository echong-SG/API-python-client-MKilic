
import asyncio
from gql import gql
from gql import Client as gql_Client
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.websockets import WebsocketsTransport
query = """query {                                                                                                                       
 search(first: 20 after: 0 filter: {                                                                                          
 brand: "Starbucks"                                                                                                           
 address: {                                                                                                                   
 city: "Brooklyn"                                                                                                             
 }                                                                                                                            
 }) {                                                                                                                         
        placekey                                                                                                              
        safegraph_weekly_patterns (date: "2021-07-12" ) {                                                                     
                parent_placekey                                                                                               
                location_name                                                                                                 
                street_address                                                                                                
                city                                                                                                          
                region                                                                                                        
                postal_code                                                                                                   
                iso_country_code                                                                                              
                safegraph_brand_ids                                                                                           
                brands                                                                                                        
                date_range_start                                                                                              
                date_range_end                                                                                                
                raw_visit_counts                                                                                              
                raw_visitor_counts                                                                                            
                visits_by_day                                                                                                 
                visits_by_each_hour                                                                                           
                poi_cbg                                                                                                       
                visitor_home_cbgs                                                                                             
                visitor_home_aggregation                                                                                      
                visitor_daytime_cbgs                                                                                          
                visitor_country_of_origin                                                                                     
                distance_from_home                                                                                            
                median_dwell                                                                                                  
                bucketed_dwell_times                                                                                          
                related_same_day_brand                                                                                        
                related_same_week_brand                                                                                       
                device_type                                                                                                   
                }                                                                                                             
    }                                                                                                                         
}    """
query = """query { lookup(placekey: "222-224@5vg-7gr-6kz") { placekey safegraph_core { location_name street_address city region postal_code iso_country_code } } }"""
async def main():
    #query = """query { lookup(query: { location_name: "Taco Bell"  street_address: "710 3rd St"  city: "San Francisco"  region: "CA"  iso_country_code: "US"  }) {         placekey         safegraph_weekly_patterns (date: "2021-07-12" ) { location_name }     }}"""
    transport = AIOHTTPTransport(
        url='https://api.safegraph.com/v1/graphql', 
        # retries=3,
        # verify=True, 
        headers={'Content-Type': 'application/json', 'apikey': "x8TUW4IV3hYC1L4Xav56nChUWwtBisRY"})
    __query__ = gql(query) 
    async with gql_Client(
        transport=transport, fetch_schema_from_transport=True,
    ) as session:
        result = await session.execute(__query__)
    print(result)

def main_sync():
    transport = RequestsHTTPTransport(url="https://api.safegraph.com/v1/graphql", verify=True, retries=3,headers={'Content-Type': 'application/json', 'apikey': "x8TUW4IV3hYC1L4Xav56nChUWwtBisRY"})
    __query__ = gql(query) 
    session =  gql_Client(transport=transport, fetch_schema_from_transport=True)
    result = session.execute(__query__)
    print(result)
    return result

async def __main__():
    await main()

async def example_main():
    transport = AIOHTTPTransport(url="https://countries.trevorblades.com/graphql")
    # Using `async with` on the client will start a connection on the transport
    # and provide a `session` variable to execute queries on this connection
    async with gql_Client(
        transport=transport, fetch_schema_from_transport=True,
    ) as session:
        # Execute single query
        query = gql(
            """
            query getContinents {
              continents {
                code
                name
              }
            }
        """
        )
        result = await session.execute(query)
        print(result)
    return result

task = asyncio.get_event_loop().run_until_complete(example_main())
task2 = asyncio.get_event_loop().run_until_complete(main())
import pdb;pdb.set_trace()
task3 = main_sync()



# ASYNC EXPLORE
# def main():
#     import asyncio
#     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
#     asyncio.run(asyncio_test());

# async def asyncio_test():
#     #START
#     print("start for lookup")
#     start_time = time.time()
#     lookup = await sgql_client.lookup( 
#         date=date,
#         product="weekly_patterns",
#         placekeys=placekeys,
#         columns="*",
#         preview_query=False)
#     print(lookup)
#     # END
#     end_time = time.time()
#     print('Total time elapsed: %.2f seconds for lookup' % (end_time - start_time));

#     #START
#     print("start for lookup_by_name")
#     start_time = time.time()
#     look_ = await sgql_client.lookup_by_name( 
#         date=date,
#         product="weekly_patterns",
#         location_name= "Taco Bell", 
#         street_address= "710 3rd St", 
#         city= "San Francisco", 
#         region= "CA", 
#         iso_country_code= "US",
#         return_type="pandas",
#         columns="*",
#         preview_query=False)
#     print(look_)
#     # END
#     end_time = time.time()
#     print('Total time elapsed: %.2f seconds for lookup_by_name' % (end_time - start_time));

#     #START
#     print("start for search")
#     start_time = time.time()
#     search_ = await sgql_client.search( 
#         product="core", 
#         columns=["location_name"], 
#         naics_code=445120,
#         return_type="pandas", 
#         max_results=10,
#         date=date,
#     )

#     print(search_)
#     # END
#     end_time = time.time()
#     print('Total time elapsed: %.2f seconds for search' % (end_time - start_time));


# def regular_test():
#     start_time = time.time()
#     #START
#     look_ = sgql_client.lookup( 
#         date=date,
#         product="weekly_patterns",
#         placekeys=placekeys,
#         columns="*",
#         preview_query=False,
#     )

#     print(look_)
#     # END
#     end_time = time.time()
#     print('Total time elapsed: %.2f seconds for lookup' % (end_time - start_time))

#     #START
#     print("start for lookup_by_name")
#     start_time = time.time()
#     look_ = sgql_client.lookup_by_name( 
#         date=date,
#         product="weekly_patterns",
#         location_name= "Taco Bell", 
#         street_address= "710 3rd St", 
#         city= "San Francisco", 
#         region= "CA", 
#         iso_country_code= "US",
#         return_type="pandas",
#         columns="*",
#         preview_query=False)
#     print(look_)
#     # END
#     end_time = time.time()
#     print('Total time elapsed: %.2f seconds for lookup_by_name' % (end_time - start_time));

#     #START
#     print("start for search")
#     start_time = time.time()
#     search_ = sgql_client.search( 
#         product="core", 
#         columns=["location_name"], 
#         naics_code=445120,
#         return_type="pandas", 
#         max_results=10,
#         date=date,
#     )

#     print(search_)
#     # END
#     end_time = time.time()
#     print('Total time elapsed: %.2f seconds for search' % (end_time - start_time));

#main()
#regular_test()