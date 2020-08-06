from elasticsearch import Elasticsearch


#Establish connection to database
es = Elasticsearch([{'host': 'iuni2.carbonate.uits.iu.edu','port': 9200}],
                   http_auth=('username', 'password'))
#Test Connect
if not es.ping():
    raise ValueError("Connection failed")


#Assign query to variable
query = {
    "query": {
        "ids": {
            "values": ["WOS:000188192600062"]
        }
    }
}
      

#Execute query and assign result to variable
result = es.search(
        index = 'wos',
        body = query
        )
