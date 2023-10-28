from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200", http_auth=("elastic", "OsboUfs5GKauVaoV5=dQ"))
index_name = "compensation_data"

def get_user_input():
    fields = input("Enter the fields you want to fetch (comma-separated): ")
    return fields.split(',')

def get_compensation_data(fields, query=None):
    body = {
        "_source": fields,
        "query": query or {"match_all": {}}
    }
    response = es.search(index=index_name, body=body)
    return [hit['_source'] for hit in response['hits']['hits']]

# Get user input for fields
fields_to_fetch = get_user_input()

# Fetch data based on user input
data = get_compensation_data(fields_to_fetch, query={"range": {"Salary": {"gte": 120000}}})

# Print the fetched data
for record in data:
    print(record)
