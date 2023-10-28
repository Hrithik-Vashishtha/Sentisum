from elasticsearch import Elasticsearch


def get_all_compensation():
    es = Elasticsearch(['https://localhost:9200'], basic_auth=('elastic', 'OsboUfs5GKauVaoV5=dQ'))
    index_name = "compensation_data"

    # Get all compensation records
    docs = es.search(index=index_name)['hits']['hits']

    # Convert the documents to JSON
    json_records = []
    for doc in docs:
        record = doc['_source']
        json_records.append(record)

    return json_records


def get_compensation_by_id(id):
    es = Elasticsearch(['https://localhost:9200'], basic_auth=('elastic', 'OsboUfs5GKauVaoV5=dQ'))
    index_name = "compensation_data"

    # Get the compensation record by ID
    doc = es.get(index=index_name, id=id)

    # If the record does not exist, return 404
    if not doc['_found']:
        return {'error': 'Compensation record not found'}

    # Convert the document to JSON
    record = doc['_source']

    return record


def search_compensation(query=None, sort_by=None):
    es = Elasticsearch(['https://localhost:9200'], basic_auth=('elastic', 'OsboUfs5GKauVaoV5=dQ'))
    index_name = "compensation_data"

    # Build the Elasticsearch query
    es_query = {}
    if query is not None:
        es_query['query'] = {'match': {'_all': query}}
    if sort_by is not None:
        es_query['sort'] = [sort_by]

    # Execute the Elasticsearch query
    docs = es.search(index=index_name, body=es_query)['hits']['hits']

    # Convert the documents to JSON
    json_records = []
    for doc in docs:
        record = doc['_source']
        json_records.append(record)

    return json_records

print(get_all_compensation())