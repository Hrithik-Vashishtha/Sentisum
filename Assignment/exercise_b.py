from flask import Flask, jsonify, request
from elasticsearch import Elasticsearch

app = Flask(__name__)

# Connect to your Elasticsearch instance
es = Elasticsearch(['https://localhost:9200'], basic_auth=('elastic', 'OsboUfs5GKauVaoV5=dQ'))

# Define your index name
index_name = "compensation_data"

# List all compensation data with filtering and sorting
@app.route('/compensation', methods=['GET'])
def list_compensation():
    # Get parameters for filtering and sorting
    filters = request.args.to_dict()
    sort_by = filters.pop('sort_by', None)

    # Build the Elasticsearch query
    es_query = {'query': {'bool': {'must': []}}}
    for field, value in filters.items():
        es_query['query']['bool']['must'].append({'match': {field: value}})
    
    if sort_by:
        es_query['sort'] = [{sort_by: {'order': 'asc'}}]

    # Execute the Elasticsearch query
    docs = es.search(index=index_name, body=es_query)['hits']['hits']

    # Convert the documents to JSON
    json_records = []
    for doc in docs:
        record = doc['_source']
        json_records.append(record)

    return jsonify(json_records)

# Fetch a single record by ID
@app.route('/compensation/<string:id>', methods=['GET'])
def get_compensation_by_id(id):
    # Get the compensation record by ID
    doc = es.get(index=index_name, id=id)

    # If the record does not exist, return 404
    if not doc['_found']:
        return jsonify({'error': 'Compensation record not found'}), 404

    # Convert the document to JSON
    record = doc['_source']

    return jsonify(record)

# Bonus Goal: Return a sparse fieldset
@app.route('/compensation/sparse/<string:id>', methods=['GET'])
def get_sparse_compensation_by_id(id):
    # Get the compensation record by ID
    doc = es.get(index=index_name, id=id)

    # If the record does not exist, return 404
    if not doc['_found']:
        return jsonify({'error': 'Compensation record not found'}), 404

    # Define the fields to include in the sparse fieldset
    fields_to_include = ['JobTitle', 'Salary', 'Location']

    # Create a sparse fieldset
    sparse_record = {field: doc['_source'].get(field) for field in fields_to_include}

    return jsonify(sparse_record)

if __name__ == '__main__':
    app.run(debug=True)
