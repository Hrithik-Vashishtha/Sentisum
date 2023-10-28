# from elasticsearch import Elasticsearch, helpers
# import json
# import pandas as pd
# es = Elasticsearch("http://localhost:9200", http_auth=("elastic", "OsboUfs5GKauVaoV5=dQ"))

# datasets = [
#     "salary_survey-1.csv",
#     "salary_survey-2.csv",
#     "salary_survey-3.csv",
# ]

# #Function to read CSV file and index documents
# def index_data(datasets):
#     df = pd.read_csv(dataset)
#     print(df)
#     json_str = df.to_json(orient='records')
#     json_records = json.loads(json_str)
    
#     action_list = []
#     for row in json_records:
#         record = {
#             '_op_type': 'index',
#             '_index': index_name,
#             '_source': row
#         }
#         action_list.append(record)
    
#     helpers.bulk(es, action_list)

# # Upload all datasets to Elasticsearch
# for dataset in datasets:
#     index_data(dataset)