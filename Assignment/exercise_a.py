from elasticsearch import Elasticsearch, helpers
import json
import pandas as pd

# Connect to your Elasticsearch instance
# es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

from elasticsearch import Elasticsearch

# es = Elasticsearch(['https://localhost:9200'], basic_auth=('elastic', 'OsboUfs5GKauVaoV5=dQ'))
es = Elasticsearch("http://localhost:9200", http_auth=("elastic", "OsboUfs5GKauVaoV5=dQ"))



# define the mapping for the index
index_name = "compensation_data_combined"
mapping = {
    "mappings":{
        "properties":{
      "Timestamp": { "type": "date" },
      "AgeRange": { "type": "keyword" },
      "Industry": { "type": "keyword" },
      "JobTitle": { "type": "text" },
      "Salary": { "type": "float" },
      "Currency": { "type": "keyword" },
      "Location": { "type": "keyword" },
      "YearsOfExperience": { "type": "keyword" },
      "AdditionalContext": { "type": "text" },
      "EmploymentType": { "type": "keyword" },
      "CompanyName": { "type": "text" },
      "CompanySize": { "type": "keyword" },
      "PrimaryLocationCountry": { "type": "keyword" },
      "PrimaryLocationCity": { "type": "keyword" },
      "IndustryInCompany": { "type": "keyword" },
      "PublicOrPrivateCompany": { "type": "keyword" },
      "YearsExperienceInIndustry": { "type": "keyword" },
      "YearsExperienceInCurrentCompany": { "type": "keyword" },
      "JobTitleInCompany": { "type": "text" },
      "JobLadder": { "type": "keyword" },
      "JobLevel": { "type": "keyword" },
      "RequiredHoursPerWeek": { "type": "integer" },
      "ActualHoursPerWeek": { "type": "integer" },
      "HighestEducationCompleted": { "type": "keyword" },
      "TotalBaseSalary2018": { "type": "float" },
      "TotalBonus2018": { "type": "float" },
      "TotalStockOptions2018": { "type": "float" },
      "HealthInsuranceOffered": { "type": "keyword" },
      "AnnualVacationWeeks": { "type": "integer" },
      "HappyAtCurrentPosition": { "type": "keyword" },
      "PlanToResignNext12Months": { "type": "keyword" },
      "IndustryDirectionThoughts": { "type": "text" },
      "Gender": { "type": "keyword" },
      "SkillsNecessaryForJobGrowth": { "type": "text" },
      "Employer": { "type": "text" },
      "Location": { "type": "text" },
      "YearsAtEmployer": { "type": "integer" },
      "YearsOfExperience": { "type": "integer" },
      "AnnualBasePay": { "type": "float" },
      "SigningBonus": { "type": "float" },
      "AnnualBonus": { "type": "float" },
      "AnnualStockValueBonus": { "type": "float" },
      "AdditionalComments": { "type": "text" }
        }
    }
}

#create the index with the defined mapping
es.indices.create(index=index_name, ignore=400, body=mapping)

#Define the lisst of datasets to be uploaded
# datasets = [
#     "salary_survey-1.csv",
#     "salary_survey-2.csv",
#     "salary_survey-3.csv",
# ]
datasets = [
    "cleaned_file-1.csv"]
#Function to read CSV file and index documents

def index_data(dataset):
        df = pd.read_csv(dataset)
        json_str = df.to_json(orient='records')
        json_records = json.loads(json_str)
        
        action_list = []
        for row in json_records:
            record = {
                '_op_type': 'index',
                '_index': index_name,
                '_source': row
            }
            action_list.append(record)
        
        helpers.bulk(es, action_list)


# Upload all datasets to Elasticsearch
for dataset in datasets:
    index_data(dataset)

# Validate that the data has been successfully stored
print("Data uploaded and validated successfully.")

# Average compensation of roles where the role is some kind of engineer
engineer_avg_salary_query = {
    "aggs": {
        "average_salary": {
            "avg": {
                "field": "Salary"
            }
        }
    },
    "query": {
        "match": {
            "JobTitle": "engineer"
        }
    }
}

# Execute the query
engineer_avg_salary_result = es.search(index=index_name, body=engineer_avg_salary_query)
print("Average Salary of Engineers:", engineer_avg_salary_result["aggregations"]["average_salary"]["value"])

# Average, min, and max compensation per city (if available in dataset)
city_salary_stats_query = {
    "aggs": {
        "average_salary": {
            "avg": {
                "field": "Salary"
            }
        },
        "min_salary": {
            "min": {
                "field": "Salary"
            }
        },
        "max_salary": {
            "max": {
                "field": "Salary"
            }
        }
    },
    "aggs": {
        "group_by_city": {
            "terms": {
                "field": "Location.keyword"
            }
        }
    }
}

# Execute the query
city_salary_stats_result = es.search(index=index_name, body=city_salary_stats_query)
for city_bucket in city_salary_stats_result["aggregations"]["group_by_city"]["buckets"]:
    city = city_bucket["key"]
    avg_salary = city_bucket["average_salary"]["value"]
    min_salary = city_bucket["min_salary"]["value"]
    max_salary = city_bucket["max_salary"]["value"]
    print(f"City: {city}, Average Salary: {avg_salary}, Min Salary: {min_salary}, Max Salary: {max_salary}")

# One interesting query of your choice
# Example: Query for top 5 highest salaries
interesting_query = {
    "query": {
        "match_all": {}  # You can change this query as per your interest
    },
    "size": 5,  # Change this to get different number of results
    "sort": [
        {"Salary": {"order": "desc"}}
    ]
}

# Execute the query
interesting_result = es.search(index=index_name, body=interesting_query)
for hit in interesting_result["hits"]["hits"]:
    print(hit["_source"])

