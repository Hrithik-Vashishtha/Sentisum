from elasticsearch import Elasticsearch, helpers
import pandas as pd
import re
from geopy.geocoders import Nominatim

# Initialize Elasticsearch connection
es = Elasticsearch("http://localhost:9200", http_auth=("elastic", "OsboUfs5GKauVaoV5=dQ"))

# Read CSV data into DataFrame
df = pd.read_csv('salary_survey-1.csv')

# Drop rows with missing values
df = df.dropna()

# Function to clean and convert salary to float
def clean_salary(salary):
    # Remove non-numeric characters and commas
    salary = re.sub(r'[^\d]', '', str(salary))
    try:
        return float(salary)
    except ValueError:
        return None

# Apply clean_salary function to the salary column
df['What is your annual salary?'] = df['What is your annual salary?'].apply(clean_salary)

# Drop rows with missing values after salary cleaning
df = df.dropna()

# Initialize geolocator
geolocator = Nominatim(user_agent="geoapiExercises")

# Function to get city from location
def get_city_from_location(location):
    try:
        location_info = geolocator.geocode(location, exactly_one=True, addressdetails=True)
        city = location_info.raw.get('address', {}).get('city')
        return city
    except Exception as e:
        print(f"Error processing location '{location}': {e}")
        return None

# Apply get_city_from_location function to the "Location" column
df['Where are you located? (City/state/country)'] = df['Where are you located? (City/state/country)'].apply(get_city_from_location)

# Convert DataFrame to dictionary records
df2 = df.to_dict('records')

# Generator function for Elasticsearch bulk indexing
def generator(df2):
    for c, line in enumerate(df2):
        yield {
            '_index': 'myelkfirst',
            '_id': line.get('show_id', None),
            '_source': {
                "Timestamp": line.get("Timestamp", ""),
                "Industry": line.get("What industry do you work in", ""),
                "JobTitle": line.get("Job title", ""),
                "Salary": line.get("What is your annual salary?", ""),
                "Currency": line.get("Please indicate the currency", ""),
                "Location": line.get("Where are you located? (City/state/country)", ""),
                "YearsOfExperience": line.get("How many years of post-college professional work experience do you have?", ""),
                "AdditionalContext": line.get("If your job title needs additional context, please clarify here:", ""),
                "Other": line.get('If "Other," please indicate the currency here', '')
            }
        }

# Elasticsearch index settings and mappings
Settings = {
    'settings': {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "Timestamp": { "type": "text" },
            "Industry": { "type": "keyword" },
            "JobTitle": { "type": "text" },
            "Salary": { "type": "float" },
            "Currency": { "type": "text" },
            "Location": { "type": "keyword" },
            "YearsOfExperience": { "type": "text" },
            "AdditionalContext": { "type": "text" },
            "Other": { "type": "text" }
        }
    }
}

# Create Elasticsearch index
IndexName = ''
my = es.indices.create(index='compensation_data', ignore=[400,404],body=Settings)

# Bulk index data into Elasticsearch
res = es.indices.get_alias(index="*")
try:
    res = helpers.bulk(es, generator(df2))
    print("Working")
    print("Response: ",res)
except Exception as e:
    # print(e)
    pass

# Print success message
print("Success")
