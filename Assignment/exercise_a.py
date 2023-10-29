from elasticsearch import Elasticsearch, helpers
import pandas as pd
import re
from geopy.geocoders import Nominatim

from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200", http_auth=("elastic", "OsboUfs5GKauVaoV5=dQ"))
df = pd.read_csv('salary_survey-1.csv')
df = df.dropna()


def clean_salary(salary):
    # Remove non-numeric characters and commas
    salary = re.sub(r'[^\d]', '', str(salary))
    try:
        return float(salary)
    except ValueError:
        return None
df['What is your annual salary?'] = df['What is your annual salary?'].apply(clean_salary)

df = df.dropna()



# Initialize the geolocator
geolocator = Nominatim(user_agent="geoapiExercises")

def get_city_from_location(location):
    try:
        location_info = geolocator.geocode(location, exactly_one=True, addressdetails=True)
        city = location_info.raw.get('address', {}).get('city')
        return city
    except Exception as e:
        print(f"Error processing location '{location}': {e}")
        return None

# Apply the function to the "Location" column and update it
df['Where are you located? (City/state/country)'] = df['Where are you located? (City/state/country)'].apply(get_city_from_location)
df2 = df.to_dict('records')

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

mycustom = generator(df2)
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
IndexName = ''
my = es.indices.create(index='compensation_data', ignore=[400,404],body=Settings)

res = es.indices.get_alias(index="*")
try:
    res = helpers.bulk(es, generator(df2))
    print("Working")
    print("Response: ",res)
except Exception as e:
    # print(e)
    pass
print("Success")