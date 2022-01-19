from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import requests
import json,csv
from datetime import datetime 
import os

class imdb_api:
    def __init__(
        self,
        country="",
        domains="",
        name="",
        state_province="",
        web_pages="",
        alpha_two_code="",
        
        
    ):
        self.country= country
        self.domains = domains
        self.name=name
        self.state_province=state_province
        self.web_pages=web_pages
        self.alpha_two_code=alpha_two_code
        
		
url = "http://universities.hipolabs.com/search?country=india"


response = requests.request("GET", url)
writeFile = open(r"/root/airflow/dags/response.json", "w")
s=response.text
s=s.replace('"state-province":','"state_province":')
response_obj = json.loads(s)
writeFile.write(s)
writeFile.close()
f = open(r"/root/airflow/dags/imdb_data.csv", "w", newline="") 
csv_writer = csv.writer(f)
data = response_obj
normalized_data = []
for mov in data:
    country = mov.get("country")
    domains = mov.get("domains")
    name = mov.get("name")
    state_province = mov.get("state_province")
    web_pages = mov.get("web_pages")
    alpha_two_code = mov.get("alpha_two_code")
    c_row = imdb_api(
        country,
        domains,
        name,
        state_province,
        web_pages,
        alpha_two_code,
        
        )
    normalized_data.append(c_row)

header = [
    "country",
    "domains",
    "name",
    "state_province",
    "web_pages",
    "alpha_two_code",
    
    
]
#csv_writer.writerow(header)
for mov in normalized_data:
    csv_writer.writerow(mov.__dict__.values())


for mov in data:
  if state_province ==" ":
    state_province = None
 

f.close()
