from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import requests
import json,csv
from datetime import datetime 
import os

class imdb_api:
    def __init__(
        self,
        id="",
        name="",
        type="",
        cast="",
        rank=None,
        year=None,
        img_url=None,
        img_height=None,
        img_width=None,
        v=None,
        vt=None,
        active_years=None,
    ):
        self.id = id
        self.name = name
        self.type = type
        self.cast = cast
        self.rank = rank
        self.year = year
        self.img_url = img_url
        self.img_height = img_height
        self.img_width = img_width
        self.v = v
        self.vt = vt
        self.active_years = active_years
url = "https://imdb8.p.rapidapi.com/auto-complete"

querystring = {"q":"tom"}
headers = {
    "x-rapidapi-key": "c9b9b16516msh7c539a4bd84174ap17c99cjsn8e4b1e6d4612",
    "x-rapidapi-host": "imdb8.p.rapidapi.com",
}
response = requests.get(url, headers=headers, params=querystring)
response_obj = json.loads(response.text)
writeFile = open(r"/root/airflow/dags/response.json", "w")
writeFile.write(response.text)
writeFile.close()
# Write to .CSV
f = open(r"/root/airflow/dags/imdb_data.csv", "w", newline="")
csv_writer = csv.writer(f)
count = 0
data = response_obj["d"]
normalized_data = []
for mov in data:
    id = mov.get("id")
    name = mov.get("l", "")
    type = mov.get("q", "")
    cast = mov.get("s", "")
    rank = mov.get("rank", None)
    year = mov.get("y", None)
    img_details = mov.get("i", {})
    img_url = img_details.get("imageUrl", None)
    img_height = img_details.get("height", None)
    img_width = img_details.get("width", None)
    v = mov.get("v", [])
    str_v = ""
    for tr in v:
        str_v += "["
        str_v += tr["id"]+","
        tr["title"] = tr.get("l", "")
        str_v += tr["title"] + ","
        tr["duration"] = tr.get("s", "")
        str_v += tr["duration"] + ","
        tr["img_url"] = tr["i"]["imageUrl"]
        str_v += tr["img_url"] + ","
        tr["img_height"] = tr["i"]["height"]
        str_v += str(tr["img_height"]) + ","
        tr["img_width"] = tr["i"]["width"]
        str_v += str(tr["img_width"])
        str_v += "],"
    str_v = str_v[:-1]
    vt = mov.get("vt", 0)
    active_years = mov.get("yr", None)
    c_row = imdb_api(
        id,
        name,
        type,
        cast,
        rank,
        year,
        img_url,
        img_height,
        img_width,
        str_v,
        vt,
        active_years,
    )
    normalized_data.append(c_row)
header = [
    "id",
    "name",
    "type",
    "cast",
    "rank",
    "year",
    "img_url",
    "img_height",
    "img_width",
    "v",
    "vt",
    "active_years",
]
#csv_writer.writerow(header)
for mov in normalized_data:
    csv_writer.writerow(mov.__dict__.values())


for mov in data:
    if year==" ":
        year=None
    if v==" ":
        v=None
    if vt==" ":
        vt=None
    if active_years==" ":
        active_years=None
f.close()
