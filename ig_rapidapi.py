import requests
import json
from datetime import datetime


def pretty(dict_: dict) -> None:
    return print(json.dumps(dict_, indent=2))


profile = "marimanotas"
user_id = int(open(f"data/{profile}/id", "r").read())

url = f"https://instagram188.p.rapidapi.com/userstories/{user_id}"

headers = {
    "X-RapidAPI-Key": "82ce440fc5mshfbc1b0d2a9f55bcp11f90bjsne7277f3c9971",
    "X-RapidAPI-Host": "instagram188.p.rapidapi.com",
}

response = requests.request("GET", url, headers=headers)

dict_ = response.json()

pretty(dict_)

dict_["success"]
dict_["count"]
pretty(dict_["data"])

len(dict_["data"])
dict_["data"][0]["image_versions2"].keys()

pretty(dict_["data"][0]["image_versions2"]["candidates"][0]["url"])

dict_["data"][0]["id"]

img_url = dict_["data"][0]["image_versions2"]["candidates"][0]["url"]

img_filename = f"{profile}_{datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.jpg"

img_data = requests.get(img_url).content
with open(img_filename, "wb") as handler:
    handler.write(img_data)
