import os
import pandas as pd
from google.cloud import bigquery
import google.auth
import pandas_gbq as pdgbq
import time

from src.rapid_api.utils import (
    renew_user_id_from_username,
    request_user_stories_dict,
    get_stories_img_urls,
    get_stories_vid_urls,
    download_img_from_url,
    download_vid_from_url,
    pretty,
)
from src.rapid_api.entities import GCP_CREDENTIALS, SCOPES, IG_PROFILES_QUERY

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS

credentials, project = google.auth.default(SCOPES)
pdgbq.context.credentials = credentials
bigquery.Client(project, credentials)

usernames = pd.read_gbq(IG_PROFILES_QUERY, project)
usernames = usernames[usernames["ACTIVO"] == 1]["PROFILE"].to_list()

# para la prueba vamos a tomar 1 (marimanotas)
username = usernames[1]

# solo en caso sea necesario renovar el user_id
renew_user_id_from_username(username)

dict_ = request_user_stories_dict(username)
print(dict_["success"])

pretty(dict_)

pretty(dict_["data"][0])

urls_img = get_stories_img_urls(dict_)
urls_vid = get_stories_vid_urls(dict_)

for url_img in urls_img:
    download_img_from_url(url_img, f"data/{username}/stories")
    time.sleep(5)
for url_vid in urls_vid:
    download_vid_from_url(url_vid, f"data/{username}/stories")
    time.sleep(5)
