import os
import pandas as pd
from google.cloud import bigquery
import google.auth
import pandas_gbq as pdgbq
import time
from datetime import datetime

from src.rapid_api.utils import (
    renew_user_id_from_username,
    request_user_stories_dict,
    get_user_id_from_username,
    get_stories_ids,
    get_stories_links,
    get_stories_mentions,
    get_stories_hashtags,
    get_stories_img_urls,
    get_stories_vid_urls,
    downupload_img_from_url,
    downupload_vid_from_url,
)
from src.rapid_api.entities import (
    GCP_CREDENTIALS,
    SCOPES,
    IG_PROFILES_QUERY,
    IG_ITEMS_DATA_PATHFILE,
)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS

credentials, project = google.auth.default(SCOPES)
pdgbq.context.credentials = credentials
bigquery.Client(project, credentials)

usernames = pd.read_gbq(IG_PROFILES_QUERY, project)
usernames = usernames[usernames["ACTIVO"] == 1]["PROFILE"].to_list()

for username in usernames:

    # solo en caso sea necesario renovar el user_id
    # renew_user_id_from_username(username)

    dict_ = request_user_stories_dict(username)

    n_stories = dict_["count"]

    user_id = get_user_id_from_username(username)

    stories_ids = get_stories_ids(dict_)
    stories_links = get_stories_links(dict_)
    stories_mentions = get_stories_mentions(dict_)
    stories_hashtags = get_stories_hashtags(dict_)

    urls_img = get_stories_img_urls(dict_)
    urls_vid = get_stories_vid_urls(dict_)

    df = pd.read_csv(IG_ITEMS_DATA_PATHFILE)

    for i in range(n_stories):
        if not stories_ids[i] in list(df.ITEM_ID):
            scraped_datetime = datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
            info = [
                username,
                user_id,
                stories_ids[i],
                scraped_datetime,
                "story",
                0,
                0,
                0,
                "-".join(stories_links[i]),
                "-".join(stories_mentions[i]),
                "-".join(stories_hashtags[i]),
                "",
                "",
            ]
            downupload_img_from_url(urls_img[i], f"data/{username}/stories")
            time.sleep(1)
            try:
                downupload_vid_from_url(urls_vid[i], f"data/{username}/stories")
            except:
                None
            else:
                time.sleep(1)

            df.loc[len(df)] = info

        print(f"{str(i+1)}/{n_stories}")
        if ((i + 1) % 5) == 0:
            df.to_csv(IG_ITEMS_DATA_PATHFILE, index=False)

    df.to_csv(IG_ITEMS_DATA_PATHFILE, index=False)
