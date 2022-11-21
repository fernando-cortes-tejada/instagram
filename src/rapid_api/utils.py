import requests
from datetime import datetime
import json
from pathlib import Path
from google.cloud import storage
import os

from src.rapid_api.entities import (
    USER_ID_URL,
    USER_STORIES_URL,
    HEADERS,
    GCP_CREDENTIALS,
    BUCKET_NAME,
)


def upload_to_bucket(gcs_path_to_file, local_path_to_file, bucket_name):
    storage_client = storage.Client.from_service_account_json(GCP_CREDENTIALS)

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_path_to_file)
    blob.upload_from_filename(local_path_to_file)


def request_user_id_from_username(username: str) -> dict:
    url = f"{USER_ID_URL}/{username}"
    headers = HEADERS

    response = requests.request("GET", url, headers=headers)
    return response.json()


def renew_user_id_from_username(username: str) -> None:
    user_id_dict_ = request_user_id_from_username(username)
    if user_id_dict_["success"]:
        user_id = user_id_dict_["data"]
        with open(f"data/{username}/id", "w") as f:
            f.write(user_id)
            f.close()


def get_user_id_from_username(username: str) -> int:
    user_id = int(open(f"data/{username}/id", "r").read())
    return user_id


def request_user_stories_dict(username: str) -> dict:
    user_id = get_user_id_from_username(username)
    url = f"{USER_STORIES_URL}/{user_id}"
    headers = HEADERS

    response = requests.request("GET", url, headers=headers)
    return response.json()


def get_stories_ids(dict_: dict) -> list[str]:
    list_ = [data["id"] for data in dict_["data"]]
    return list_


def get_stories_links(dict_: dict) -> list[str]:
    list_ = []

    for data in dict_["data"]:
        try:
            list_tmp = []
            for data_2 in data["story_link_stickers"]:
                list_tmp.append(data_2["story_link"]["display_url"])
        except:
            list_.append("")
        else:
            list_.append(list_tmp)
    return list_


def get_stories_mentions(dict_: dict) -> list[str]:
    list_ = []

    for data in dict_["data"]:
        try:
            list_tmp = []
            for data_2 in data["story_bloks_stickers"]:
                list_tmp.append(
                    data_2["bloks_sticker"]["sticker_data"]["ig_mention"]["username"]
                )
        except:
            list_.append("")
        else:
            list_.append(list_tmp)
    return list_


def get_stories_hashtags(dict_: dict) -> list[str]:
    list_ = []

    for data in dict_["data"]:
        try:
            list_tmp = []
            for data_2 in data["story_hashtags"]:
                list_tmp.append(data_2["hashtag"]["name"])
        except:
            list_.append("")
        else:
            list_.append(list_tmp)
    return list_


def get_stories_img_urls(dict_: dict) -> list[str]:
    list_ = [data["image_versions2"]["candidates"][0]["url"] for data in dict_["data"]]
    return list_


def get_stories_vid_urls(dict_: dict) -> list[str]:
    list_ = [
        data["video_versions"][0]["url"]
        for data in dict_["data"]
        if data["media_type"] == 2
    ]
    return list_


def downupload_img_from_url(url: str, to_filepath: str) -> None:
    filename = f"{datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.jpg"
    Path(to_filepath).mkdir(parents=True, exist_ok=True)
    fullpath = f"{to_filepath}/{filename}"
    img_data = requests.get(url).content
    with open(f"{to_filepath}/{filename}", "wb") as handler:
        handler.write(img_data)

    upload_to_bucket(f"instagram/{fullpath}", fullpath, BUCKET_NAME)
    os.remove(fullpath)


def downupload_vid_from_url(url: str, to_filepath: str) -> None:
    filename = f"{datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.mp4"
    Path(to_filepath).mkdir(parents=True, exist_ok=True)
    fullpath = f"{to_filepath}/{filename}"
    vid_data = requests.get(url).content
    with open(fullpath, "wb") as handler:
        handler.write(vid_data)

    upload_to_bucket(f"instagram/{fullpath}", fullpath, BUCKET_NAME)
    os.remove(fullpath)


def pretty(dict_: dict) -> None:
    print(json.dumps(dict_, indent=2))
