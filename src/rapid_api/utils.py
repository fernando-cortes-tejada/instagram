import requests
from datetime import datetime
import json

from src.rapid_api.entities import (
    USER_ID_URL,
    USER_STORIES_URL,
    HEADERS,
)


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


def download_img_from_url(url: str, to_filepath: str) -> None:
    filename = f"{datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.jpg"

    img_data = requests.get(url).content
    with open(f"{to_filepath}/{filename}", "wb") as handler:
        handler.write(img_data)


def download_vid_from_url(url: str, to_filepath: str) -> None:
    filename = f"{datetime.today().strftime('%Y-%m-%d_%H-%M-%S')}.mp4"

    vid_data = requests.get(url).content
    with open(f"{to_filepath}/{filename}", "wb") as handler:
        handler.write(vid_data)


def pretty(dict_: dict) -> None:
    print(json.dumps(dict_, indent=2))
