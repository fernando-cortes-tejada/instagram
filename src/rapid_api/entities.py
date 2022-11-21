BUCKET_NAME = "hevofavik"
GCP_CREDENTIALS = "credentials_favik.json"
SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
]
IG_PROFILES_QUERY = "select * from data-warehouse-325605.Drive.ig_profiles"
USER_ID_URL = "https://instagram188.p.rapidapi.com/userid"
USER_STORIES_URL = "https://instagram188.p.rapidapi.com/userstories"
HEADERS = {
    "X-RapidAPI-Key": "b7f79d471fmsh43c6332f4d13995p13d9f8jsne2e390a36dd0",
    "X-RapidAPI-Host": "instagram188.p.rapidapi.com",
}
IG_ITEMS_DATA_PATHFILE = f"gs://{BUCKET_NAME}/instagram/ig_items_data.csv"
