from datetime import datetime, timedelta
import instaloader
from csv import writer
from pathlib import Path
import pandas as pd
import time
import os
from google.cloud import storage

ig_data_filename = 'ig_data.csv'
ig_profiles_filename = 'ig_profiles.csv'
ig_accounts_filename = 'ig_accounts.csv'

if not Path(ig_data_filename).is_file():
    with open(ig_data_filename, 'w', encoding='utf8', newline='') as csv_file:
        thewriter = writer(csv_file)
        header = ['PROFILE', 'OWNER_ID', 'DATETIME', 'TYPE', 'LIKES', 'COMMENTS', 'VIDEO_VIEWS', 'TYPE_IG', 'HASHTAGS', 'MENTIONS', 'TAGGED', 'URL']
        thewriter.writerow(header)

ig = instaloader.Instaloader()

ig_acc = pd.read_csv(ig_accounts_filename)

ig.login(ig_acc.iloc[0, 1], ig_acc.iloc[0, 2])

ig_profiles = pd.read_csv(ig_profiles_filename)
ig_profiles = ig_profiles[ig_profiles['ACTIVO'] == 1]['PROFILE'].to_list()

os.chdir('data')
for ig_profile in ig_profiles:
    ig.check_profile_id(ig_profile)
    time.sleep(3)
os.chdir('..')

ig_ids = []
for id_file in ['data/' + ig_profile + '/id' for ig_profile in ig_profiles]:
    ig_ids += [int(open(id_file,'r').read())]

min_date = datetime.now() - timedelta(hours=24, minutes=0)

for ig_profile in ig_profiles:
    posts = instaloader.Profile.from_username(ig.context, ig_profile).get_posts()
    time.sleep(5)
    i = 1
    for post in posts:
        time.sleep(3)
        if post.date >= min_date:
            with open(ig_data_filename, 'a', encoding='utf8', newline='') as csv_file:
                thewriter = writer(csv_file)
                info = [ig_profile,
                        post.owner_id, 
                        post.date,
                        'post', 
                        post.likes, 
                        post.comments, 
                        post.video_view_count, 
                        post.typename, 
                        ','.join(post.caption_hashtags), 
                        ','.join(post.caption_mentions), 
                        ','.join(post.tagged_users), 
                        'https://instagram.com/p/' + post.shortcode + '/']
                thewriter.writerow(info)
            ig.download_post(post, Path('data\\' + ig_profile + '\posts'))
        i += 1
        if i > 6:
            break

stories = ig.get_stories(userids=ig_ids)

for story in stories:
    time.sleep(5)
    for item in story.get_items():
        time.sleep(5)
        with open(ig_data_filename, 'a', encoding='utf8', newline='') as csv_file:
            thewriter = writer(csv_file)
            info = [item.profile, 
                    item.owner_id,
                    item.date,
                    'story', 
                    0, 
                    0, 
                    0, 
                    item.typename, 
                    '', 
                    '', 
                    '', 
                    '']
            thewriter.writerow(info)
        ig.download_storyitem(item, Path('data\\' + item.profile + '\stories'))

ig.close()

def authenticate_implicit_with_adc(project_id):
    storage_client = storage.Client(project=project_id)
    buckets = storage_client.list_buckets()
    print("Buckets:")
    for bucket in buckets:
        print(bucket.name)

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="credentials_favik.json"

storage_client = storage.Client(project='data-warehouse-325605')
buckets = storage_client.list_buckets()
for bucket in buckets:
    print(bucket.name)

authenticate_implicit_with_adc('data-warehouse-325605')
upload_blob('hevofavik', 'ig_accounts.csv', 'instagram/ig_accounts.csv')


def list_blobs(bucket_name):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    # Note: The call returns a response only when the iterator is consumed.
    for blob in blobs:
        print(blob.name)

list_blobs('hevofavik')