from datetime import datetime, timedelta
import instaloader
from csv import writer
from pathlib import Path
import pandas as pd
import time
import os
from os import listdir
from os.path import join, isdir
import requests
import easyocr
from google.cloud import bigquery
import google.auth
import pandas_gbq as pdgbq

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="credentials_favik.json"

credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
    ]
)
pdgbq.context.credentials = credentials
bigquery.Client(project, credentials)

ig_data_filename = 'ig_data.csv'
ig_accounts_filename = 'ig_accounts.csv'

if not Path(ig_data_filename).is_file():
    with open(ig_data_filename, 'w', encoding='utf8', newline='') as csv_file:
        thewriter = writer(csv_file)
        header = ['PROFILE', 'OWNER_ID', 'DATETIME', 'TYPE', 'LIKES', 'COMMENTS', 'VIDEO_VIEWS', 'TYPE_IG', 'HASHTAGS', 'MENTIONS', 'TAGGED', 'URL']
        thewriter.writerow(header)

ig = instaloader.Instaloader()

ig_acc = pd.read_csv(ig_accounts_filename)

requests.get("https://api.ipify.org/?format=json").json()['ip']

ig.login(ig_acc.iloc[0, 1], ig_acc.iloc[0, 2])

ig_profiles = pd.read_gbq('select * from data-warehouse-325605.Drive.ig_profiles', project)
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
    time.sleep(10)
    for item in story.get_items():
        time.sleep(10)
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
        time.sleep(5)    
        ig.download_storyitem(item, Path('data\\' + item.profile + '\stories'))

ig.close()

reader = easyocr.Reader(['en','es'], gpu = False)

profiles = [f for f in listdir('data') if isdir(join('data', f))]

ig_data = pd.read_csv('ig_data.csv')

for profile in profiles:
    prof_index = ig_data.loc[(ig_data['PROFILE'] == profile) & (ig_data['TYPE'] == 'story') & (pd.isna(ig_data['TAGGED']))].index
    n = len(prof_index)
    c = 1

    for i in prof_index:
        storydatetime = ig_data.loc[i, 'DATETIME']
        img_path = f"data/{profile}/stories\\{storydatetime[0:10]}_{storydatetime[11:20].replace(':','-')}_UTC.jpg"
        try:
            results = reader.readtext(img_path)
        except:
            ig_data.loc[i, 'TAGGED'] = 'img_not_found'
        else: 
            results = pd.DataFrame(results, columns=['bbox','text','conf']).text
            mentions = results[results.str.startswith('@')].str.cat(sep=',').replace('@','')
            hashtags = results[results.str.startswith('#')].str.cat(sep=',').replace('#','')
            tagged = results.str.cat(sep=' ')
            ig_data.loc[i, ['HASHTAGS', 'MENTIONS', 'TAGGED']] = [hashtags, mentions, tagged]
        print(f"Profile {profile}, Iteration {c} of {n}")
        c = c + 1
    
    ig_data.to_csv('ig_data.csv', index=False)