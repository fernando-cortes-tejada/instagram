from itertools import islice
import getopt, sys
from math import ceil
import json
from instaloader import Instaloader, Profile
import pandas as pd
import instaloader
import pymysql
import time
from datetime import datetime
import logging
import sshtunnel
from sshtunnel import SSHTunnelForwarder
active_account = {};

processed = ['CHYQllRHIEX','CW4LiwEAqLV'];

ssh_host = ''
ssh_username = ''
ssh_password = ''
database_username = ''
database_password = ''
database_name = ''
localhost = '127.0.0.1'
PROFILE = ""  
call_function = ""

# Remove 1st argument from the
# list of command line arguments
argumentList = sys.argv[1:]
 
# Options
options = "ht:u:p:s:w:n:f:r:"
 
# Long options
long_options = ["Help", "Host =", "ssh_username =","ssh_password =","db_username =","db_password =","db_name =","function =","profile ="]
 
try:
    # Parsing argument
    arguments, values = getopt.getopt(argumentList, options, long_options)
     
    # checking each argument
    for currentArgument, currentValue in arguments:
 
        if currentArgument in ("-h", "--Help"):
            print ("Displaying Help")

        elif currentArgument in ("-t", "--Host"):
            ssh_host = currentValue
            print ("Displaying Host:  (% s)", currentValue)
        elif currentArgument in ("-u", "--ssh_username"):
            ssh_username = currentValue
            print ("Displaying ssh username:  (% s)", currentValue)
        elif currentArgument in ("-p", "--ssh_password"):
            ssh_password = currentValue
        elif currentArgument in ("-s", "--db_username"):
            database_username = currentValue
            print ("Displaying database username:  (% s)", currentValue)
        elif currentArgument in ("-w", "--db_password"):
            database_password = currentValue
        elif currentArgument in ("-n", "--db_name"):
            database_name = currentValue
            print ("Displaying database name:  (% s)", currentValue)
        elif currentArgument in ("-r", "--profile"):
            PROFILE = currentValue
            print ("Displaying Profile:  (% s)", currentValue)
        elif currentArgument in ("-f", "--function"):
            call_function = currentValue
            print ("Displaying call function:  (% s)", currentValue)
except getopt.error as err:
    # output error, and return with an error code
    print (str(err))


def open_ssh_tunnel(verbose=False):
    """Open an SSH tunnel and connect using a username and password.
    
    :param verbose: Set to True to show logging
    :return tunnel: Global SSH tunnel connection
    """
    
    if verbose:
        sshtunnel.DEFAULT_LOGLEVEL = logging.DEBUG
    
    global tunnel
    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username = ssh_username,
        ssh_password = ssh_password,
        remote_bind_address = ('127.0.0.1', 3306)
    )
    
    tunnel.start()



def mysql_connect():
    """Connect to a MySQL server using the SSH tunnel connection
    
    :return connection: Global MySQL database connection
    """
    
    global connection
    
    connection = pymysql.connect(
        host='127.0.0.1',
        user=database_username,
        passwd=database_password,
        db=database_name,
        port=tunnel.local_bind_port
    )

def mysql_disconnect():
    """Closes the MySQL database connection.
    """
    
    connection.close()

def close_ssh_tunnel():
    """Closes the SSH tunnel connection.
    """
    
    tunnel.close    





def get_most_liked_posts_profile(L,PROFILE,X_percentage):
    profile = Profile.from_username(L.context, PROFILE)
    print("Got profile")
    print(profile)
    posts_sorted_by_likes = sorted(profile.get_posts(),
                               key=lambda p: p.likes + p.comments,
                               reverse=True)
    #for post in islice(posts_sorted_by_likes, ceil(profile.mediacount * X_percentage / 100)):
    print("iterating posts")
    return islice(posts_sorted_by_likes, ceil(profile.mediacount * X_percentage / 100))

def get_post_by_shortcode(L,shortcode):
    post = instaloader.Post.from_shortcode(L.context, shortcode)
    posts = []
    posts.append(post)
    return posts

def replace_in_file(file,user1,user2):
    #read input file
    fin = open(file, "rt")
    #read file contents to string
    data = fin.read()
    #replace all occurrences of the required string
    data = data.replace(user1, user2)
    #close the input file
    fin.close()
    #open the input file in write mode
    fin = open(file, "wt")
    #overrite the input file with the resulting data
    fin.write(data)
    #close the file
    fin.close()

def handle_failed_attempt(PROFILE,type,user_social_id):
    print("handle_failed_attempt")
    print(PROFILE)
    print(type)
    print(user_social_id)
    L = Instaloader()
    global active_account
    print("Changing account")
    print(active_account[0])
    
    account = get_next_account()
    update_instagram_account_time(account[0])
    print("To Account")
    print(account[0])
    replace_in_file(str(PROFILE)+"_resume_info.json",active_account[0],account[0])
    active_account = account
    L.login(account[0], account[1])
    print("get_followers_profile2")
    print(type)
    print(user_social_id)
    get_followers_profile2(L,PROFILE,type,user_social_id)

def get_inst_followers(L,PROFILE,f):
    prof = instaloader.Profile.from_username(L.context, PROFILE)
    counter = 0
    frozen  = instaloader.load_structure_from_file(L.context,str(PROFILE)+"_resume_info.json") # Check if you have a frozen iterator in the file
    iterator  = prof.get_followers()
    iterator.thaw(frozen ) # If you do, resume that

    try:
        for follower in iterator:
            #candidate = {'profile_id':PROFILE, 'name':follower.username,'inst_id':follower.userid}
            print("1,"+PROFILE+","+follower.username+","+str(follower.userid)+",instagram_post", file=f)
            counter = counter +1
            if counter % 20 == 0:
                #instaloader.save_structure_to_file(iterator.freeze(),"resume_info.json")
                time.sleep(2)
                #iterator  = instaloader.load_structure_from_file("resume_info.json") # Check if you have a frozen iterator in the file
                #prof.get_followers().thaw(iterator ) # If you do, resume that
            #followerList.append(candidate)
            print(follower.username)
            try_add_follower(follower.username,follower.userid,prof.username,prof.userid,False)
    except:
        instaloader.save_structure_to_file(iterator.freeze(),str(PROFILE)+"_resume_info.json")
        handle_failed_attempt(PROFILE,'followers',123)

def get_inst_followees(L,PROFILE,f,user_social_id):
    print("get_inst_followees")
    prof = instaloader.Profile.from_username(L.context, PROFILE)
    counter = 0;
    try:
        frozen  = instaloader.load_structure_from_file(L.context,str(PROFILE)+"_resume_info.json") # Check if you have a frozen iterator in the file
        iterator  = prof.get_followees()
        iterator.thaw(frozen ) # If you do, resume that
    except:
        iterator  = prof.get_followees()

    print("Getting followers")
    print(iterator)
    try:
        for follower in iterator:
            #candidate = {'profile_id':PROFILE, 'name':follower.username,'inst_id':follower.userid}
            print("1,"+PROFILE+","+follower.username+","+str(follower.userid)+",instagram_post", file=f)
            counter = counter +1
            if counter % 20 == 0:
                #instaloader.save_structure_to_file(iterator.freeze(),"resume_info.json")
                time.sleep(2)
                #iterator  = instaloader.load_structure_from_file("resume_info.json") # Check if you have a frozen iterator in the file
                #prof.get_followers().thaw(iterator ) # If you do, resume that
            #followerList.append(candidate)
            print("insert_special_follow")
            print(follower.username)
            print(user_social_id)
            insert_special_follow(follower.username,user_social_id)
        print("User finished")
        update_user_social_time(user_social_id)
    except:
        instaloader.save_structure_to_file(iterator.freeze(),str(PROFILE)+"_resume_info.json")
        handle_failed_attempt(PROFILE,'followees',user_social_id)

def get_inst_followees2(L,PROFILE,f,user_social_id,type):
    print("get_inst_followees")
    try:
        prof = instaloader.Profile.from_username(L.context, PROFILE)
    except:
        return True
    counter = 0;
    try:
        frozen  = instaloader.load_structure_from_file(L.context,str(PROFILE)+"_resume_info.json") # Check if you have a frozen iterator in the file
        if type == "followers":
            iterator  = prof.get_followers()
        else:
            iterator  = prof.get_followees()
        iterator.thaw(frozen ) # If you do, resume that
    except:
        if type == "followers":
            iterator  = prof.get_followers()
        else:
            iterator  = prof.get_followees()

    print("Getting "+type)
    print(iterator)
    print("Call started")
    dateTimeObj = datetime.now()
    print(dateTimeObj)
    try:
        for follower in iterator:
            #candidate = {'profile_id':PROFILE, 'name':follower.username,'inst_id':follower.userid}
            print("1,"+PROFILE+","+follower.username+","+str(follower.userid)+",instagram_post", file=f)
            counter = counter +1
            if counter % 20 == 0:
                time.sleep(2)
            print(follower.username)
            print(user_social_id)
            if type == "followers":
                try_add_follower(follower.username,follower.userid,prof.username,prof.userid,False)
            else:
                print("insert_special_follow: "+str(counter))
                insert_special_follow(follower.username,user_social_id)
            
        print("User finished")
        dateTimeObj = datetime.now()
        print(dateTimeObj)
        if type == "followees":
            update_user_social_time(user_social_id)
    except:
        instaloader.save_structure_to_file(iterator.freeze(),str(PROFILE)+"_resume_info.json")
        handle_failed_attempt(PROFILE,type,user_social_id)

def get_followers_profile2(L,PROFILE,type,user_social_id):
    
    print("Fetching "+type+" of profile {}.".format(PROFILE))
    filename = PROFILE+ ' followersss.csv'
    if type == "followee":
        filename = PROFILE+ ' followeeesss.csv'
    
    # followerList = []
    counter = 0;
    with open(filename, 'w') as f:
        print("id,profile_id,username,external_user_id,followable_type", file=f)
        print("writing csv header")
        get_inst_followees2(L,PROFILE,f,user_social_id,type)
            
    #df2 = pd.DataFrame(followerList) 
    f.close()
    # saving the dataframe 
    #df2.to_csv(PROFILE+ ' followers2.csv') 
    print("saved followers to csv")

def check_follow_exists(type,followable_id,followee_username,followee_id):
    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT `id` FROM `follows` WHERE `followable_type`=%s AND `followable_id`=%s AND `username`=%s AND `users_social_id`=%s"
        cursor.execute(sql, (type,followable_id,followee_username,followee_id,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return 0


def check_user_social_exists(typeS,username):
    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT `id` FROM `users_social` WHERE `provider`=%s AND `username`=%s "
        cursor.execute(sql, (typeS,username,))
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return 0

def get_profiles():
    try:
        mysql_connect()
    except:
        print("Connection active, not creating")

    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT id,username FROM `users_social` WHERE `last_checked` is null or last_checked > DATE_SUB(NOW(),INTERVAL 1 YEAR) ORDER BY RAND() limit 1 "
        cursor.execute(sql,)
        result = cursor.fetchone()
        if result:
            return result
        else:
            return 0

def get_next_account():
    try:
        mysql_connect()
    except:
        print("Connection active, not creating")

    with connection.cursor() as cursor:
        # Read a single record
        sql = "SELECT username,password FROM `instagram_accounts` order by last_used limit 1 "
        cursor.execute(sql,)
        result = cursor.fetchone()
        if result:
            return result
        else:
            return 0

def insert_user_social(type,users_social_id,username):
    with connection:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `users_social` (`username`, `provider`, `provider_id`) VALUES (%s, %s, %s)"
            cursor.execute(sql, (username,type, users_social_id,))

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()

def update_user_social_time(users_social_id):
    try:
        mysql_connect()
    except:
        print("Connection active, not creating")
    with connection:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "UPDATE `users_social` set last_checked = NOW() WHERE id=%s "
            cursor.execute(sql, (users_social_id,))

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()

def update_instagram_account_time(username):
    try:
        mysql_connect()
    except:
        print("Connection active, not creating")
    with connection:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "UPDATE `instagram_accounts` set last_used = NOW() WHERE username=%s "
            cursor.execute(sql, (username,))

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()

def insert_special_follow(account,users_social_id):
    sql = 'insert into follows (followable_type,followable_id,username,users_social_id)  '
    sql += 'select "instagram_follower",%s,u.username,u.id from key_accounts k '
    sql += 'join users_social u on k.account=u.username '
    sql += 'where account =%s and account not in '
    sql += '(select username from follows '
    sql += 'where followable_type = "instagram_follower" and followable_id = %s);'
    try:
        mysql_connect()
    except:
        print("Connection active, not creating")
    with connection:
        with connection.cursor() as cursor:
            # Create a new record
            cursor.execute(sql, (int(users_social_id),account, users_social_id))

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()
    print("Done insert_special_follow")
    


def insert_user_follow(type,follower_social_id,followee_username,followee_social_id):
    with connection:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `follows` (`followable_type`, `followable_id`, `username`, `users_social_id`) VALUES (%s, %s, %s, %s)"
            cursor.execute(sql, (type, follower_social_id,followee_username,followee_social_id,))

        # connection is not autocommit by default. So you must commit to save
        # your changes.
        connection.commit()

def try_add_follower(follower_username,follower_id,followee_username,followee_id,examine):
    mysql_connect()
    complete = True
    if examine:
        complete = False
        check_account_counts(follower_username)
    if complete:
        results = check_user_social_exists('instagram',follower_username)
        follower_local_id = 0
        if results == 0:
            insert_user_social('instagram',follower_id,follower_username)
            mysql_connect()
            follower_local_id = check_user_social_exists('instagram',follower_username)
        else:
            follower_local_id = results

        mysql_connect()
        results2 = check_user_social_exists('instagram',followee_username)
        followee_local_id = 0
        if results2 == 0:
            insert_user_social('instagram',followee_id,followee_username)
            mysql_connect()
            followee_local_id = check_user_social_exists('instagram',followee_username)
        else:
            followee_local_id = results2

        mysql_connect()
        follow_results = check_follow_exists('instagram_follower',follower_local_id,followee_username,followee_local_id)
        if follow_results == 0:
            insert_user_follow('instagram_follower',follower_local_id,followee_username,followee_local_id)

def get_followees_main(L):

    followees = get_profiles()
    print("followees")
    print(followees)
    print("getting user followees")
    print(followees[1])
    print(followees[0])
    get_followers_profile2(L,followees[1],"followees",followees[0])
    print("Iterating")
    get_followees_main(L)

def get_post_by_subject(L,PROFILE):
          # profile to download from
    # percentage of posts that should be downloaded
    X_percentage = 10 
    global processed
    #posts = get_most_liked_posts_profile(L,PROFILE,X_percentage)
    posts = get_post_by_shortcode(L,"CIdugwSnbfb")
    print("iterating posts")
    with open('posts.csv', 'w') as f:
        print("id,social_network,content_type,external_id,title,username,likes,comments,date", file=f)
        print("writing csv header")
        for post in posts:
            if post.shortcode in processed:
                continue
            print("shortcode")
            print(post.shortcode)
            print("owner_username")
            print(post.owner_username)
            print("likes")
            print(post.likes)
            print("comments")
            print(post.comments)
            print("date")
            print(post.date.strftime("%Y-%m-%d %H:%M:%S"))
            print("1,instagram,instagram_post,"+post.shortcode+","+post.owner_username+","+str(post.likes)+","+str(post.comments)+","+post.date.strftime("%Y-%m-%d %H:%M:%S"), file=f)
            parse_post(post)

        f.close()

def parse_post(post):
    print("per post")
    
    
    print("get comments")

    # Iterate over all comments of the post.
    # Each comment is represented by a PostComment namedtuple with fields 
    # text (string), created_at (datetime), id (int), owner (Profile) 
    # and answers (~typing.Iterator[PostCommentAnswer]) if available.
    #commentList = []
    try:
        frozen  = instaloader.load_structure_from_file(L.context,str(PROFILE)+"_"+post.shortcode+"_comments_resume_info.json") # Check if you have a frozen iterator in the file
        post_comments = post.get_comments()
        iterator.thaw(frozen ) # If you do, resume that
    except:
        post_comments = post.get_comments()
    try:    
        with open(post.shortcode+ ' comments.csv', 'w') as f:
            print("id,type,social_network,external_id,username,external_user_id,date,text", file=f)
            print("writing csv header")
            for comment in post_comments:
                # candidate = {'post_id':post.shortcode, 'name':comment.owner.username,'external_id':comment.owner.userid,'text':comment.text,'date':comment.created_at_utc}
                #commentList.append(candidate)
                clean_com = comment.text.replace(",", " ")
                clean_com = clean_com.replace("'", " ")
                clean_com = clean_com.replace('"','' )
                print("1,instagram_post,instagram,"+post.shortcode+","+comment.owner.username+","+str(comment.owner.userid)+","+comment.created_at_utc.strftime("%Y-%m-%d %H:%M:%S")+","+clean_com, file=f)
                print(comment.owner.username)
        
        f.close()
    except:
        instaloader.save_structure_to_file(post_comments.freeze(),str(PROFILE)+"_"+post.shortcode+"_comments_resume_info.json")

    #df2 = pd.DataFrame(commentList) 
    # saving the dataframe 
    #df2.to_csv(post.shortcode+ ' comments2.csv') 
    print("saved comments to csv")
    return True
    try:
        frozen  = instaloader.load_structure_from_file(L.context,str(PROFILE)+"_"+post.shortcode+"_likes_resume_info.json") # Check if you have a frozen iterator in the file
        post_likes = post.get_likes()
        iterator.thaw(frozen ) # If you do, resume that
    except:
        post_likes = post.get_likes()
    print("get likes")
    # Iterate over all likes of the post. A Profile instance of each likee is yielded.
    #likeList = []
    try:
        with open(post.shortcode+ ' likes.csv', 'w') as f:
            print("id,type,external_id,username,external_user_id", file=f)
            print("writing csv header")
            for likee in post_likes:
                #candidate = {'post_id':post.shortcode, 'name':likee.username,'inst_id':likee.userid}
                print("1,instagram_post,"+post.shortcode+","+likee.username+","+str(likee.userid), file=f)
                print(likee.username)
                print(likee.userid)
                #likeList.append(candidate)

    except:
        instaloader.save_structure_to_file(post_comments.freeze(),str(PROFILE)+"_"+post.shortcode+"_likes_resume_info.json")
    f.close()
    #df1 = pd.DataFrame(likeList) 
    # saving the dataframe 
    #df1.to_csv(post.shortcode+ ' likes2.csv') 
    print("saved likes to csv")
    
    



#exit()
open_ssh_tunnel()
#mysql_connect()
#insert_special_follow("carlosvives","1225")
#exit()
#PROFILE = "luisafernandaw"  
L = Instaloader()
account = get_next_account()
update_instagram_account_time(account[0])
print(account)
active_account = account
L.login(account[0], account[1]);
#get_post_by_subject(L,PROFILE)
#L2 = Instaloader()
if call_function == "followers":
    get_followers_profile2(L,PROFILE,"followers",123)
elif call_function == "followees":
    get_followees_main(L)
elif call_function == "comments":
    get_post_by_subject(L,PROFILE)