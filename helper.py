import os
import glob
import logging
from tqdm import tqdm
import ast
import requests
import os
import json
import pandas as pd
import time

import numpy as np
import re
import ast

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyser = SentimentIntensityAnalyzer()

import emoji



def set_logger(log_file_path="debug.log", level="INFO"):
    # def set_logger(log_file_path="debug.log", level="DEBUG"):
    logger = logging.getLogger()
    logger.setLevel(level)
    scream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    scream_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(scream_handler)
    logger.addHandler(file_handler)
    return logger


try:
    # print(len(logger.handlers))
    while len(logger.handlers) > 1:
        logger.handlers.pop(0)
        # print(len(logger.handlers))
except:
    pass

logger = set_logger()



def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
    return headers


def connect_to_endpoint(endpoint_url, headers, params):
    response = requests.request("GET", endpoint_url, headers=headers, params=params)
    # print(response.status_code)
    # if response.status_code != 200:
        # raise Exception(response.status_code, response.text)
    return response.json()


def get_api_token(token_path):
    try:
        with open(token_path, "r") as f:
            logger.debug("token_path: %s" % token_path)
            lines = f.readlines()
            logger.debug("lines in the file: %s" % lines)

            lines = [line.split(": ")[-1][:-1] for line in lines]
        return lines

    except Exception as e:
        logger.error("Error: %s" % str(e))

def is_too_many_requests(json_response, start_time):
    is_too_many = False
    now = time.perf_counter()
    elapsed_time = now - start_time
    # time_window = 15 * 60  # seconds, 15 min
    time_window = 2 * 60  # seconds, 2 min
    title = json_response.get('title', "")
    if title == 'Too Many Requests':
        is_too_many = True
        # print(f"To many requests, will sleep {int(time_window - elapsed_time)} seconds.")
        print(f"To many requests, will sleep {time_window} seconds.")
        time.sleep(time_window)

    return is_too_many, elapsed_time


# -------------------- merge results -------------------#
def find_place_id(row):
    # print(row)

    cell_text = row.get("geo", "")
    # print(cell_text)
    if len(cell_text) > 1:
        place_dict = ast.literal_eval(cell_text)
    else:
        return ""
    # print(place_dict)
    if isinstance(place_dict, dict):
        place_id = place_dict.get("place_id", "")
        if len(place_id) > 1:
            return place_id


def clean_tweets(row):
    text = row['text'].replace('\n', ' ').replace(",", ";").replace('\r', '').replace('\t', ' ').strip()
    return text

def remove_newline_comma(df):
    for c in df.columns:
        df[c] = df[c].astype(str).str.replace('\n', ' ')
        df[c] = df[c].str.replace('\n', ' ').replace(",", ";").replace('\r', '').replace('\t', ' ').str.strip()
    return df

def find_poll_id(row):
    text = row['text'].replace('\n', ' ').replace(",", ";").replace('\r', '').replace('\t', ' ').strip()
    return text


def refine_data(df):
    df['place_id'] = df.apply(find_place_id, axis=1)
    df['text'] = df.apply(clean_tweets, axis=1)

    if 'attachments' not in df.columns:
        df['attachments'] = ''
        logger.warning("Found no attachments column in the returned tweets.")

    return df


def find_media_row(row, df_media):
    try:
        cell_text = row["attachments"]
        cell_text = str(cell_text)

        if len(cell_text) > 1:
            if cell_text[0] != "{" and cell_text[-1] != "{":
                return ""
            attachments_dict = ast.literal_eval(cell_text)
        else:
            return ""

        if isinstance(attachments_dict, dict):
            media_keys = attachments_dict.get("media_keys", [])
            # if len(media_keys) > 1:
            #     print(media_keys)

            if media_keys == "":
                return ""
            media_rows = []
            # print(df_media)
            # print(attachments_dict)

            targets_df = df_media[df_media['media_table_media_key'].isin(media_keys)]

            targets_json = json.dumps(json.loads(targets_df.to_json(orient='records')))

            # for key in media_keys:
            #     key = str(key)
            #     if len(key) > 1:
            #         # print(key)
            #         # print(df_media['media_table_media_key'])
            #         row = df_media[df_media['media_table_media_key'] == key]
            #
            #         row = df_media[df_media['media_table_media_key'] == key].to_json(orient='records')#[1:-1]
            #         row = json.dumps(row)
            #         # print(df_media[df_media['media_table_media_key'] == key])
            #         media_rows.append(row)
            # print(media_rows)
            return targets_json
        return ""
    except Exception as e:
        print("Error in find_media_row():", e, cell_text)
        logger.error(e, exc_info=True)
        return ""


def find_poll_row(row, df_poll):
    try:
        cell_text = row["attachments"]
        cell_text = str(cell_text)
        if len(cell_text) > 1:
            if cell_text[0] != "{" and cell_text[-1] != "{":
                return ""
            attachments_dict = ast.literal_eval(cell_text)
        else:
            return ""

        if isinstance(attachments_dict, dict):
            poll_ids = attachments_dict.get("poll_ids", [])
            if poll_ids == "":
                return ""
            poll_rows = []

            target_df = df_poll[df_poll['polls_table_id'].isin(poll_ids)]
            target_json = json.dumps(json.loads(target_df.to_json(orient='records')))
            # print(df_poll)
            # print(attachments_dict)
            # for i in poll_ids:
            #     i = str(i)
            #     if len(i) > 1:
            #         row = df_poll[df_poll['polls_table_id'] == i].to_json(orient='records')
            #         poll_rows.append(row)

            return target_json
    except Exception as e:
        print("Error in find_poll_row():", e)
        logger.error(e, exc_info=True)


def get_lonlat(row):
    row["lon"] = ""
    row["lat"] = ""
    #     print('row[places_table_geo]:', row["places_table_geo"])
    if len(row["places_table_geo"]) > 1:
        geo_dict = ast.literal_eval(row["places_table_geo"])
        #         print('geo_dict:', geo_dict)
        bbox = geo_dict.get("bbox", [])
        if len(bbox) == 4:
            row["lon"] = (bbox[0] + bbox[2]) / 2
            row["lat"] = (bbox[1] + bbox[3]) / 2
    return row


def merge_a_response(data_file_name, save_path=""):
    try:
        d = data_file_name   # data csv file
        df_data = pd.read_csv(d, engine='python')
        # print(d)
        df_data = df_data.fillna("")
        df_data = refine_data(df_data)

        df_data_columns = ['author_id', 'conversation_id', 'created_at', 'entities', 'geo', 'id', 'lang', 'possibly_sensitive', 'public_metrics', 'reply_settings', 'source', 'text']
        for c in df_data_columns:
            if c not in df_data.columns:
                df_data[c]=''

        df_merged = df_data

        # process places file
        places_csv = d.replace("data.csv", "includes_places.csv")
        if os.path.exists(places_csv):
            df_places = pd.read_csv(places_csv, engine='python').fillna("")
        else:
            df_places = pd.DataFrame(columns=['country', 'country_code', 'full_name', 'geo', 'id', 'name', 'place_type'])
        new_column_name = {name: "places_table_" + name for name in df_places.columns}
        df_places = df_places.rename(columns=new_column_name)
        df_merged = pd.merge(df_merged, df_places, how='left', left_on="place_id", right_on="places_table_id")

        places_columns = ['country', 'country_code', 'full_name', 'geo', 'id', 'name', 'place_type']
        for c in places_columns:
            c = "places_table_" + c
            if c not in df_merged.columns:
                df_merged[c] = ''


        # process tweets file
        tweets_csv = d.replace("data.csv", "includes_tweets.csv")
        if os.path.exists(tweets_csv):
            df_tweets = pd.read_csv(tweets_csv, engine='python').fillna("")
            df_tweets["text"] = df_tweets["text"].str.replace("\n", " ")
            if 'in_reply_to_user_id' not in df_tweets.columns:
                df_tweets['in_reply_to_user_id'] = ''
                logger.warning("Found no in_reply_to_user_id column in the returned includes_tweets.")

            new_column_name = {name: "tweets_table_" + name for name in df_tweets.columns}
            df_tweets = df_tweets.rename(columns=new_column_name)
            df_merged = pd.merge(df_merged, df_tweets, how='left', left_on="id", right_on="tweets_table_id")

        tweets_df_columns = ['attachments', 'author_id', 'conversation_id', 'created_at', 'geo', 'id', 'in_reply_to_user_id', 'lang', 'possibly_sensitive', 'public_metrics', 'referenced_tweets', 'reply_settings', 'source', 'text']
        # tweets_df_columns = {name: "tweets_table_" + name for name in tweets_df_columns}
        for c in tweets_df_columns:
            c = "tweets_table_" + c
            if c not in df_merged.columns:
                df_merged[c] = ''


        # process users file
        users_csv = d.replace("data.csv", "includes_users.csv")
        users_columns = ['created_at', 'description', 'entities', 'id', 'location', 'name', 'pinned_tweet_id', 'profile_image_url', 'protected', 'public_metrics', 'url', 'username', 'verified']
        if os.path.exists(users_csv):
            df_users = pd.read_csv(users_csv, engine='python').fillna("")
            df_users = df_users[df_users['id'] != '']
            df_users['id'] = df_users['id'].astype(str)
            df_users = df_users[df_users['id'].str.isnumeric()]
            # df_users["description"] = df_users["description"].str.replace("\n", " ").replace("\n", " ").replace("\n", " ").replace("\r", " ").strip()
            df_users["description"] = df_users["description"].str.replace("\t", " ").replace("\b", " ").replace("\n", " ").replace("\r", " ").str.strip()
            # df_users["id"] = df_users["id"].astype(int)  # there may be some error rows such as the empyty id

            new_column_name = {name: "users_table_" + name for name in df_users.columns}
            df_users = df_users.rename(columns=new_column_name)
            df_merged['author_id'] = df_merged['author_id'].astype(str)
            df_merged = pd.merge(df_merged, df_users, how='left', left_on="author_id", right_on="users_table_id")

        for c in users_columns:
            c = "users_table_" + c
            if c not in df_merged.columns:
                df_merged[c] = ''

            # process media file
        media_csv = d.replace("data.csv", "includes_media.csv")
        if os.path.exists(media_csv):
            df_media = pd.read_csv(media_csv, engine='python').fillna("")
        else:
            df_media = pd.DataFrame(columns=['duration_ms', 'height', 'media_key', 'preview_image_url', 'public_metrics', 'type', 'url', 'width'])
        df_media['media_key'] = df_media['media_key'].astype(str)
        new_column_name = {name: "media_table_" + name for name in df_media.columns}
        df_media = df_media.rename(columns=new_column_name)
        df_merged["media_table_rows"] = df_merged.apply(find_media_row, args=(df_media,), axis=1)

        media_columns = ['height', 'media_key', 'type', 'url', 'width']
        for c in media_columns:
            c = "media_table_" + c
            if c not in df_merged.columns:
                df_merged[c] = ''

        # process poll file
        poll_csv = d.replace("data.csv", "includes_polls.csv")
        if os.path.exists(poll_csv):
            df_poll = pd.read_csv(poll_csv, engine='python').fillna("")
        else:
            df_poll = pd.DataFrame(
                columns=['duration_minutes', 'end_datetime', 'id', 'options', 'voting_status'])
        df_poll['id'] = df_poll['id'].astype(str)
        new_column_name = {name: "polls_table_" + name for name in df_poll.columns}
        df_poll = df_poll.rename(columns=new_column_name)
        df_merged["polls_table_rows"] = df_merged.apply(find_poll_row, args=(df_poll,), axis=1)
        poll_columns = ['duration_minutes', 'end_datetime', 'id', 'options']
        for c in poll_columns:
            c = "polls_table_" + c
            if c not in df_merged.columns:
                df_merged[c] = ''


    except Exception as e:
        print("Error in merge_results for loop: ", e)
        logger.error(e, exc_info=True)

    df_merged = df_merged.fillna("")
    df_merged.replace(np.nan, "")
    df_merged.replace("\n", " ")
    df_merged = df_merged.drop_duplicates(subset=['id'], keep='last')

    df_merged = df_merged.apply(get_lonlat, axis=1)

    df_merged = df_merged.sort_index(axis=1)

    if save_path != "" and os.path.exists(save_path):
        base_name = os.path.basename(d).replace("_data.", '.')
        new_name = os.path.join(save_path, base_name)
        df_merged.to_csv(new_name, index=False)

    return df_merged

def merge_results(saved_path, is_zipped=False):
    if is_zipped:
        suffix = '.csv.gz'
    else:
        suffix = '.csv'

    line_tweets_dir = os.path.join(saved_path, 'line_tweets')
    os.makedirs(line_tweets_dir, exist_ok=True)
    data_files = glob.glob(os.path.join(saved_path, r'raw_tweets', f"*_data{suffix}"))
    logger.info("Start to merge %d filles." % len(data_files))
    all_df = []
    for d in tqdm(data_files[:]):
        df_merged = merge_a_response(d, save_path=line_tweets_dir)
        # print(len(df_places))
        # return df_merged
        all_df.append(df_merged)

    print("\nGenerating final CSV file, including %d small CSV files." % len(all_df))
    print("\nPlease wait...")

    if len(all_df) == 0:
        print("No DataFrames to be merge.")
        return None

    final_df = pd.concat(all_df, ignore_index=False).fillna("")
    final_df = final_df.apply(get_lonlat, axis=1)#.reset_index()
    final_file = os.path.join(saved_path, "merged.csv")
    final_df.to_csv(final_file, index=False)
    logger.info("\nSaved merged tweets in %s ." % final_file)

    return final_df

###############################################
# for the cluster CSV columns
def get_username(row):
    author = ast.literal_eval(row['author'])
    username = author['username'].replace("\t", " ").replace("\r", ";").replace(",", ";").replace('\n', ';').strip()
    return username

def get_geoType(row):
    try:

        cell_text = row['places_table_geo']
        if cell_text == "":
            return ""
        geo = ast.literal_eval(cell_text)
        bbox = geo['bbox']

        if bbox[1] == bbox[3]:
            geoType = 'LatLon'
        else:
            geoType = 'Place'
    except:
        geoType = ''
    return geoType

def get_longitude(row):
    try:
        cell_text = row['places_table_geo']
        if cell_text == "":
            return ""
        geo = ast.literal_eval(cell_text)
        bbox = geo['bbox']
        bbox = [float(i) for i in bbox]
        longitude = (bbox[0] + bbox[2]) / 2
    except:
        longitude = ''
    return longitude

def get_latitude(row):
    try:
        cell_text = row['places_table_geo']
        if cell_text == "":
            return ""
        geo = ast.literal_eval(cell_text)
        bbox = geo['bbox']
        bbox = [float(i) for i in bbox]
        latitude = (bbox[1] + bbox[3]) / 2
    except:
        latitude = ''
    return latitude

def get_place(row):
    try:
        geo = ast.literal_eval(row['geo'])
        place = geo['full_name'].replace("\t", " ").replace("\r", ";").replace(",", ";").replace('\n', ';').strip()
    except:
        place = ''
    return place

def get_placeBboxWest(row):
    try:
        cell_text = row['places_table_geo']
        if cell_text == "":
            return ""
        geo = ast.literal_eval(cell_text)
        bbox = geo['bbox']
        bbox = [float(i) for i in bbox]
        placeBboxWest = bbox[0]
    except:
        placeBboxWest = ''
    return placeBboxWest

def get_placeBboxEast(row):
    try:
        cell_text = row['places_table_geo']
        if cell_text == "":
            return ""
        geo = ast.literal_eval(cell_text)
        bbox = geo['bbox']
        bbox = [float(i) for i in bbox]
        placeBboxEast = bbox[2]
    except:
        placeBboxEast = ''
    return placeBboxEast

def get_placeBboxSouth(row):
    try:
        cell_text = row['places_table_geo']
        if cell_text == "":
            return ""
        geo = ast.literal_eval(cell_text)
        bbox = geo['bbox']
        bbox = [float(i) for i in bbox]
        placeBboxSouth = bbox[1]
    except:
        placeBboxSouth = ''
    return placeBboxSouth

def get_placeBboxNorth(row):
    try:
        cell_text = row['places_table_geo']
        if cell_text == "":
            return ""
        geo = ast.literal_eval(cell_text)
        bbox = geo['bbox']
        bbox = [float(i) for i in bbox]
        placeBboxNorth = bbox[3]
    except:
        placeBboxNorth = ''
    return placeBboxNorth

def get_source(row):
    try:
        source = row['source'].replace("\t", " ").replace("\r", ";").replace(",", ";").replace('\n', ';').strip()
    except:
        source = ''
    return source


def get_userMentions(row):
    try:
        cell_text = row['entities']
        if cell_text == "":
            return ""
        entities = ast.literal_eval(cell_text)
        # if len(entities) > 1:
        #     print("entities' length > 1 !!")

        userMentions = entities.get('mentions', "")  # a list)
        userMentions = [u['username'] for u in userMentions]
        mentioned_names = []
        for u in userMentions:
            mentioned_names.append(u.replace("\t", " ").replace("\r", ";").replace(",", ";").replace('\n', ';').strip())
        userMentions = ';'.join(mentioned_names)
    except:
        userMentions = ''
    return userMentions


def get_urls(row):
    try:
        cell_text = row['entities']
        # if cell_text == "":
        #     return ""
        entities = ast.literal_eval(cell_text)


        # if len(entities) > 1:
        #     print("entities' length > 1 !!")

        urls = entities.get('urls', "")
        urls = [u['expanded_url'] for u in urls]
        # urls = ';'.join(urls)
    except:
        urls = []
    try:
        # get media url
        cell_text = row['media_table_rows']
        if cell_text == "":
            media_table_rows = []
        else:
            media_table_rows = ast.literal_eval(cell_text)

        media_urls = [u['media_table_url'] for u in media_table_rows]
        # media_urls = ';'.join(media_urls)
    except:
        media_urls = []

    all_list = urls + media_urls
    return ";".join(all_list)

def get_hashtags(row):
    try:
        cell_text = row['entities']
        if cell_text == "":
            return ""
        entities = ast.literal_eval(cell_text)
        # if len(entities) > 1:
        #     print("entities' length > 1 !!")

        hashtags = entities.get('hashtags', "")
        hashtags = [u['tag'] for u in hashtags]
        hashtags = ';'.join(hashtags)
    except:
        hashtags = ''
    return hashtags

def get_retweetCount(row):
    try:
        public_metrics = ast.literal_eval(row['public_metrics'])
        retweetCount = public_metrics['retweet_count']
    except:
        retweetCount = ''
    return retweetCount

def get_userLocation(row):
    try:
        author = ast.literal_eval(row['author'])
        userLocation = author['location'].replace(",",";").replace('\n', ' ').strip()
    except:
        userLocation = ''
    return userLocation

def get_followersCount(row):
    try:
        users_table_public_metrics = ast.literal_eval(row['users_table_public_metrics'])
        followersCount = users_table_public_metrics['followers_count']
    except:
        followersCount = ''
    return followersCount

def get_friendsCount(row):
    try:
        friendsCount = ''  # Do not know how to obtain yet.
    except:
        friendsCount = ''
    return friendsCount

def get_joinDay(row):
    try:
        author = ast.literal_eval(row['author'])
        joinDay = author['created_at']
        # joinDay = joinDay[:10]
    except:
        joinDay = ''
    return joinDay

def get_favouritesCount(row):
    try:
        favouritesCount = ''  # Do not know how to obtain yet.
    except:
        favouritesCount = ''
    return favouritesCount

def get_language(row):
    try:
        # language = row['lang'].replace(",", ";").replace('\r', '').replace('\n', ' ').strip()
        user_language = ''   # Do not know how to obtain yet.
    except:
        user_language = ''
    return user_language

def get_statusesCount(row):
    try:
        statusesCount = ''     # Do not know how to obtain yet.
    except:
        statusesCount = ''
    return statusesCount

def get_replyToStatusId(row):
    try:
        replyToStatusId = ''     # Do not know how to obtain yet.
    except:
        replyToStatusId = ''
    return replyToStatusId


def get_replyToUserId(row):
    try:
        replyToUserId = row['in_reply_to_user_id']
        replyToUserId = int(replyToUserId)
    except:
        replyToUserId = ''
    return replyToUserId

def get_userVerified(row):
    try:
        author = ast.literal_eval(row['author'])
        userVerified = author['verified']
    except:
        userVerified = ''
    return userVerified

def get_userDescription(row):
    try:
        author = ast.literal_eval(row['author'])
        userDescription = author['description'].replace(",", ";").replace('\r', ' ').replace('\n', ' ').replace("\r", " ").strip()
    except:
        userDescription = ''
    return userDescription

def get_userUrl(row):
    try:
        author = ast.literal_eval(row['author'])
        userUrl = author['url']
    except:
        userUrl = ''
    return userUrl



def get_listedCount(row):
    try:
        users_table_public_metrics = ast.literal_eval(row['users_table_public_metrics'])
        listed_count = users_table_public_metrics['listed_count']
    except:
        listed_count = ''
    return listed_count


def get_placeType(row):
    try:
        geo = ast.literal_eval(row['geo'])
        placeType = geo['place_type']
    except:
        placeType = ''
    return placeType


def get_bboxType(row):
    try:
        bboxType = ''   # Do not know how to obtain yet.
    except:
        bboxType = ''
    return bboxType


def get_placeId(row):
    try:
        geo = ast.literal_eval(row['geo'])
        placeId = geo['place_id']
    except:
        placeId = ''
    return placeId

def get_country_code(row):
    try:
        geo = ast.literal_eval(row['geo'])
        country_code = geo['country_code']
    except:
        country_code = ''
    return country_code

def get_country(row):
    try:
        geo = ast.literal_eval(row['geo'])
        country = geo['country'].replace(",", ";").replace('\r', '').replace('\n', ' ').strip()
    except:
        country = ''
    return country

def get_tweet_lang(row):
    try:
        tweet_lang = row['lang'].replace(",", ";").replace('\r', '').replace('\n', ' ').strip()
    except:
        tweet_lang = ''
    return tweet_lang


def get_sentiment(row):
    try:
        tw = row['text']
        cleaned_tw = clean_twts(tw)
        sentiment = float(analyser.polarity_scores(cleaned_tw)['compound'])
    except:
        sentiment = ''
    return sentiment

def clean_twts(tw):  # input is a string
    # tw = row['text']
  # remove urls
    pattern = 'https{0,1}:\/\/t.co\/[a-zA-Z0-9]+'
    tw = re.sub(pattern, "", tw)

  # remove @
    pattern = '@[a-zA-Z0-9_]+ '
    tw = re.sub(pattern, "", tw)

  # remove emojis
    tw = emoji.demojize(tw)



    return tw
#######################################



def convert_to_cluster(df):
    # csvHeader = "tweetID,userID,username,date,message,geoType,longitude,latitude,place,placeBboxWest,placeBboxEast,placeBboxSouth,placeBboxNorth,
    # source,userMentions,urls,hashtags,retweetCount,userLocation,followersCount,friendsCount,joinDay,favouritesCount,language,statusesCount,
    # replyToStatusId,replyToUserId,userVerified,userDescription,userUrl,favoriteCount,listedCount,placeType,bboxType,placeId,country_code,country,
    # tweet_lang,message_en,message_cn,sentiment,topic" + '\n'

    new_df = pd.DataFrame()

    new_df['tweetid'] = df['id']
    new_df['userid'] = df['author_id']
    # new_df['username'] = df.apply(get_username, axis=1)
    try:
        new_df['username'] = df['users_table_name'].replace("\t", " ").replace("\r", ";").replace(",", ";").replace('\n', ';').str.strip()
    except Exception as e:
        logger.info("No name file in users_table.")
        print(df)
        new_df['username'] = ""
    new_df['postdate'] = df['created_at']
    new_df['message'] = df['text'].replace('\n', ';')
    new_df['geoType'] = df.apply(get_geoType, axis=1)
    new_df['longitude'] = df.apply(get_longitude, axis=1)
    new_df['latitude'] = df.apply(get_latitude, axis=1)
    new_df['place'] = df['places_table_name'].replace("\t", " ").replace("\r", ";").replace(",", ";").replace('\n', ';').str.strip()

    new_df['placeBboxwest'] = df.apply(get_placeBboxWest, axis=1)
    new_df['placeBboxeast'] = df.apply(get_placeBboxEast, axis=1)
    new_df['placeBboxsouth'] = df.apply(get_placeBboxSouth, axis=1)
    new_df['placeBboxnorth'] = df.apply(get_placeBboxNorth, axis=1)

    new_df['source'] = df['source']
    new_df['usermentions'] = df.apply(get_userMentions, axis=1)  # Not understand yet
    new_df['urls'] = df.apply(get_urls, axis=1)
    new_df['hashtags'] = df.apply(get_hashtags, axis=1)
    new_df['retweetCount'] = df.apply(get_retweetCount, axis=1)
    new_df['userlocation'] = df['users_table_location'].replace("\t", " ").replace("\r", ";").replace(",", ";").replace('\n', ';').str.strip()
    new_df['followerscount'] = df.apply(get_followersCount, axis=1)
    new_df['friendscount'] = "" # Do not know how to obtain yet. #df.apply(get_friendsCount, axis=1)
    new_df['joinday'] = df['users_table_created_at'].replace("\t", " ").replace("\r", ";").replace(",", ";").replace('\n', ';').str.strip()
    new_df['favouritesCount'] = ""   # Do not know how to obtain yet. df.apply(get_favouritesCount, axis=1)

    new_df['language'] = ""   # Do not know how to obtain yet.  # df.apply(get_language, axis=1)
    new_df['statusescount'] =  ""   # Do not know how to obtain yet.  # df.apply(get_statusesCount, axis=1)
    new_df['replytostatusid'] = ""   # Do not know how to obtain yet.  # df.apply(get_replyToStatusId, axis=1)
    new_df['replytoUserid'] = df['tweets_table_in_reply_to_user_id']
    new_df['userverified'] = df['users_table_verified']
    new_df['userdescription'] = df['users_table_description'].replace("\t", " ").replace("\r", ";").replace(",", ";").replace('\n', ';').str.strip().replace('\n', ';')
    new_df['userurl'] = df['users_table_url']

    new_df['favoritecount'] = ''  # Do not know how to obtain yet. # df.apply(get_favouritesCount, axis=1)

    new_df['listedcount'] = df.apply(get_listedCount, axis=1)
    new_df['placetype'] = df['places_table_place_type']

    new_df['bboxtype'] = ''   # Do not know how to obtain yet. # df.apply(get_bboxType, axis=1)
    new_df['placeid'] = df['places_table_id']

    new_df['year'] = new_df['postdate'].str[:4]
    new_df['month'] = new_df['postdate'].str[5:7]
    new_df['geo'] = new_df['geoType'].str.lower()

    new_df['country_code'] = df['places_table_country_code']
    new_df['country'] = df['places_table_country']

    new_df['tweet_lang'] = df['lang']
    new_df['message_en'] = ''
    new_df['message_cn'] = ''
    new_df['sentiment'] = df.apply(get_sentiment, axis=1)
    new_df['topic'] = ''

    new_df['username_account'] = df['users_table_username'].replace("\t", " ").replace("\r", ";").replace(",", ";").replace('\n',
                                                                                                                ';').str.strip()

    return new_df


if __name__ == "__main__":
    data_file_name = r'/media/gpu/Seagate/Research/tweet_download/downloaded_tweets_suicide_2020/raw_tweets/2020-12-09T03_18_30.000Z_2020-12-09T22_11_39.000Z_482_data.csv'
    merge_a_response(data_file_name, save_path="")