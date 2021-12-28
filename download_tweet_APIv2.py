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
import math
import emoji

import pandas as pd

import helper


logger = helper.logger
json_response_list = [] # a global variable to store the returned tweet. Will be emptied periodically

def save_search(json_response,
                saved_path,
                is_zipped=False,
                ):
    try:
        if not os.path.exists(saved_path):
            os.mkdir(saved_path)

        if is_zipped:
            suffix = '.csv.gz'
        else:
            suffix = '.csv'

        raw_tweets_dir = os.path.join(saved_path, 'raw_tweets')
        # line_tweets_dir = os.path.join(saved_path, 'line_tweets')
        meta = json_response['meta']
        data = json_response['data']
        includes = json_response['includes']

        df = pd.DataFrame(data)
        df = df.fillna("")
        df = helper.remove_newline_comma(df)

        df = df.sort_index(axis=1)

        # basename = f"{meta['oldest_id']}_{meta['newest_id']}_{meta['result_count']}"  # use tweet_id as basename
        lastest_time =  df.iloc[0]['created_at'].replace(":", "_")
        oldest_time =  df.iloc[-1]['created_at'].replace(":", "_")
        basename = f"{oldest_time}_{lastest_time}_{meta['result_count']}"  # use tweet time as basename

        data_filename = os.path.join(raw_tweets_dir, basename + f"_data{suffix}")

        df.to_csv(data_filename, index=False)
        result_count = meta['result_count']
        result_count = str(result_count)
        logger.info("Saved %s tweets in: %s" % (result_count, data_filename))

        for key in includes.keys():
            includes_filename = os.path.join(raw_tweets_dir, basename + f"_includes_{key}{suffix}")
            df = pd.DataFrame(includes[key])
            df = df.fillna("")
            df = helper.remove_newline_comma(df)
            df = df.sort_index(axis=1)

            df.to_csv(includes_filename, index=False)

        return data_filename

    except Exception as e:
        logger.error(e, exc_info=True)


def get_tweet_count(query, start_time, end_time, granularity='day', next_token=None, until_id=None):
    print("Counting tweets, please wait...")
    tweet_count_total = 0
    endpoint = r'https://api.twitter.com/2/tweets/counts/all'
    query_params = {'query': query, \
                    "start_time": start_time, \
                    "end_time": end_time, \
                    "granularity": granularity, \
                    "next_token": next_token, \
                    # "until_id": until_id,
                    }
    headers = helper.create_headers(bearer_token)

    next_token = 'Start'

    page_cnt = 0

    start_timer = time.perf_counter()
    while next_token is not None:

        try:
            json_response = helper.connect_to_endpoint(endpoint, headers, query_params)
            is_too_many, elapsed_time = helper.is_too_many_requests(json_response, start_timer)
            next_token = json_response['meta'].get('next_token', None)
            query_params['next_token'] = next_token
            tweet_count = json_response['meta']['total_tweet_count']
            tweet_count_total += tweet_count
            page_cnt += 1

            if page_cnt % 20 == 0:
                print(f"    current tweet count: {tweet_count_total}")
        except Exception as e:

            logger.error("Error in get_tweet_count():", exc_info=True)
            print("json_response:", json_response)
    #         print(f"next_token: {next_token}. total_tweet_count: {tweet_count_total}")

    return tweet_count_total  # , json_response

def execute_download(saved_path=os.getcwd(),
                    is_zipped=False,
                     ):
    if is_zipped:
        suffix = '.csv.gz'
    else:
        suffix = '.csv'

    query = "telemedicine  OR telehealth  OR telecare"

    # query = f"({keyword})"
    start_time = "2019-01-01T00:00:00Z"
    # end_time   = "2019-12-31T23:59:59Z"
    end_time   = "2021-12-01T00:00:00.000Z"
    # end_time = "2021-07-17T04_51_53.000Z".replace("_", ":")
    # until_id = '1139156316075757568'
    max_results = 500  # max_results can be 500 if do not request the field: context_annotations
    chunk_size = 500000  # tweets
    tweet_count_total = 0

    # since_id = "139819805172285849"  # cannot used with start/end_time!

    # borrow from Twitter:
    # https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/master/Full-Archive-Search/full-archive-search.py
    search_url = "https://api.twitter.com/2/tweets/search/all"

    # saved_path = os.path.join(os.getcwd(), "saved_tweets")
    saved_path = r"downloaded_tweets_test"
    raw_tweet_dir = os.path.join(saved_path, 'raw_tweets')
    line_tweet_dir = os.path.join(saved_path, 'line_tweets')
    chunks_dir =  os.path.join(saved_path, 'chunks_tweets')
    cluster_csvs_dir =  os.path.join(saved_path, 'cluster_csvs')
    os.makedirs(raw_tweet_dir, exist_ok=True)
    os.makedirs(line_tweet_dir, exist_ok=True)
    os.makedirs(chunks_dir, exist_ok=True)
    os.makedirs(cluster_csvs_dir, exist_ok=True)


    has_context_annotations = False

    start_timer = time.perf_counter()

    next_token = 'start'
    search_url = "https://api.twitter.com/2/tweets/search/all"
    headers = helper.create_headers(bearer_token)
    total = 0
    query_params = {'query': query, \
                    "max_results": str(max_results), \
                    'expansions': 'attachments.poll_ids,attachments.media_keys,author_id,entities.mentions.username,geo.place_id,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id', \
 \
                    # HAVE context_annotations, max_results can be only 100
                    #'tweet.fields': 'attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,reply_settings,source,text,withheld', \

                    # NO context_annotations,  max_results can be 500
                    'tweet.fields': 'attachments,author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,reply_settings,source,text,withheld', \
                    'place.fields': 'contained_within,country,country_code,full_name,geo,id,name,place_type', \
                    "user.fields": 'created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld', \
                    "media.fields": "duration_ms,height,media_key,preview_image_url,type,url,width,public_metrics", \
                    "poll.fields": "duration_minutes,end_datetime,id,options,voting_status", \
                    # "list.fields": "created_at,description,private,follower_count,member_count,owner_id", \
                    # "space.fields": "created_at,creator_id,created_at,host_ids,lang,is_ticketed,invited_user_ids,participant_count,scheduled_start,speaker_ids,started_at,state,title,updated_at", \
                    # Spaces are ephemeral and become unavailable after they end or when they are canceled by their creator.
                    "start_time": start_time, \
                    "end_time": end_time, \
                    # "since_id":since_id, \  # cannot used with start/end_time!
                    # "until_id": until_id,    # cannot used with start/end_time!
                    }

    if has_context_annotations:
        query_params['tweet.fields'] = query_params['tweet.fields'] + ",context_annotations"
        if max_results > 100:
            print(f"max_results has set to 100 when requesting context_annotations. ")
            max_results = 100
            query_params['query'] = max_results

    tweet_count_total = get_tweet_count(query, start_time, end_time, granularity='day', next_token=None, until_id=None)
    print(f"Found {tweet_count_total} tweets for query: {query}. Period: {start_time} - {end_time}")

    merged_df_list = []
    merge_file_count = round(chunk_size / max_results, 0)
    while next_token != "":
        try:
            json_response = helper.connect_to_endpoint(search_url, headers, query_params)
            next_token = json_response['meta'].get('next_token', "")
            query_params.update({"next_token": next_token})

            is_too_many, elapsed_time = helper.is_too_many_requests(json_response, start_timer)

            if isinstance(json_response, dict): # if have returned tweet
                json_response_list.append(json_response)

            data_filename = save_search(json_response, saved_path)

            df_merged = helper.merge_a_response(data_filename, save_path=line_tweet_dir)

            merged_df_list.append(df_merged)



            total += int(json_response['meta']['result_count'])
            logger.info("Downloaded %s tweets in total." % total)

            tweet_count_total += max_results

            if len(merged_df_list) >= merge_file_count or next_token == "":

                logger.info("Merging a tweets chuck...")
                df_all = pd.concat(merged_df_list)

                lastest_time = df_all['created_at'].max().replace(":", "_")
                oldest_time  = df_all['created_at'].min().replace(":", "_")

                base_name = f"{oldest_time}_{lastest_time}_{len(df_all)}"  # use tweet time as basename

                new_name = os.path.join(chunks_dir, base_name + f"{suffix}")
                # new_name = os.path.join(chunks_dir, base_name)
                df_all = df_all.sort_index(axis=1)

                df_all.to_csv(new_name, index=False)

                logger.info("Finished merging a tweets chuck at: %s" % new_name)

                logger.info("Converting a tweet chunck to cluster format...")

                df_all_cluster = helper.convert_to_cluster(df_all)
                new_name = os.path.join(cluster_csvs_dir, base_name + f"{suffix}")
                df_all_cluster.to_csv(new_name, index=False)

                merged_df_list = []

                logger.info("Finished converting a tweets chuck to cluster format: %s" % new_name)

            if next_token == "":
                print("No next page! Exit.")
                return

        except Exception as e:
            print(json_response)
            logger.error(e, exc_info=True)
            print(e)
            now = time.perf_counter()
            time_window = 1 * 60  # seconds
            elapsed_time = int(now - start_timer)
            need_to_wait_time = time_window - elapsed_time
            print(f'Waiting for {time_window} seconds.')
            time.sleep(time_window)

            continue



token_path = r'tweet_api_keys.txt'

tokens = helper.get_api_token(token_path)

consumer_key = tokens[0]
consumer_secret = tokens[1]
bearer_token = tokens[2]
access_token = tokens[3]
access_token_secret = tokens[4]


if __name__ == '__main__':

    # token_path = r'K:\Research\tweet_downloading\python_code\tweet_api_keys.txt'

    # tokens = helper.get_api_token(token_path)
    # consumer_key = tokens[0]
    # consumer_secret = tokens[1]
    # bearer_token = tokens[2]
    # access_token = tokens[3]
    # access_token_secret = tokens[4]

    saved_path = r'K:\Research\tweet_downloading\python_code\downloaded_tweets_test'
    execute_download(saved_path=saved_path)
    # merge_df = helper.merge_results(saved_path)
