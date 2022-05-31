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
import multiprocessing as mp

import pandas as pd
from datetime import datetime
import helper
from multiprocessing.connection import wait

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

        if 'users' not in list(includes.keys()):
            print("Not users table!")

        return data_filename

    except Exception as e:
        logger.error(e, exc_info=True)


def get_tweet_count(query='place_country:UA',
                    start_time="2021-02-24T00:00:00Z",
                    end_time  ="2022-04-08T00:00:00Z",
                    granularity='day',
                    next_token=None,
                    until_id=None):
    logger.info("Counting tweets, please wait...")

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
        json_response = ''
        try:
            json_response = helper.connect_to_endpoint(endpoint, headers, query_params)
            is_too_many, elapsed_time = helper.is_too_many_requests(json_response, start_timer)
            if is_too_many:
                continue
            next_token = json_response['meta'].get('next_token', None)
            query_params['next_token'] = next_token
            tweet_count = json_response['meta']['total_tweet_count']
            tweet_count_total += tweet_count
            page_cnt += 1

            if page_cnt % 20 == 0:
                logger.info(f"    current tweet count: {tweet_count_total}")
        except Exception as e:

            logger.error("Error in get_tweet_count():", exc_info=True)
            print("json_response:", json_response)
    #         print(f"next_token: {next_token}. total_tweet_count: {tweet_count_total}")

    return tweet_count_total  # , json_response

def merge_a_response_list(data_filename_list, merged_df_list, save_path):
    logger.info("PID %s merge_a_response_list() start!" % os.getpid())

    #
    # print("len(data_filename_list): ", len(data_filename_list))
    # print("len(data_filename_list): ", len(data_filename_list))
    while len(data_filename_list) > 0:
        # print("len(data_filename_list): ", len(data_filename_list))
        data_filename = data_filename_list.pop(0)
        # time.sleep(1)
        try:
            logger.info("PID %s Merging %s" % (os.getpid(), data_filename))
            df_merged = helper.merge_a_response(data_filename, save_path=save_path)
            merged_df_list.append(df_merged)
        except Exception as e:
            logger.error(f"{e}, {data_filename}", exc_info=True)
            print(data_filename)
            continue
    logger.info("PID %s merge_a_response_list() end!" % os.getpid())
    # return df_merged

def convert_to_cluster_process(all_df, new_name):
    logger.info("PID %s convert_to_cluster_process() start!" % os.getpid())
    df = helper.convert_to_cluster(all_df)

    df.to_csv(new_name, index=False)

    logger.info("PID %s Finished converting a tweets chuck to cluster format: %s" % (os.getpid(), new_name))
    logger.info("PID %s convert_to_cluster_process() end!" % os.getpid())


def download_user_tweets():

    user_df = pd.read_csv(r'K:\Research\Ukraine_tweets\2021_2022_users.csv')
    user_df['id_len'] = user_df['userid'].astype(str).str.len()
    user_df = user_df.sort_values('id_len')
    user_df['userid'] = user_df['userid'].astype(str)
    users_list = user_df['userid'].to_list()

    query_len_cap = 1023

    # last_id = '2202066812'   # last_id = -1, if from the begining.
    last_id = -1
    # for idx, user_id in enumerate(feb_users_list):
    processed_cnt = 0


    if last_id != -1:
        processed_id = users_list.pop(0)
        while processed_id != last_id:
            processed_id = users_list.pop(0)
    all_user_cnt = len(users_list)

    while len(users_list) > 0:

        user_id = users_list.pop(0)
        processed_cnt += 1

        current_user_id_len = len(user_id)
        # query = f'has:geo from:{user_id}'
        user_id_list = f"from:{user_id}"

        added_id_len = 0
        while (len(user_id_list) + added_id_len + len('has:geo () OR from:')) < query_len_cap:
            if len(users_list) > 0:
                added_id_len = len(users_list[0])
                added_user_id = users_list.pop(0)
            processed_cnt += 1
            user_id_list = f"{user_id_list} OR from:{added_user_id}"

        query = f'has:geo ({user_id_list})'

        print("Query length:",len(query))


        # start_time = "2021-01-01T00:00:00Z"
        # end_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        # end_time ="2022-03-12T00:00:00Z"

        start_time = "2022-04-21T00:00:00Z"
        end_time   = "2022-05-30T00:00:00Z"

        # saved_path = r"K:\Research\Ukraine_tweets\User_2021_tweets_Ukraine_20220312_20220314"
        # saved_path = r"K:\Research\Ukraine_tweets\Tweets_Ukraine_20220314_20220323"
        saved_path = r"K:\Research\Ukraine_tweets\2021_2022_User_tweets_20220421_20220529"

        execute_download(query,
                         start_time=start_time,
                         end_time=end_time,
                         chunk_size=10000,
                         max_results=500,  # max_results can be 500 if do not request the field: context_annotations
                         saved_path=saved_path,
                         is_zipped=False,
                         )

        logger.info(f"Processed: {processed_cnt} / {all_user_cnt}.")


def download_country_tweet():
    end_time = "2022-05-30T00:00:00Z"
    start_time = "2022-04-21T00:00:00Z"
    # saved_path = r"K:\Research\Ukraine_tweets\User_2021_tweets_Ukraine_20220312_20220314"
    saved_path = r"K:\Research\Ukraine_tweets\Tweets_Ukraine_20220421_20220529"

    query = f'place_country:UA'

    # start_time = "2022-01-01T00:00:00Z"
    # end_time =   "2022-03-08T00:00:00Z"
    # expected_tweet_count_total = get_tweet_count(query, start_time, end_time, granularity='day', next_token=None, until_id=None)
    # print(f"\nFound {expected_tweet_count_total} tweets for query: {query}. Period: {start_time} - {end_time}\n")
    # Found 276303 tweets for query: place_country:UA. Period: 2022-01-01T00:00:00Z - 2022-03-08T00:00:00Z

    execute_download(query,
                     start_time=start_time,
                     end_time=end_time,
                     chunk_size=10000,
                     max_results=500,  # max_results can be 500 if do not request the field: context_annotations
                     saved_path=saved_path,
                     is_zipped=False,
                     )


def execute_download(query,
                     start_time,
                     end_time,
                     chunk_size=10000,
                     max_results=500,  # max_results can be 500 if do not request the field: context_annotations
                     saved_path=r"downloaded_tweets_Ukraine_20190101_20211231",
                     is_zipped=False,
                     ):
    """":argument

    main workflow:
    1. parameter set up
    2. get the first response.
    3. pagnation: process each page using the returned page_token. The Twitter server needs about 3 seconds to return response.
    4. data processing:
        1) save the raw data of each page (0.3 seconds per response);
        2) use a sub-process to merge the five parts of tweets data (1.4 seconds per response, paralleling with the main process);
        3) the main process merge several response CSV files to a chunk
        4) use a sub-process to convert the chunk CSV to cluster format.

    rate limit: 520,000 tweet/hour (without context_annotation)

    """


    # tweets

    # Set the save path

    ###############################################################


    if is_zipped:
        suffix = '.csv.gz'
    else:
        suffix = '.csv'


    raw_tweet_dir = os.path.join(saved_path, 'raw_tweets')
    line_tweet_dir = os.path.join(saved_path, 'line_tweets')
    chunks_dir =  os.path.join(saved_path, 'chunks_tweets')
    cluster_csvs_dir =  os.path.join(saved_path, 'cluster_csvs')
    os.makedirs(raw_tweet_dir, exist_ok=True)
    os.makedirs(line_tweet_dir, exist_ok=True)
    os.makedirs(chunks_dir, exist_ok=True)
    os.makedirs(cluster_csvs_dir, exist_ok=True)


    has_context_annotations = False

    # since_id = "139819805172285849"  # cannot used with start/end_time!
    # until_id = '1139156316075757568'

    # borrow from Twitter:
    # https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/master/Full-Archive-Search/full-archive-search.py


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

    logger.info("PID %s execute_download() start!" % os.getpid())

    expected_tweet_count_total = get_tweet_count(query, start_time, end_time, granularity='day', next_token=None, until_id=None)
    logger.info(f"Found {expected_tweet_count_total} tweets for query: {query}. Period: {start_time} - {end_time}")
    print(f"\nFound {expected_tweet_count_total} tweets for query: {query}. Period: {start_time} - {end_time}\n")

    if expected_tweet_count_total == 0:
        logger.info("Exit: no tweets.")
        return

    tweet_count_total = 0

    merge_file_count = round(chunk_size / max_results, 0)
    merge_file_count = int(merge_file_count)

    data_filename_list_mp = mp.Manager().list()
    merged_df_list_mp = mp.Manager().list()

    t0 = time.perf_counter()

    # 1) request response
    while next_token != "":
        try:
            #
            json_response = helper.connect_to_endpoint(search_url, headers, query_params)
            is_too_many, elapsed_time = helper.is_too_many_requests(json_response, start_timer)
            if is_too_many: # if request too many times, wait for 1 minute.
                continue
            # needs about 2 - 3 seconds. Hard to accelerate.
            # print("response time (second): ", time.perf_counter() - t1)

            next_token = json_response['meta'].get('next_token', "")
            query_params.update({"next_token": next_token})




            # save the raw data of each page (0.3 seconds per response)
            data_filename = save_search(json_response, saved_path)


            # t2 = time.perf_counter()
            data_filename_list_mp.append(data_filename)  # put the name of the saved file to a list for further merging.
            # _mp means it is a shared list for multiprocessing.

            # df_merged = helper.merge_a_response(data_filename, save_path=line_tweet_dir)
            # df_merged = merge_a_response_list(data_filename_list_mp, merged_df_list=merged_df_list_mp, save_path=line_tweet_dir)

            # merged_df_list_mp.append(df_merged)

            total += int(json_response['meta']['result_count'])
            logger.info("Downloaded %s tweets in total." % total)

            tweet_count_total += max_results

            if len(data_filename_list_mp) > 5 or next_token == "":
                # 2) use a sub-process to merge the five parts of tweets data (1.4 seconds per response, paralleling with the main process);
                # merge the five parts of a response in data_filename_list_mp, then put the result into merged_df_list_mp
                # the merge file is stored in folder: line_tweet_dir
                merge_process = mp.Process(target=merge_a_response_list,
                                           args=(data_filename_list_mp, merged_df_list_mp, line_tweet_dir))
                merge_process.start()
                speed = tweet_count_total / (time.perf_counter() - t0) * 3600  # per hour
                logger.info("Speed: %.0f tweet/hour" % speed)

            # print("merge time (second): ", time.perf_counter() - t2)
            # Merging time is about 1.4 seconds .

            # print("request and merge time (second): ", time.perf_counter() - t1)  # < 3.0 seconds using multi-processing


            if len(merged_df_list_mp) >= merge_file_count or next_token == "":
                # use a sub-process to merge several response CSV files to a chunk and to convert the chunk to cluster format.
                logger.info("Merging a tweets chuck...")
                logger.info("data_filename_list_mp lengh: %d" % len(data_filename_list_mp))
                logger.info("merged_df_list lengh: %d" % len(merged_df_list_mp))
                if next_token == "":  # the last several response, not enough for the merge_file_count
                    merge_a_response_list(data_filename_list_mp, merged_df_list_mp, line_tweet_dir)
                    # df_all = pd.concat(merged_df_list)

                merged_df_list = []  # 3) make a chunk in the main process
                # pop  merged_df from merged_df_list_mp until the number is up to merge_file_count
                for i in range(int(merge_file_count)):
                    if len(merged_df_list_mp) > 0:
                        merged_df_list.append(merged_df_list_mp.pop(0))

                if len(merged_df_list) == 0:
                    logger.info("No tweets return")
                    continue

                df_all = pd.concat(merged_df_list)

                # time.sleep(3) # wait for the merging process.
                # while len(data_filename_list_mp) != 0:
                    # logger.info("Waiting for merging responses: %d left." % len(data_filename_list_mp))
                    # merge_process.wait()
                # wait(merged_df_list_mp)

                lastest_time = df_all['created_at'].max().replace(":", "_")
                oldest_time  = df_all['created_at'].min().replace(":", "_")
                base_name = f"{oldest_time}_{lastest_time}_{len(df_all)}"  # use tweet time as basename
                new_name = os.path.join(chunks_dir, base_name + f"{suffix}")
                df_all = df_all.sort_index(axis=1)

                df_all.to_csv(new_name, index=False)

                logger.info("Finished merging a tweets chuck at: %s" % new_name)

                logger.info("Converting a tweet chunk to cluster format...")

                # converted_df_list = mp.Manager().list()
                new_name = os.path.join(cluster_csvs_dir, base_name + f"{suffix}")

                # 4) use a sub-process to  convert the chunk to cluster format.
                convert_process = mp.Process(target=convert_to_cluster_process,
                                           args=(df_all,  new_name))
                convert_process.start()
                if next_token == "":
                    convert_process.join()  # the main process will wait for the sub-process
                # df_all_cluster = converted_df_list[0]
                # df_all_cluster = helper.convert_to_cluster(df_all)

                speed = tweet_count_total / (time.perf_counter() - t0) * 3600 # per hour
                logger.info("Speed: %.0f tweet/hour" % speed )

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

def execute_download0(
                    is_zipped=False,
                     ):
    """":argument

    main workflow:
    1. parameter set up
    2. get the first response.
    3. pagnation: process each page using the returned page_token. The Twitter server needs about 3 seconds to return response.
    4. data processing:
        1) save the raw data of each page (0.3 seconds per response);
        2) use a sub-process to merge the five parts of tweets data (1.4 seconds per response, paralleling with the main process);
        3) the main process merge several response CSV files to a chunk
        4) use a sub-process to convert the chunk CSV to cluster format.

    rate limit: 520,000 tweet/hour (without context_annotation)

    """

    ############## SET YOUR PARAMERTERS HERE ######################
    # query = "telemedicine  OR telehealth  OR telecare"
    # keywords = ' OR '.join(["suicide",
    #                    "suicidal",
    #                    "shoot myself",
    #                    "a gun to head",
    #                    "hang myself",
    #                    "intention to die",
    #                    "hurt myself",
    #                    "cut myself",
    #                    "leave this world",
    #                    "wanna die",
    #                    "deserve to die",
    #                    "desire to end own life",
    #                    "kill myself",
    #                    "self harm",
    #                    "want death",
    #                    "take my life",
    #                    "depression",
    #                    "want to die",
    #                    ])     # Found 77058632 tweets for query
    # query = f"({keywords}) place_country:US " # Found 306438 tweets for query:

    # query = f"({keywords}) place_country:US has:geo"  # Found 306440 tweets for query:
    # NOTE: () is required!

    # query = '#mangrove OR #mangroves'   #  Found 142011 tweets for query: #mangrove OR #mangroves. Period: 2019-01-01T00:00:00Z - 2022-01-01T00:00:00Z
    # query = 'mangrove OR mangroves'   # Found 1435688 tweets for query: mangrove OR mangroves. Period: 2019-01-01T00:00:00Z - 2022-01-01T00:00:00Z
    # query = 'mangrove'    # Found 943147 tweets for query: mangrove. Period: 2019-01-01T00:00:00Z - 2022-01-01T00:00:00Z
    # query = 'mangroves'   # Found 594896 tweets for query: mangroves. Period: 2019-01-01T00:00:00Z - 2022-01-01T00:00:00Z
    # query = '(#mangrove OR #mangroves)'  # Found 142003 tweets for query: (#mangrove OR #mangroves). Period: 2019-01-01T00:00:00Z - 2022-01-01T00:00:00Z
    query = 'place_country:UA'

# start_time = "2020-12-05T09:25:03Z"
    start_time = "2019-01-01T00:00:00Z"
    end_time   = "2022-01-01T00:00:00Z"
    # end_time   = "2021-12-01T00:00:00.000Z"
    # end_time = "2021-07-17T04_51_53.000Z".replace("_", ":")

    max_results = 500  # max_results can be 500 if do not request the field: context_annotations
    chunk_size = 10000  # tweets

    # Set the save path
    saved_path = r"downloaded_tweets_Ukraine_20190101_20211231"
    ###############################################################


    if is_zipped:
        suffix = '.csv.gz'
    else:
        suffix = '.csv'




    raw_tweet_dir = os.path.join(saved_path, 'raw_tweets')
    line_tweet_dir = os.path.join(saved_path, 'line_tweets')
    chunks_dir =  os.path.join(saved_path, 'chunks_tweets')
    cluster_csvs_dir =  os.path.join(saved_path, 'cluster_csvs')
    os.makedirs(raw_tweet_dir, exist_ok=True)
    os.makedirs(line_tweet_dir, exist_ok=True)
    os.makedirs(chunks_dir, exist_ok=True)
    os.makedirs(cluster_csvs_dir, exist_ok=True)


    has_context_annotations = False

    # since_id = "139819805172285849"  # cannot used with start/end_time!
    # until_id = '1139156316075757568'

    # borrow from Twitter:
    # https://github.com/twitterdev/Twitter-API-v2-sample-code/blob/master/Full-Archive-Search/full-archive-search.py


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

    logger.info("PID %s execute_download() start!" % os.getpid())

    expected_tweet_count_total = get_tweet_count(query, start_time, end_time, granularity='day', next_token=None, until_id=None)
    logger.info(f"Found {expected_tweet_count_total} tweets for query: {query}. Period: {start_time} - {end_time}")
    print(f"\nFound {expected_tweet_count_total} tweets for query: {query}. Period: {start_time} - {end_time}\n")
    tweet_count_total = 0

    merge_file_count = round(chunk_size / max_results, 0)
    merge_file_count = int(merge_file_count)

    data_filename_list_mp = mp.Manager().list()
    merged_df_list_mp = mp.Manager().list()

    t0 = time.perf_counter()

    # 1) request response
    while next_token != "":
        try:
            #
            json_response = helper.connect_to_endpoint(search_url, headers, query_params)
            # needs about 2 - 3 seconds. Hard to accelerate.
            # print("response time (second): ", time.perf_counter() - t1)

            next_token = json_response['meta'].get('next_token', "")
            query_params.update({"next_token": next_token})

            # if request too many times, wait for 1 minute.
            is_too_many, elapsed_time = helper.is_too_many_requests(json_response, start_timer)

            # save the raw data of each page (0.3 seconds per response)
            data_filename = save_search(json_response, saved_path)


            # t2 = time.perf_counter()
            data_filename_list_mp.append(data_filename)  # put the name of the saved file to a list for further merging.
            # _mp means it is a shared list for multiprocessing.

            # df_merged = helper.merge_a_response(data_filename, save_path=line_tweet_dir)
            # df_merged = merge_a_response_list(data_filename_list_mp, merged_df_list=merged_df_list_mp, save_path=line_tweet_dir)

            # merged_df_list_mp.append(df_merged)

            total += int(json_response['meta']['result_count'])
            logger.info("Downloaded %s tweets in total." % total)

            tweet_count_total += max_results

            if len(data_filename_list_mp) > 5:
                # 2) use a sub-process to merge the five parts of tweets data (1.4 seconds per response, paralleling with the main process);
                # merge the five parts of a response in data_filename_list_mp, then put the result into merged_df_list_mp
                # the merge file is stored in folder: line_tweet_dir
                merge_process = mp.Process(target=merge_a_response_list,
                                           args=(data_filename_list_mp, merged_df_list_mp, line_tweet_dir))
                merge_process.start()
                speed = tweet_count_total / (time.perf_counter() - t0) * 3600  # per hour
                logger.info("Speed: %.0f tweet/hour" % speed)

            # print("merge time (second): ", time.perf_counter() - t2)
            # Merging time is about 1.4 seconds .

            # print("request and merge time (second): ", time.perf_counter() - t1)  # < 3.0 seconds using multi-processing


            if len(merged_df_list_mp) >= merge_file_count or next_token == "":
                # use a sub-process to merge several response CSV files to a chunk and to convert the chunk to cluster format.
                logger.info("Merging a tweets chuck...")
                logger.info("data_filename_list_mp lengh: %d" % len(data_filename_list_mp))
                logger.info("merged_df_list lengh: %d" % len(merged_df_list_mp))
                if next_token == "":  # the last several response, not enough for the merge_file_count
                    merge_a_response_list(data_filename_list_mp, merged_df_list_mp, line_tweet_dir)
                    # df_all = pd.concat(merged_df_list)

                merged_df_list = []  # 3) make a chunk in the main process
                # pop  merged_df from merged_df_list_mp until the number is up to merge_file_count
                for i in range(int(merge_file_count)):
                    if len(merged_df_list_mp) > 0:
                        merged_df_list.append(merged_df_list_mp.pop(0))

                df_all = pd.concat(merged_df_list)

                # time.sleep(3) # wait for the merging process.
                # while len(data_filename_list_mp) != 0:
                    # logger.info("Waiting for merging responses: %d left." % len(data_filename_list_mp))
                    # merge_process.wait()
                # wait(merged_df_list_mp)

                lastest_time = df_all['created_at'].max().replace(":", "_")
                oldest_time  = df_all['created_at'].min().replace(":", "_")
                base_name = f"{oldest_time}_{lastest_time}_{len(df_all)}"  # use tweet time as basename
                new_name = os.path.join(chunks_dir, base_name + f"{suffix}")
                df_all = df_all.sort_index(axis=1)

                df_all.to_csv(new_name, index=False)

                logger.info("Finished merging a tweets chuck at: %s" % new_name)

                logger.info("Converting a tweet chunk to cluster format...")

                # converted_df_list = mp.Manager().list()
                new_name = os.path.join(cluster_csvs_dir, base_name + f"{suffix}")

                # 4) use a sub-process to  convert the chunk to cluster format.
                convert_process = mp.Process(target=convert_to_cluster_process,
                                           args=(df_all,  new_name))
                convert_process.start()
                if next_token == "":
                    convert_process.join()  # the main process will wait for the sub-process
                # df_all_cluster = converted_df_list[0]
                # df_all_cluster = helper.convert_to_cluster(df_all)

                speed = tweet_count_total / (time.perf_counter() - t0) * 3600 # per hour
                logger.info("Speed: %.0f tweet/hour" % speed )

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
    # execute_download()
    # download_country_tweet()
    download_user_tweets()

    # data_filename_list = list(range(10))
    # print(get_tweet_count())
    # merged_df_list = []
    # save_path = "test"
    # merge_a_response_list(data_filename_list, merged_df_list, save_path)
    # merge_df = helper.merge_results(saved_path)
