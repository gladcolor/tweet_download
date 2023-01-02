import os
from tqdm import tqdm
import pandas as pd

import download_tweet_APIv2 as v2


def sampling_download(year=2021):
    query = "-is:retweet lang:ja (ノ OR の)",  # meaning: and,的  # (ノ OR の) これ
    save_path = fr'P:\Shared drives\T5\Japan_Tweets_2021_01_01_2022_01_01\Japan_tweets\sampling_tweets\{year}'
    os.makedirs(save_path, exist_ok=True)
    start_time = pd.to_datetime(f'{year}-01-01')
    end_time   = pd.to_datetime(f'{year + 1}-01-01')
    time_interval = '500min'
    time_slots = pd.date_range(start_time, end_time, freq=time_interval)
    print(f"Generated {len(time_slots)} time slots: {time_slots[0]} - {time_slots[-1]}")
    slot_width_min = 1
    try:
        for start in tqdm(time_slots[:]):
            end = start + pd.Timedelta(minutes=slot_width_min)
            start = start.strftime("%Y-%m-%dT%H:%M:%SZ")
            end = end.strftime("%Y-%m-%dT%H:%M:%SZ")
            print(start, end)
            v2.execute_download(query=query,
                                start_time=start,
                                end_time=end,
                                chunk_size=10000,
                                max_results=500,
                                # max_results can be 500 if do not request the field: context_annotations
                                saved_path=save_path,
                                )
    except Exception as e:
        print("Error in sampling_download:", e)


if __name__ == '__main__':
    years = range(2022, 2012, -1)
    print("years:", list(years))
    for year in years:
        sampling_download(year=year)