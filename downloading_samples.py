import os
from tqdm import tqdm
import pandas as pd

import download_tweet_APIv2 as v2


def sampling_download(year=2021):
    query = "lang:ja (ノ OR の)",  # meaning: and,的  # (ノ OR の) これ
    save_path = fr'H:\Research\Japan_tweets\sampling_tweets\{year}_have_retweets'
    os.makedirs(save_path, exist_ok=True)
    start_time = pd.to_datetime(f'{year}-01-01')
    end_time   = pd.to_datetime(f'{year + 1}-01-01')
    # start_time = pd.to_datetime("2022-12-06T14:00:00Z")
    # end_time = pd.to_datetime("2023-01-01T00:00:00Z")
    time_interval = '1000min'
    time_slots = pd.date_range(start_time, end_time, freq=time_interval)

    slot_width_min = 1

    skip_cnt = 19 # 159
    end_cnt =  len(time_slots) #  158 #  438  # len(time_slots)
    time_slots = time_slots[skip_cnt:end_cnt]

    print(f"Generated {len(time_slots)} time slots: {time_slots[0]} - {time_slots[-1]}")
    processed_slot_cnt = 0   # + skip_cnt
    try:
        for start in tqdm(time_slots[:]):
            end = start + pd.Timedelta(minutes=slot_width_min)
            start = start.strftime("%Y-%m-%dT%H:%M:%SZ")
            end = end.strftime("%Y-%m-%dT%H:%M:%SZ")
            print(f'{processed_slot_cnt} / {len(time_slots)}', start, end)
            v2.execute_download(query=query,
                                start_time=start,
                                end_time=end,
                                chunk_size=10000,
                                max_results=500,
                                # max_results can be 500 if do not request the field: context_annotations
                                saved_path=save_path,
                                )
            processed_slot_cnt += 1

    except Exception as e:
        print("Error in sampling_download:", e)


if __name__ == '__main__':
    years = range(2020, 2019, -1)
    print("years:", list(years))
    for year in years:
        sampling_download(year=year)