import json
import os
from collections import Counter
from pathlib import Path
from typing import Union

import nltk
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
from tqdm import tqdm
from wordfreq import tokenize, get_frequency_dict, freq_to_zipf

from yt_transcript_analyzer.get_transcripts import process_transcript

STOPWORDS = stopwords.words('english')
WORD_FREQS = get_frequency_dict('en', wordlist='large')


def freq_to_zipf_mod(x: float) -> float:
    try:
        return freq_to_zipf(x)
    except ValueError:
        return np.nextafter(np.float32(0), np.float32(1))


def count_words(transcripts: list[list[str]], word_file: Union[str, bytes, os.PathLike] = None,
                thresh: float = 5.5) -> pd.DataFrame:
    word_counter = Counter()

    for video in tqdm(transcripts, desc="Extracting words from transcripts..."):
        tokens = pd.Series(tokenize(' '.join(video).lower(), lang='en'), dtype=str)
        tokens_tagged = nltk.pos_tag(tokens, tagset='universal')
        for token in tokens_tagged:
            if token[0] not in STOPWORDS:
                word_counter[token] += 1

    total = word_counter.total()

    data = [(word, word_type, count) for (word, word_type), count in word_counter.most_common()]

    word_df = pd.DataFrame(data, columns=['word', 'type', 'count'])
    word_df['word_freq'] = word_df['word'].apply(lambda word: WORD_FREQS.get(word, 0))

    # Trim
    word_df = word_df[word_df['word_freq'] > 0]
    word_df['word_freq'] = word_df['word_freq']
    word_df = word_df[word_df['word_freq'] < thresh]
    word_df['relative_freq'] = (word_df['count'] / total)
    word_df['freq_delta'] = (word_df['relative_freq'] - word_df['word_freq']) / word_df['word_freq']
    tqdm.pandas(desc='Computing word zipf frequency')
    word_df['word_freq'] = word_df['word_freq'].progress_apply(freq_to_zipf_mod)
    tqdm.pandas(desc='Computing relative zipf frequency')
    word_df['relative_freq'] = word_df['relative_freq'].progress_apply(freq_to_zipf_mod)

    word_df = word_df.reset_index(drop=True)

    list_of_series = [
        pd.Series(range(1, word_df.shape[0] + 1), name='rank', dtype=int),
        pd.Series(range(1, word_df.shape[0] + 1), name='type_rank', dtype=int),
        word_df
    ]
    write_df = pd.concat(list_of_series, axis=1)

    for word_type in write_df['type'].unique():
        write_df.loc[write_df['type'] == word_type, 'type_rank'] = range(1, write_df[write_df['type'] == word_type].shape[0] + 1)


    if word_file:
        with open(word_file, 'w') as file:
            write_df.to_json(path_or_buf=file, orient="records", indent=4)

    print(f"No words: {len(list(word_counter))}. Total count: {total}")

    return write_df


def main():
    channel_name = 'tarnished_archaeologist'
    channel_dir = Path('../../data') / channel_name
    transcript_file = channel_dir / f'{channel_name}_transcripts.json'
    word_file = channel_dir / f'{channel_name}_words.json'

    with open(transcript_file, 'r') as file:
        transcripts_json = json.load(file)

    transcripts = process_transcript(transcripts_json)

    count_words(transcripts, word_file)


if __name__ == '__main__':
    main()
