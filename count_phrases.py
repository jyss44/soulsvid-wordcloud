import json
import os
import string
from collections import Counter
from collections.abc import Iterable
from pathlib import Path
from typing import Union

from nltk import word_tokenize
from nltk.util import ngrams
from nltk.corpus import stopwords

import pandas as pd
from wordfreq import freq_to_zipf

from get_transcripts import process_transcript
from tqdm import tqdm

STOPWORDS = stopwords.words('english')


def extract_phrases(text: str, phrase_length: int = 3) -> Iterable:
    tokens = word_tokenize(text)
    phrases = ngrams(tokens, phrase_length)

    return phrases


def phrase_is_bad(phrase: tuple[str]) -> bool:
    stopword_count = 0
    for word in phrase:
        if len(word) == 1 and word in string.punctuation:
            return True
        if word in STOPWORDS:
            stopword_count += 1

    if stopword_count >= len(phrase):
        return True

    return False


def count_phrases(transcripts, phrases_file: Union[str, bytes, os.PathLike] = None, phrase_length=3) -> pd.DataFrame:
    phrase_counter = Counter()

    for video in tqdm(transcripts, desc='Extracting phrases from transcripts...'):
        phrases = extract_phrases(' '.join(video), phrase_length=phrase_length)
        for phrase in phrases:
            phrase_counter[phrase] += 1

    phrases_df = pd.DataFrame(phrase_counter.most_common(), columns=['phrase', 'count'])
    phrases_df['str'] = phrases_df.apply(lambda row: ' '.join(row['phrase']), axis=1)

    tqdm.pandas(desc='Trimming phrases')
    phrases_df = phrases_df[~phrases_df['phrase'].progress_apply(phrase_is_bad)]
    total = phrase_counter.total()

    tqdm.pandas(desc='Computing phrase frequency')

    rank = pd.Series(range(1, phrases_df.shape[0] + 1), name='rank', dtype=int)
    phrase_len = phrases_df['str'].apply(lambda s: len(s.split())).rename('phrase_len').reset_index(drop=True)

    list_of_series = [
        rank,
        phrase_len,
        phrases_df['str'].rename('phrase').reset_index(drop=True),
        phrases_df['count'].reset_index(drop=True),
        (phrases_df['count'] / total).progress_apply(freq_to_zipf).rename('relative_freq').reset_index(drop=True)
    ]
    write_df = pd.concat(list_of_series, axis=1)

    if phrases_file:
        with open(phrases_file, 'w') as file:
            write_df.to_json(path_or_buf=file, orient="records", indent=4)

    print(f"No phrases: {len(list(phrase_counter))}. Total count: {total}")

    return write_df


def aggregate_phrases(dataframes: list[pd.DataFrame], phrases_file=None) -> pd.DataFrame:
    agg_df = pd.concat(dataframes).sort_values(by='relative_freq', ascending=False)
    agg_df['rank'] = pd.Series(range(1, agg_df.shape[0] + 1), name='rank', dtype=int).values

    if phrases_file:
        with open(phrases_file, 'w') as file:
            agg_df.to_json(path_or_buf=file, orient="records", indent=4)

    return agg_df


def main():
    channel_name = 'tarnished_archaeologist'
    channel_dir = Path('data') / channel_name
    transcript_file = channel_dir / f'{channel_name}_transcripts.json'
    phrases_file = channel_dir / f'{channel_name}_phrases.json'

    with open(transcript_file, 'r') as file:
        transcripts_json = json.load(file)

    transcripts = process_transcript(transcripts_json)
    count_phrases(transcripts, phrases_file)


if __name__ == '__main__':
    main()
