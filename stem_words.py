import os
from pathlib import Path
from typing import Union

import pandas as pd
from nltk.stem import PorterStemmer
from tqdm import tqdm

from count_words import freq_to_zipf_mod

ps = PorterStemmer()


def collect_stems(words_df: pd.DataFrame, file_name: Union[str, bytes, os.PathLike]=None) -> pd.DataFrame:
    tqdm.pandas(desc='Extracting stems')
    words_df['stem'] = words_df['word'].progress_apply(ps.stem)
    stems_group = words_df.groupby(['stem', 'type'], sort=False)
    stems_df = stems_group.sum().reset_index()

    total = stems_df['count'].sum()

    tqdm.pandas(desc='Gathering common stems')
    stems_df['word'] = stems_group['word'].progress_apply(tuple).reset_index()['word']

    tqdm.pandas(desc='Recomputing relative frequency')
    stems_df['relative_freq'] = (stems_df['count'] / total).progress_apply(freq_to_zipf_mod)
    stems_df['word_freq'] = stems_group['word_freq'].median().reset_index()['word_freq']
    stems_df['freq_delta'] = (stems_df['relative_freq'] - stems_df['word_freq']) / stems_df['word_freq']
    stems_df = stems_df.sort_values(by='count', ascending=False).reset_index(drop=True)

    stems_df = stems_df.drop(['rank', 'type_rank'], axis=1)
    rank = pd.Series(range(1, stems_df.shape[0] + 1), name='rank', dtype=int)
    type_rank = pd.Series(range(1, stems_df.shape[0] + 1), name='type_rank', dtype=int)

    list_of_series = [
        rank,
        type_rank,
        stems_df
    ]
    write_df = pd.concat(list_of_series, axis=1)

    for word_type in stems_df['type'].unique():
        write_df.loc[write_df['type'] == word_type, 'type_rank'] = range(1, write_df[write_df['type'] == word_type].shape[0] + 1)

    if file_name:
        with open(file_name, 'w') as file:
            write_df.to_json(path_or_buf=file, orient="records", indent=4)
    unusuality_index = write_df['freq_delta'].abs().mean()
    print(f"Unusuality index: {unusuality_index}")

    return write_df


if __name__ == '__main__':
    channel_name = 'tarnished_archaeologist'
    channel_dir = Path('data') / channel_name

    words_file = channel_dir / f'{channel_name}_words.json'
    stems_file = channel_dir / f'{channel_name}_stems.json'

    words = pd.read_json(words_file)
    stems = collect_stems(words, stems_file)
