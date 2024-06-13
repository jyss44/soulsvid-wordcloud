import argparse
import json
from collections import namedtuple
from pathlib import Path
import csv

from count_phrases import count_phrases, aggregate_phrases
from count_words import count_words
from get_transcripts import get_youtube_api, get_videos_from_channel, get_transcripts, process_transcript
import time
from datetime import timedelta

from yt_transcript_analyzer.stem_words import collect_stems

Channel = namedtuple('Channel', ['id', 'name'])


def get_argparser(parser: argparse.ArgumentParser = None) -> argparse.ArgumentParser:
    """
    Gets argparser for transcript analyzer
    :param parser: parser
    :return: parser with necessary arguments
    """
    if not parser:
        parser = argparse.ArgumentParser(prog="YouTubeTranscriptAnalyzer")

    parser.add_argument("channels", help="csv file of channel IDs and names", type=lambda p: Path(p).absolute())
    parser.add_argument("key", help="Google API key", type=lambda p: Path(p).absolute())
    parser.add_argument("-m", "--max-phrase-len", help="maximum phrase length", type=int, default=4)
    parser.add_argument("-o", "--out", help="Sets directory for output files", default=Path('../../data').absolute(), type=lambda p: Path(p).absolute())
    parser.add_argument('-r', '--refresh', help='Redownloads transcripts', action='store_true')

    return parser


def read_channels_file(filename):
    ret = []
    with open(filename, 'r') as file:
        csv_file = csv.reader(file)
        for line in csv_file:
            ret.append(Channel(*line))
    return ret


if __name__ == '__main__':
    args = get_argparser().parse_args()
    data_dir = args.out
    client_secrets_file = args.key
    channels = read_channels_file(args.channels)
    refresh = args.refresh

    data_dir.mkdir(exist_ok=True)
    main_start = time.time()

    yt = None

    print("==========================================================================")
    for channel in channels:
        channel_start = time.time()
        print(f"Channel name: {channel.name}")

        channel_dir = data_dir / channel.name
        video_info_file = channel_dir / f'{channel.name}_videos.json'
        transcript_file = channel_dir / f'{channel.name}_transcripts.json'
        words_file = channel_dir / f'{channel.name}_words.json'
        stems_file = channel_dir / f'{channel.name}_stems.json'
        agg_phrases_file = channel_dir / f'{channel.name}_phrases.json'

        channel_dir.mkdir(exist_ok=True)

        # Get video info
        if video_info_file.exists() and not refresh:
            with open(video_info_file, 'r') as file:
                videos = json.load(file)
        else:
            if not yt:
                yt = get_youtube_api(client_secrets_file)
            start = time.time()
            print("--------------------------------------------------------------------------")
            print("Getting channel videos...")
            videos = get_videos_from_channel(channel.id, youtube=yt, filename=video_info_file)
            print(f"Done in {timedelta(seconds=time.time() - start)} seconds!")

        # Get transcripts
        if not transcript_file.exists() and not refresh:
            video_ids = [video["id"]["videoId"] for video in videos if video["id"]["kind"] == "youtube#video"]
            start = time.time()
            print("--------------------------------------------------------------------------")
            print("Getting video transcripts...")
            transcripts_json = get_transcripts(video_ids, transcript_file)
            print(f"{len(transcripts_json)} transcripts found out of {len(videos)} videos.")
            print(f"Done in {timedelta(seconds=time.time() - start)} seconds!")
        else:
            with open(transcript_file, 'r') as file:
                transcripts_json = json.load(file)

        transcripts = process_transcript(transcripts_json)

        start = time.time()
        print("--------------------------------------------------------------------------")
        print("Counting words...")
        words = count_words(transcripts, words_file)
        print(f"Done in {timedelta(seconds=time.time() - start)} seconds!")

        start = time.time()
        print("--------------------------------------------------------------------------")
        print("Counting words by common stem...")
        stems = collect_stems(words, stems_file)
        print(f"Done in {timedelta(seconds=time.time() - start)} seconds!")

        # Get phrases
        start = time.time()
        print("--------------------------------------------------------------------------")
        print("Counting phrases...")

        phrases = []

        for phrase_len in range(2, 5):
            phrases_file = channel_dir / f'{channel.name}_phrases{phrase_len}.json'
            phrases.append(count_phrases(transcripts, phrases_file, phrase_length=phrase_len))

        agg_phrases = aggregate_phrases(phrases, agg_phrases_file)

        print(f"{agg_phrases.shape[0]} phrases analysed in {timedelta(seconds=time.time() - start)} seconds!")

        print("--------------------------------------------------------------------------")
        print(f"{len(transcripts)} transcripts analysed from channel {channel.name} in {timedelta(seconds=time.time() - channel_start)} seconds!")
        print("==========================================================================")

    print(f"{len(channels)} analysed in {timedelta(seconds=time.time() - main_start)} seconds!")
