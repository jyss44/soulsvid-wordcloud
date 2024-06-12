# -*- coding: utf-8 -*-
import argparse
import json
# Sample Python code for youtube.channels.list
# See instructions for running these code samples locally:
# https://developers.google.com/explorer-help/code-samples#python

import os
from pathlib import Path
from typing import Union

import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors

from youtube_transcript_api import YouTubeTranscriptApi

scopes = ["https://www.googleapis.com/auth/youtube.readonly"]


def get_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="GetTranscripts")
    parser.add_argument("key", help="Google API key", type=lambda p: Path(p).absolute())

    return parser


def get_youtube_api(secrets_filename: Union[str, bytes, os.PathLike]) -> googleapiclient.discovery.Resource:
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"

    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        secrets_filename, scopes)
    credentials = flow.run_local_server()
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials)

    return youtube


def get_videos_from_channel(channel_id: str, youtube: googleapiclient.discovery.Resource = None, filename: Union[str, bytes, os.PathLike] = None):
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.

    if not youtube:
        youtube = get_youtube_api()

    request = youtube.search().list(
        part="id",
        channelId=channel_id,
        maxResults=500,
        order='date'
    )

    ret = []

    while request:
        response = request.execute()
        ret += response['items']

        request = youtube.search().list_next(request, response)

    if filename:
        with open(filename, 'w') as file:
            json.dump(ret, file, indent=4)

    return ret


def process_transcript(transcripts: dict[str, list[dict[str]]]) -> list[list[str]]:
    video_transcripts = []

    for video in transcripts.values():
        transcript = []

        for line in video:
            transcript.append(line['text'].replace(u'\xa0', u' ').replace(u'\n', u' ').replace(u'\u2019', u' '))

        video_transcripts.append(transcript)

    return video_transcripts


def get_transcripts(video_ids: list[str], filename: Union[str, bytes, os.PathLike] = None) -> dict[str, list[dict[str]]]:
    transcript = YouTubeTranscriptApi.get_transcripts(video_ids, continue_after_error=True)

    if filename:
        with open(filename, 'w') as file:
            json.dump(transcript[0], file, indent=4)

    return transcript[0]


if __name__ == "__main__":
    args = get_argparser().parse_args()

    channel = 'UCcIpxEgQbZSJNe20jbyKJFQ'
    video_info_file = 'data/tarnished_archeologist/tarnished_archeologist_videos.json'
    transcripts_file = 'data/tarnished_archeologist/tarnished_archeologist_transcript.json'

    yt = get_youtube_api(args.key)
    videos = get_videos_from_channel(channel, youtube=yt, filename=video_info_file)

    # with open(video_info_file, 'r') as file:
    #     videos = json.load(file)

    video_ids = [video["id"]["videoId"] for video in videos if video["id"]["kind"] == "youtube#video"]

    transcript = YouTubeTranscriptApi.get_transcripts(video_ids, continue_after_error=True)

    with open(transcripts_file, 'w') as file:
        json.dump(transcript[0], file, indent=4)

    print(transcript[0])
