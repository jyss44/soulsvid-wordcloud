import argparse
import warnings
from pathlib import Path

from pandasgui import show
import pandas as pd
from tqdm import tqdm


def get_argparser(parser=None) -> argparse.ArgumentParser:
    if not parser:
        parser = argparse.ArgumentParser(prog="show_transcript_data")
    parser.add_argument("files", help="json files", type=lambda p: Path(p).absolute(), nargs='+')

    return parser


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog="show_transcript_data")
    args = get_argparser(parser).parse_args()

    files = []

    for file in args.files:
        if file.is_dir():
            files += list(file.glob("*.json"))
        elif file.suffix == ".json":
            files.append(file)
        else:
            parser.error(f"{file} is not a .json file or directory")

    dfs = {}

    for file in tqdm(files, desc="Reading json files"):
        try:
            dfs[file.stem] = pd.read_json(file)
        except ValueError:
            warnings.warn(f"{file} could not be read.")

    if len(dfs) < 1:
        print("No data found!")
        exit(1)

    show(
        **dfs
    )
