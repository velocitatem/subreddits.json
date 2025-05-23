#!/usr/bin/env python3
"""
Scrape https://zissou.infosci.cornell.edu/convokit/datasets/subreddit-corpus/corpus-zipped/
and build a plain-text file containing the complete list of subreddit names
included in ConvoKit’s by-subreddit corpus (≈ 948 k entries).

Memory footprint:   < 500 MB.
Output file size:   ~11 MB (one subreddit per line, UTF-8).
"""

import os
import re
import time
import math
import json
import shutil
import string
import pathlib
import itertools
import concurrent.futures as cf
from typing import List, Set
from urllib.parse import urljoin, quote

import requests
from bs4 import BeautifulSoup

ROOT = "https://zissou.infosci.cornell.edu/convokit/datasets/subreddit-corpus/corpus-zipped/"
OUT  = "all_subreddits.txt"
HEADERS = {"User-Agent": "ConvoKit crawler (educational, contact info in code header)"}

session = requests.Session()
session.headers.update(HEADERS)
session.mount("https://", requests.adapters.HTTPAdapter(max_retries=3))

def list_range_dirs() -> List[str]:
    """Return absolute URLs of the 700-ish range directories."""
    soup = BeautifulSoup(session.get(ROOT, timeout=30).text, "html.parser")
    return [
        urljoin(ROOT, a["href"])
        for a in soup.select("a[href$='/']")
        if "~-" in a["href"]          # every range-folder looks like  a~-~b/
    ]

def list_zips(range_url: str) -> List[str]:
    """Return the .corpus.zip filenames inside one range directory."""
    txt = session.get(range_url, timeout=60).text
    # HTML is a simple Apache index; regex is fastest & perfectly safe.
    return [
        m.group(1)
        for m in re.finditer(r'href="([^"]+?\.corpus\.zip)"', txt)
    ]

def scrape() -> List[str]:
    all_names: Set[str] = set()

    dirs = list_range_dirs()
    total = len(dirs)
    from tqdm import tqdm

    print(f"Discovered {total} range directories.")

    with cf.ThreadPoolExecutor(max_workers=12) as pool:
        for zips in tqdm(pool.map(list_zips, dirs), total=total, desc="Processing range directories"):
            all_names.update(name[:-11] for name in zips)   # strip ".corpus.zip"

    result = sorted(all_names, key=str.lower)
    print(f"Extracted {len(result):,} unique subreddit names.")
    return result

def main() -> None:
    subreddits = scrape()
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(subreddits))
    # also write to all_subreddits.json
    print(f"Wrote {OUT} ({os.path.getsize(OUT)/1_048_576:.1f} MB)")
    with open("subreddits.json", "w", encoding="utf-8") as f:
        json.dump(subreddits, f)
    print(f"Wrote subreddits.json ({os.path.getsize('subreddits.json')/1_048_576:.1f} MB)")

if __name__ == "__main__":
    main()
