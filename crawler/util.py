import os
import io
import re
import ipaddress

from tld import get_fld
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

from os.path import isdir, join, basename
from glob import glob

from tranco import Tranco
t = Tranco()


def load_tranco_list(num_domains): # data from 12/25/19
    domain_list = t.list(date="2019-12-25")
    return domain_list.top(num_domains)


# Memoize URL to PS+1 conversion to save time with the repeated lookups
# Depending on the number of queries, this may fill the memory
def memoize(f):
    memo = {}

    def helper(x):
        if x not in memo:
            memo[x] = f(x)
        return memo[x]
    return helper


# @memoize
def get_tld_or_host(url):
    if not url.startswith("http"):
        url = 'http://' + url

    try:
        return get_fld(url, fail_silently=False)
    except Exception:
        hostname = urlparse(url).hostname
        try:
            ipaddress.ip_address(hostname)
            return hostname
        except Exception:
            return None


def mkdir(dir_path):
    if not isdir(dir_path):
        os.makedirs(dir_path)


MAX_FILENAME_LEN = 64


# https://stackoverflow.com/a/7406369
def safe_filename_from_url(url):
    domain = re.compile(r"https?://(www\.)?").split(url)[-1]
    domain = domain.replace("web.archive.org/web/", "")
    domain = domain.replace("_/http%3A//www.", "_")
    domain = domain.replace("_/http%3A//", "_")
    domain = domain.replace("_/https%3A//www.", "_")
    domain = domain.replace("_/https%3A//", "_")
    domain = domain.replace("/", "_")
    keepcharacters = ('.', '_', '-')
    return "".join(c for c in domain if c.isalnum() or
                   c in keepcharacters).rstrip()[:MAX_FILENAME_LEN]


def write_to_file(path, text, mode='w'):
    if mode == 'wb':
        with io.open(path, mode) as f:
            f.write(text)
    else:
        with io.open(path, mode, encoding='utf-8') as f:
            f.write(text)

ALEXA_CSV_DIR = "../data/alexa/"

def get_normalized_site_ranks_from_alexa_csv(alexa_csv_path):
    site_ranks = {}
    for line in open(alexa_csv_path):
        rank, url_with_path = line.strip().split(',', 1)
        # get rid of the path
        url = url_with_path.split("/")[0]
        # don't take the domain if it's already seen
        if url not in site_ranks:
            site_ranks[url] = int(rank)
    return site_ranks


def get_historic_alexa_ranks(alexa_csv_dir=ALEXA_CSV_DIR):
    site_ranks = {}
    for alexa_csv in glob(join(alexa_csv_dir, "alexa-top1m*.csv")):
        parts = basename(alexa_csv).split("-")
        year = int(parts[2])
        month = int(parts[3])
        interval = "%s_%s" % (year, "A" if month <= 6 else "B")
        site_ranks[interval] = get_normalized_site_ranks_from_alexa_csv(alexa_csv)
    return site_ranks