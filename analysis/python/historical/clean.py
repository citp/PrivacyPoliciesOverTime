#!/bin/python3
import argparse
import spacy
import re
import os
import sys
import argparse
import subprocess
import io
import multiprocessing as mp
import itertools
import pickle
from datetime import datetime

from bs4 import BeautifulSoup
from markdown import markdown

from historical import util
from historical import ioutils

import timer



#https://gist.github.com/gruber/8891611
url_re = r"(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"
#https://emailregex.com/
email_re = r"(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])"
num_re = r"\d+"


doNER = True
TAGS_TO_SWAP = [
    "ORG",
    "PERSON",
    #    "FAC",
    #"WORK_OF_ART",
]

def extract_html_links(html):
    urls = set()
    emails = set()
    soup = BeautifulSoup(html, "html.parser")
    for link in soup.findAll('a', attrs={'href': re.compile("^https?://")}):
        urls.add(link.get('href'))
    for link in soup.findAll('a', attrs={'href': re.compile("^mailto:")}):
        emails.add(link.get('href')[7:])
    return urls, emails

def clean_doc(text, html, *args):
    if text is None and html is None:
        return None,None,None,None,None

    urls, emails = extract_html_links(html)
    
    text = re.sub("(?<![a-z])truste(?![a-z])", "TRUSTarc",text,flags=re.IGNORECASE) #Merge TRUSTe and TRUSTarc

    entities = set()
    if doNER:
        doc = nlp(text)
        for entity in sorted(doc.ents, key=lambda x: -len(x.text)):
            if entity.label_ in TAGS_TO_SWAP:
                entities.add(entity.text.lower())
                if entity.text.lower() == "truste": sys.err.write("TRUSTe should be cleaned\n")
                if entity.text.lower() in ["truste", "trustarc", "digital advertising alliance", "national advertising initiative", "daa", "nai"]:
                    continue
                text = text.replace(entity.text, "_ORGANIZATION_")

    emails.update(re.findall(email_re, text, flags=re.IGNORECASE))
    text = re.sub(email_re, "_EMAIL_", text, flags=re.IGNORECASE)

    urls.update(re.findall(url_re, text,flags=re.IGNORECASE))
    text = re.sub(url_re, "_URL_", text,flags=re.IGNORECASE)

    nums = set(re.findall(num_re, text))
    text = re.sub(num_re, "_NUMBER_", text)

    return text,entities,emails,urls,nums

def clean_parallel():
    global data_cache
    
    cols = ("crawl_time", "site_url", "homepage_snapshot_url", "policy_snapshot_url", "year", "season", "policy_text", "policy_filetype", "visit_info", "entities", "emails", "urls", "nums")
    loaded_docs = {}

    pool = util.get_pool()

    data_cache = {} if CACHE_RES else None
    
    ct = 0
    policies_it = ioutils.load_all_policies(clean=False)
    next_data_ar = list(itertools.islice(policies_it,100*util.WORKERS))

    blacklist = util.get_blacklist()
    
    while True:
        data_ar = next_data_ar

        if len(data_ar) == 0:
            break

        args = [(data["policy_text"],data["policy_source"])
                if (data["homepage_snapshot_url"] not in blacklist) else
                (None,None)
                for data,_ in data_ar ]
        result = pool.starmap_async(clean_doc, args)
        next_data_ar = list(itertools.islice(policies_it,100*util.WORKERS))
        res = list(result.get())
        

        to_write = []
        resnum = 0
        for i in range(len(data_ar)):
            text,entities,emails,urls,nums = res[i]
            if text is None:
                continue
            
            data,_ = data_ar[i]
            data = dict(data)
            
            data["policy_text"] = text
            del data["policy_source"]
            data["entities"] = entities
            data["emails"] = emails
            data["urls"] = urls
            data["nums"] = nums
            cols = data.keys()
            to_write.append(data)

            domain = data["site_url"]
            year = data["year"]
            season = data["season"]
            if domain not in loaded_docs:
                loaded_docs[domain] = set()
            loaded_docs[domain].add((year,season))

            if data_cache is not None:
                data_cache[(domain,year,season)] = data
            
            ct += 1
            if ct % 100 == 0:
                print("%d/%d done" % (ct,total),end='\r')
                
        ioutils.write_policy(to_write, cols)
        ioutils.flush_db()

    util.close_pool()

    return loaded_docs    

def impute(loaded_docs,data_cache):
    print("Done cleaning. Imputing")
    def iter_impute_args():
        for domain, docs in loaded_docs.items():
            most_recent = None
            missing_intervals = set()
            for year,season in util.iter_year_season():
                interval = (year,season)
                if interval in docs:
                    if len(missing_intervals) != 0:
                        yield (domain,most_recent,missing_intervals)
                        missing_intervals = set()
                    most_recent = interval
                elif most_recent is not None:
                    missing_intervals.add(interval)
    cols = ["crawl_time","site_url","homepage_snapshot_url","policy_snapshot_url","year","season","policy_text","policy_filetype","visit_info","link_text","exact_match","matching_pattern","entities","emails","urls","nums"]
    ioutils.impute(iter_impute_args(),cols,cache=data_cache)


def init_nlp():
    # Load English tokenizer, tagger, parser, NER and word vectors
    global nlp
    try:
        nlp
    except NameError:
        nlp = spacy.load("en_core_web_lg")
        nlp.max_length *= 5

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cleans documents')
    parser.add_argument('--no-cache-clean', dest="cache_results", action='store_const', const=False, default=True, help='Set this flag to avoid caching the cleaned data. Reduces memory usage but significantly increases disk reads.')
    
    util.add_arguments(parser)
    args = parser.parse_args()
    CACHE_RES = args.cache_results
    util.process_arguments(args)

    print("Loading NLP data            at %s" % (datetime.now().strftime("%H:%M:%S")))
    init_nlp()
    
    total = ioutils.count_num_policies()

    print("Starting old policy removal at %s" % (datetime.now().strftime("%H:%M:%S")))
    ioutils.remove_clean_policies()
    print("Starting cleaning policies  at %s" % (datetime.now().strftime("%H:%M:%S")))
    loaded_docs = clean_parallel()
    print("Starting imputing policies  at %s" % (datetime.now().strftime("%H:%M:%S")))
    impute(loaded_docs, data_cache)
    print("Done imputing policies      at %s" % (datetime.now().strftime("%H:%M:%S")))
    ioutils.close_db()

    print("Exiting                     at %s" % (datetime.now().strftime("%H:%M:%S")))
