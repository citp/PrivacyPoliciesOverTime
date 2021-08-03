#NLP
import gensim
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
from gensim import corpora, models
import nltk
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.stem.porter import *
from nltk.tokenize import sent_tokenize
import spacy
import simhash

#Data management
import pandas as pd
#import swifter
#from pandarallel import pandarallel
#pandarallel.initialize(progress_bar=True,nb_workers=4)

#Math
import numpy as np
import math
import random

from multiprocessing import Pool

#System libraries
import time
import os
import sys
import re
import logging

#Data structures
import heapq
import itertools
import collections

#Rendering
from matplotlib import pyplot as plt
import seaborn as sns

from . import markdown_to_text

#Progress bars
from tqdm import tqdm
#tqdm.pandas()

CHUNKSIZE = 1000

#Load Spacy
nlp = spacy.load("en_core_web_lg")
nlp.max_length *= 5


#Regexes:
truste_regex = re.compile(r"(?<![a-z])truste(?![a-z])",flags=re.IGNORECASE)
#https://emailregex.com/
email_regex = re.compile(r"(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|\"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*\")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])",flags=re.IGNORECASE)
#https://gist.github.com/gruber/8891611
url_regex = re.compile(r"(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))",flags=re.IGNORECASE)
num_regex = re.compile(r"(?<![a-z])\d+(\.\d+)?(?![a-z])",flags=re.IGNORECASE)


def truste_sub(text):
    return truste_regex.sub("TrustArc",text)

def email_sub(text):
    return email_regex.sub("email_sub",text)

def url_sub(text):
    return url_regex.sub("url_sub",text)

def num_sub(text):
    return num_regex.sub("NUMBER",text)

#NER Tags to swap out
TAGS_TO_SWAP = [
    "ORG",
    "PERSON",
    #    "FAC",
    #"WORK_OF_ART",
]

SAMPLE = True

def load_data(sample=SAMPLE):
    #Load the data
    with open("data/deduped_policy_text_v11no_html_with_links_and_emails.pickle", "rb") as f:
        df_all = pd.read_pickle(f)
    df_sample = df_all.sample(10000)
    return df_sample if sample else df_all

def get_entities(text):
    entities = set()
    doc = nlp(text)
    for entity in sorted(doc.ents, key=lambda x: -len(x.text)):
        if entity.label_ in TAGS_TO_SWAP:
            entities.add(entity.text.lower())
    return entities

def filter_entities(entities):
    blacklist = ["email","url","number"]
    return list(filter(lambda x: x in blacklist or any(map(str.isalpha,x)),entities))


def entity_sub(text):
    lower_text = text.lower()
    #return entity_re.sub("ENTITY",text)
    for entity, entity_re in entity_re_pairings:
        if entity in lower_text:
            text = entity_re.sub("ENTITY",text)
    return text

def sub_entities(texts):
    """
    p is a multiprocessing pool
    """

    global entity_re_pairings
    
    with Pool(32) as p:
        #Get all entities
        logger.info("Finding all entities")
        entity_lists = tqdm(p.imap(get_entities,texts),total=len(texts))
        entities = sorted(list(set(itertools.chain.from_iterable(entity_lists))),key=len,reverse=True)
        logger.info("Filtering entities")
        entities = filter_entities(entities)

    logger.info("Creating substitution regex")
    entity_re_pairings = [
        (entity.lower(), re.compile(r"\b(?:%s)\b",re.IGNORECASE)) for entity in entities
    ]

    #entity_re = re.compile(r"\b(?:%s)\b" % "|".join(map(re.escape,entities)),re.IGNORECASE)

    del entities
        
    with Pool(32) as p:
        logger.info("Applying NER substitution")
        #FIXME: not parallelized
        texts = tqdm(p.imap(entity_sub,texts),total=len(texts))
        texts = list(texts)

    del entity_re_pairings

    return texts

def prune(df):
    adj_list = collections.defaultdict(set)

    #Self matches
    identity_match_count = 0
    for sh,subdf in df.groupby(["simhash","year_season"]):
        for i in subdf.index:
            for j in subdf.index:
                if i != j:
                    identity_match_count += 1
                    adj_list[i].add(j)

    #Cross matches
    for m1,m2 in simhash.find_all(df.simhash,2,1):
        for i in df[df.simhash == m1].index:
            for j in df[(df.simhash == m2) & (df.year_season == df.loc[i].year_season)].index:
                adj_list[i].add(j)
                adj_list[j].add(i)

    rev_map, rep_map = bfs(adj_list)
    to_keep = set((i for i in rep_map if i in rep_map[i])) | (set(df.index) - set(rev_map.keys()))

    df_pruned = df.loc[to_keep]
    
    logger.info(f"Removed {len(df)-len(df_pruned)} of {len(df)} documents due to similarity. Wanted to keep {len(to_keep)}")

    return df_pruned

def get_undirected(adj_list):
    ajl = {}
    for n in adj_list:
        if n not in ajl: ajl[n] = set()
        for o in adj_list[n]:
            if o not in ajl: ajl[o] = set()
            ajl[n].add(o)
            ajl[o].add(n)
    return ajl    

def bfs(adj_list):
    """Needs an undirected graph"""
    ajl = get_undirected(adj_list)
    nodes = set(ajl)
    #print("Nodes: " + str(nodes))

    rev_map = {}#{n: None for n in nodes}
    rep_map = {}
    while len(nodes) > 0:
        start = nodes.pop()
        #print("Starting at %s" % start)
        rev_map[start] = start
        rep_map[start] = set([start])
        to_visit = ajl[start].copy()
        for n in to_visit:
            if start != n:
                nodes.remove(n)
        while len(to_visit) > 0:
            n = to_visit.pop()
            #print("\tVisiting %s" % n)
            rev_map[n] = start
            rep_map[start].add(n)
            for o in ajl[n]:
                if o not in nodes:
                    continue
                nodes.remove(o)
                to_visit.add(o)
    return rev_map, rep_map
    

        

def clean_text(s_text):
    """
    s_text should be a Pandas series
    """
    texts = list(s_text)
    with Pool(32) as p:
        logger.info("Running markdown render")
        texts = list(tqdm(p.imap(markdown_to_text.clean,texts,CHUNKSIZE),total=len(texts)))
        logger.info("Applying basic substitutions")
        texts = tqdm(p.imap(truste_sub,texts,CHUNKSIZE),total=len(texts))
        texts = list(texts)

        texts = tqdm(p.imap(email_sub,texts,CHUNKSIZE),total=len(texts))
        texts = list(texts)

        texts = tqdm(p.imap(url_sub,texts,CHUNKSIZE),total=len(texts))
        texts = list(texts)
        
    logger.info("Running NER substitutions")
    texts = sub_entities(texts)

    with Pool(32) as p:
        logger.info("Applying number substituions")
        texts = tqdm(p.imap(num_sub,texts,CHUNKSIZE))
        texts = list(texts)

    return pd.Series(texts,index=s_text.index)

def save_data(df):
    logger.info("Saving data")
    if SAMPLE:
        fn = "data/policy_text_cleaned_sample.pkl"
    else:
        fn = "data/policy_text_cleaned.pkl"
    df.to_pickle(fn)

def setup_logger():
    global logger
    logger = logging.getLogger("preprocessing")

    fileHandler = logging.FileHandler("logs/automated_analysis_preprocess_%s.txt"
                                      % time.strftime("%Y%m%d-%H%M"))
    streamHandler = logging.StreamHandler()
    handlers = [fileHandler,streamHandler]
    
    logging.basicConfig(level=logging.INFO,format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                       handlers=handlers)
    logger.setLevel(logging.DEBUG)

def main():
    setup_logger()
    df = load_data()
    #df.swifter.progress_bar(enable=True)
    df["policy_text"] = clean_text(df.policy_text)

    save_data(prune(df))
    return df

if __name__ == "__main__":
    df = main()

    adj_list = collections.defaultdict(set)

    #Self matches
    identity_match_count = 0
    for sh,subdf in df.groupby(["simhash","year_season"]):
        for i in subdf.index:
            for j in subdf.index:
                if i != j:
                    identity_match_count += 1
                    adj_list[i].add(j)

    #Cross matches
    for m1,m2 in simhash.find_all(df.simhash,2,1):
        for i in df[df.simhash == m1].index:
            for j in df[(df.simhash == m2) & (df.year_season == df.loc[i].year_season)].index:
                adj_list[i].add(j)
                adj_list[j].add(i)

    rev_map, rep_map = bfs(adj_list)
