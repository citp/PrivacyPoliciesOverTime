#NLP
import nltk
from nltk.stem import WordNetLemmatizer, SnowballStemmer
from nltk.stem.porter import *
from nltk.tokenize import sent_tokenize
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS
import spacy

#Data management
import pandas as pd
import swifter
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True,nb_workers=4)

#Math
import numpy as np
import math
import random

#System libraries
import time
import os
import sys
import re
import logging

from multiprocessing import Pool

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
tqdm.pandas()

N=3

stemmer = SnowballStemmer("english")

def lemmatize_stemming(text):
    return stemmer.stem(WordNetLemmatizer().lemmatize(text, pos='v'))

def load_data():
    fn = "data/policy_text_cleaned.pkl"
    return pd.read_pickle(fn)

def save_data(df):
    fn = f"data/{N}grams.pkl"
    df.to_pickle(fn)


def get_ngrams(text,n=None):
    if n is None:
        n = N
    ngrams = set()
    for sentence in nltk.tokenize.sent_tokenize(text):
        if n is "s":
            sentence = sys.intern(sentence)
            ngrams.add(sentence)
            continue
        words = simple_preprocess(sentence)
        words = (word for word in words if word not in STOPWORDS)
        words = filter(str.isalnum,words)
        words = map(lemmatize_stemming, words)
        phrases = (" ".join(phrase) for phrase in nltk.ngrams(words,n))
        phrases = map(sys.intern,phrases)
        ngrams.update(phrases)
    return ngrams

def setup_logger():
    global logger
    logger = logging.getLogger("ngrams")

    fileHandler = logging.FileHandler("logs/automated_analysis_ngrams_%s.txt"
                                      % time.strftime("%Y%m%d-%H%M"))
    streamHandler = logging.StreamHandler()
    handlers = [fileHandler,streamHandler]
    
    logging.basicConfig(level=logging.INFO,format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                       handlers=handlers)
    logger.setLevel(logging.DEBUG)


def main():        
    df = load_data()
    counters = {}

    with Pool(32) as p:
        for ys, sub_df in df.groupby("year_season"):
            logger.info(f"Starting counting {ys}")
            ngram_lists = tqdm(p.imap(get_ngrams, sub_df["policy_text"]),total=len(sub_df))
            counters[ys] = collections.Counter(itertools.chain.from_iterable(ngram_lists))

    logger.info("Saving")
    ys_values = sorted(list(counters.keys()))
    df = pd.DataFrame.from_records((counters[ys] for ys in ys_values), index=ys_values)
    df = df.transpose()
    df = df.fillna(0)
    save_data(df)

if __name__ == "__main__":
    N = sys.argv[1]
    try:
        N = int(N)
    except:
        pass
    setup_logger()
    main()
