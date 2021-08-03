import sys
import multiprocessing as mp
import csv
import itertools
import nltk
import nltk.corpus
import re
import argparse
import logging

from datetime import datetime
from collections import defaultdict, deque


import historical.ioutils as ioutils
import historical.util as util

from timer import timeme


MAX_FIELD_LEN = 50000

workers = 32

punct_re = re.compile(r"[\,\?\|\(\)•\*\:';“”\#" + r'\"' + r"]")

LOG_MEM = False
if LOG_MEM:
    import tracemalloc
    import gc

def get_entity_grams_from_preloaded(entities,emails,urls,nums,domain,year,season):
    ngrams = {n:defaultdict(set) for n in "emun"}
    #Phrases are already loaded, just count
    n = "e"
    for phrase in util.deserialize_str_array(entities):
        phrase = sys.intern(phrase)
        ngrams[n][phrase].add((year,season,domain))
    n="m"
    for phrase in util.deserialize_str_array(emails):
        phrase = sys.intern(phrase)
        ngrams[n][phrase].add((year,season,domain))
    n="u"
    for phrase in util.deserialize_str_array(urls):
        phrase = sys.intern(phrase)
        ngrams[n][phrase].add((year,season,domain))
    if nums is not None:
        n="n"
        for phrase in util.deserialize_str_array(nums):
            phrase = sys.intern(phrase)
            ngrams[n][phrase].add((year,season,domain))
    return ngrams


def get_sentence_grams(text,domain,year,season):
    ngrams = {"s":defaultdict(set)}
    n = "s"
    for paragraph in text.split("\n"):
        for phrase in nltk.tokenize.sent_tokenize(paragraph):
            if " " not in phrase: #A sentence must have at least one space
                continue
            alpha_ct = sum((1 if x.isalpha() else 0 for x in phrase))
            #Must have at least 10 alphabetic characters & alphabetic characters
            #must compose more than two thirds of characters
            if alpha_ct < 10 or (3 * alpha_ct) / (2 * len(phrase)) < 1:
                continue 
            phrase.replace("\n", " ")
            phrase.replace("\r", "")
            phrase = sys.intern(phrase)
            ngrams[n][phrase].add((year,season,domain))
    return ngrams

def get_word_grams(text,domain,year,season):
    ngrams = {"w":defaultdict(set)}
    n = "w"
    for phrase in nltk.tokenize.word_tokenize(text):
        #Skip cleaned words and stopwords
        if phrase.lower() in stopwords: continue
        
        #A word should start & end with a word character
        matches = re.findall(r'.*?(?=\w)(.*)(?<=\w)',phrase)
        if len(matches) == 0:
            continue
        phrase = matches[0]
        
        if phrase == "": continue #no empty words

        phrase = sys.intern(phrase)
        
        ngrams[n][phrase].add((year,season,domain))
    return ngrams

def get_n_grams(n,text,domain,year,season):
    ngrams = {n:defaultdict(set)}
    if NO_PUNCTUATION:
        text = re.sub(punct_re, "", text.lower())
        word_list = nltk.tokenize.word_tokenize(text)
        word_list = filter(lambda w: any((c.isalpha() for c in w)), word_list)
        word_list = filter(lambda w: w not in stopwords, word_list)
        word_list = [sys.intern(w) for w in word_list]
        phrases = set((tuple(l) for l in nltk.ngrams(word_list, n)))
    else:
        text = text.lower()
        word_list = filter(lambda w: any((c.isalpha() for c in w)),nltk.tokenize.word_tokenize(text))
        word_list = [sys.intern(w) for w in word_list]
        phrases = nltk.ngrams(word_list, n)
        raise Exception("Puncutation should be removed")
    for phrase in phrases:
        ngrams[n][phrase].add((year,season,domain))
    return ngrams

def combine_ngrams(ngrams1, ngrams2, do_intern=False):
    for n,grams in ngrams2.items():
        for phrase, hits in grams.items():
            if do_intern:
                hits = set((y,s,sys.intern(d)) for y,s,d in hits)
            ngrams1[n][phrase].update(hits)

def get_swn_grams(text,domain,year,season,gram_list,start,stop):
    ngrams = {n:defaultdict(set) for n in gram_list}
    if 's' in gram_list: #Sentences
        other_ngrams = get_sentence_grams(text,domain,year,season)
        combine_ngrams(ngrams, other_ngrams)
    if 'w' in gram_list: #Words
        other_ngrams = get_word_grams(text,domain,year,season)
        combine_ngrams(ngrams, other_ngrams)
    for n in range(start,stop): #n-grams
        other_ngrams = get_n_grams(n,text,domain,year,season)
        combine_ngrams(ngrams,other_ngrams)
    return ngrams

def get_n_str(gram_list):
    if len(gram_list) == 1:
        return str(gram_list[0])
    else:
        return "(%s)" % ",".join((str(g) for g in gram_list))
            
def generate_grams(gram_list,yearseason,policies_db_cache=None):
    """
    Accumulates n-grams, words, etc. in the policies
    Stores in memory to prepare for writing
    """
    #Which gram types are numbers?
    num_grams = [n for n in gram_list if type(n) is int]
    #By default don't generate any n-grams
    start = min(num_grams,default=1)
    stop = max(num_grams,default=0) + 1

    n_str = get_n_str(gram_list)
    
    logging.info("\tStarting counts for %s at %s"  % (yearseason, datetime.now().strftime("%H:%M:%S")))
    pool = mp.Pool(processes=workers)
    ngrams = {n:defaultdict(set) for n in gram_list}

    if policies_db_cache is None:
        policies_db = ioutils.load_clean_policies_db()
    else:
        policies_db = policies_db_cache
    policies_it = ioutils.load_all_policies(db=policies_db,limit=-1,filtername=yearseason)
    #We want more batches than workers, so slow workers don't hold up the group
    next_data_ar = list(itertools.islice(policies_it,10000*workers))
    ng_res = []
    while True: #We iterate over all policies in batches
        data_ar = next_data_ar
        if len(data_ar) == 0: break

        #Entities are pre-extracted. Just need to count
        if 'e' in gram_list:
            args = [
                (data["entities"],
                 data["emails"],
                 data["urls"],
                 data["nums"] if 'n' in gram_list else None,
                 data["site_url"],
                 data["year"],
                 data["season"],
                )
                for data,_ in data_ar
            ]
            ent_res = pool.starmap(get_entity_grams_from_preloaded,args)
        
            for other_ngrams in ent_res:
                combine_ngrams(ngrams, other_ngrams)

        #Next we do n-grams, words, and sentences
        args = [
            (data["policy_text"],
             data["site_url"],
             data["year"],
             data["season"],
             gram_list,
             start,
             stop
            )
            for data,_ in data_ar
        ]
        
        result = pool.starmap_async(get_swn_grams,args)

        #While we're counting n-grams, let's do IO
        next_data_ar = list(itertools.islice(policies_it,10000*workers))

        #And combine results from previous rounds
        for other_ngrams in ng_res:
            combine_ngrams(ngrams, other_ngrams,do_intern=False)
            
        ng_res = list(result.get())    

    #Clean up any leftover results
    for other_ngrams in ng_res:
        combine_ngrams(ngrams, other_ngrams,do_intern=False)
    
    pool.close()
    if policies_db_cache is None:
        ioutils.clean_up(policies_db)
    
    logging.info("\tDone counting       %s at %s" % (yearseason, datetime.now().strftime("%H:%M:%S")))
    return ngrams

def write_grams(ngrams,year,season):
    """
    Write n-gram values to disk. Called after loading n-grams into memory
    """
    grams_db = ioutils.load_grams_db(year=year,season=season)
    for n in ngrams:
        logging.info("\tWriting %s-grams at %s" % (n, datetime.now().strftime("%H:%M:%S")))
        
        def iterate_grams():
            grams = ngrams[n]
            for k in grams:
                l = list(set([x[2] for x in grams[k]])) #Extract just the domains
                readable_form = ' '.join(k) if type(n) is int else str(k) #phrase as string
                if len(readable_form) > MAX_FIELD_LEN:
                    sys.stderr.write("Skipping row due to excessive length\n")
                    continue
                yield len(l), readable_form, l #count, phrase, domains

        ioutils.write_grams((gram for gram in iterate_grams()), str(n), year, season, db=grams_db, clean=util.USE_CLEAN, nopunct=util.NO_PUNCTUATION, nonredundant=True, merge_similar=False)
        logging.info("\tDone writing        %s%s at %s" % (year,season,datetime.now().strftime("%H:%M:%S")))
    ioutils.clean_up(grams_db)

@timeme("Generate All Grams")
def generate_gram_list(gram_groups,intervals):
    if LOG_MEM:
        logging.info("Memory: current: %s, peak: %s" % tuple((tracemalloc._format_size(m,False) for m in tracemalloc.get_traced_memory())))

    policies_db_cache = ioutils.load_clean_policies_db()
    
    for gram_list in gram_groups:#[sum(gram_groups,[])]:
        n_str = get_n_str(gram_list)
        logging.info("Starting counts for %s-grams at %s"  % (n_str, datetime.now().strftime("%H:%M:%S")))
        for year,season in intervals:
            yearseason = "%d%s" % (year,season)
            
            ngrams = generate_grams(gram_list,yearseason,policies_db_cache=policies_db_cache)
            #Write changes
            write_grams(ngrams,year,season)
            #Save changes to disk to save memory
            #ioutils.close_db(year=year,season=season)
            ngrams = None
        logging.info("Done with counts for %s-grams at %s"  % (n_str, datetime.now().strftime("%H:%M:%S")))

        if LOG_MEM:
            gc.collect() # Collect garbage so we know the memory usage is accurate
            logging.info("Memory: current: %s, peak: %s" % tuple((tracemalloc._format_size(m,False) for m in tracemalloc.get_traced_memory())))

        
def main():

    global NO_PUNCTUATION
    
    if LOG_MEM:
        tracemalloc.start()
    
    logging.info("Starting at %s " % datetime.now().strftime("%H:%M:%S"))
    parser = argparse.ArgumentParser(description='Breaks documents into n-grams under a variety of fitlers')
    parser.add_argument('--start', dest="MIN", default=3, type=int,
                                            help='Analyze n-grams with n>=start')
    parser.add_argument('--stop', dest="MAX", default=9, type=int,
                                            help='Analyze n-grams with n<=stop')
    parser.add_argument(dest="intervals", type=str, nargs='+',
                                            help='Intervals to collect n-grams over')
    parser.add_argument('-s', dest="sentences", action='store_const', const=True, default=False, help='Examine sentences')
    parser.add_argument('-w', dest="words", action='store_const', const=True, default=False, help='Examine words')
    parser.add_argument('-e', dest="entities", action='store_const', const=True, default=False, help='Examine entities')

    util.add_arguments(parser)

    args = parser.parse_args()
    
    #Arguments:
    #analytics.py <MIN> <MAX> <N> (sw)
    #Finds the top N n-grams for each n \in [MIN .. MAX]
    #"s" in the last argument indicates including sentences, "w" words. Blank for nothing
    start = args.MIN
    stop = args.MAX + 1
    yearseasons=args.intervals
    SENTENCES = args.sentences
    WORDS = args.words
    ENTITIES = args.entities

    util.process_arguments(args)
    
    NO_PUNCTUATION = util.NO_PUNCTUATION
    MERGE_SIMILAR = util.MERGE_SIMILAR
    clean = "_CL" if util.USE_CLEAN else ""
    np = "_NP" if NO_PUNCTUATION else ""


    global stopwords
    stopwords = set(nltk.corpus.stopwords.words('english'))
    cleaned_words = set(["_organization_", "_number_", "_url_", "_email_"])
    stopwords.update(cleaned_words)

    try:
        os.mkdir("../data/%s/" % yearseason)
    except:
        pass


    gram_groups = [[n] for n in range(start,stop)]
    if SENTENCES:
        gram_groups.append(["s"])
    if WORDS:
        gram_groups.append(["w"])
    if ENTITIES:
        gram_groups.append(["e","m","u"])


    #Decide how much we're going to iterate
    if yearseasons[0] == "all":
        logging.info("Removing old data at %s " % datetime.now().strftime("%H:%M:%S"))
        ioutils.remove_grams()
        logging.info("Done removing old data at %s" % datetime.now().strftime("%H:%M:%S"))
        intervals = [t for t in util.iter_year_season()]
    else:
        intervals = []
        for yearseason in yearseasons:
            year = int(yearseason[:4])
            if len(yearseason) == 5:
                season = yearseason[4]
                intervals.append((year,season))
            elif len(yearseason) == 4:
                intervals.append((year,'A'))
                intervals.append((year,'B'))
            else:
                logging.error("Error on %s\n" % yearseason)

    generate_gram_list(gram_groups,intervals)
    
    
    #logging.info("Closing DB at %s " % datetime.now().strftime("%H:%M:%S"))
    #ioutils.close_db()
    #logging.info("Finished at %s"  % datetime.now().strftime("%H:%M:%S"))
    if LOG_MEM:
        print("Max memory usage:")
        print("Current: %s, Peak: %s" % tuple((tracemalloc._format_size(m,False) for m in tracemalloc.get_traced_memory())))
    
    
if __name__ == "__main__":
    time = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    fh = logging.FileHandler("../logs/count_terms_%s.txt" % time)
    sh = logging.StreamHandler()
    logging.getLogger().addHandler(sh)
    logging.getLogger().addHandler(fh)
    logging.getLogger().setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)
    main()
