import sys
import os
import os.path
import csv
import json
import pprint
import traceback
import io
import multiprocessing as mp
import functools
import historical.alexa as alexa
import pickle
import historical.slack_notify as slack

pp = pprint.PrettyPrinter(indent=4)

DATA_DIR = "../data/"
YEARSEAS_DOMAINS_BASE = os.path.join(DATA_DIR, "text_sim", "%s", "policy_links.json")

OUT_DIR = "../out/"

#CLI Settings:
NO_PUNCTUATION = True #FIXME doesn't seem to be enforced
NON_REDUNDANT = True
MERGE_SIMILAR = True
USE_CLEAN = True


domain_to_domainset_cache = {} #map of yearseas -> map str->int
domainsets_cache = {} #map of yearseas -> map int->list of string

pool = None

def get_pool():
    global pool
    if pool is None:
        pool = mp.Pool(WORKERS)
    return pool

def close_pool():
    global pool
    if pool is not None:
        pool.close()
        pool = None

def serialize_str_array(ar):
    out_f = io.StringIO()
    csv.writer(out_f,lineterminator='\n').writerow(ar)
    return out_f.getvalue()

def deserialize_str_array(s):
    rows = list(csv.reader(io.StringIO(s),lineterminator='\n'))
    if len(rows) > 1: raise Exception("Too many rows")
    if len(rows) == 0:
        return []
    return rows[0]
        

def get_web_fn(*args):
    if len(args) < 2:
        raise Exception("Must have at least one folder")
    path = args[:-1]
    name = args[-1]
    name = name[:100]
    if ".html" not in name:
        name = "%s.html" % name
    folder = "/".join(path)
    url="%s/%s/%s" % (WEB_PREFIX, folder, name)
    fn = os.path.join(WEB_DIR, folder, '%s' % name)
    try:
        os.makedirs(os.path.join(WEB_DIR, folder))
    except FileExistsError:
        pass
    return fn, url

def add_arguments(parser):
    parser.add_argument('-p', dest="punctuation", action='store_const', const=True, default=False, help='Include punctuation in n-grams')
    parser.add_argument('--notify', dest="notify", action='store_const', const=True, default=False, help='Notify me by slack when the job is done')
    parser.add_argument('-r', dest="redundant", action='store_const', const=True, default=False, help='Include repeat phrases within a single policy in frequency counts')
    parser.add_argument('-l', dest="split_similar", action='store_const', const=True, default=False, help='Separate similar policies')
    parser.add_argument('--not-clean', dest="clean", action='store_const', const=False, default=True, help='Set to not use the cleaned data')
    parser.add_argument('--use-sample', dest="sample", action='store_const', const=True, default=False, help='Use only a sample of the data')
    parser.add_argument('--no-cache', dest="cache", action='store_const', const=False, default=True, help='Set this flag to avoid caching the database. Reduces memory usage.')
    parser.add_argument('--test-name', dest="testing", default="", type=str,
                        help='Place output into the listed folder')
    parser.add_argument('--cores', dest="workers", default=32, type=int,
                                            help='How many cores to use (default: 32)')

def process_arguments(args):
    global NO_PUNCUTATION, NON_REDUNDANT, MERGE_SIMILAR, USE_CLEAN
    NO_PUNCTUATION = not args.punctuation
    NON_REDUNDANT = not args.redundant
    MERGE_SIMILAR = not args.split_similar
    USE_CLEAN = args.clean
    
    notify = args.notify
    if notify:
        slack.enable_slack_message()

    csv.field_size_limit(sys.maxsize)


    #Other config options and setup
    global DO_SAMPLE, TESTING, USE_FRACTIONAL_COUNTS, CACHE_DB, WEB_PREFIX, WEB_DIR, POLICIES_DB_FN, CLEAN_DB_FN, ANALYTICS_DB_FN, YS_GRAMS_DB_FN, testing_prefix, WORKERS

    WORKERS = args.workers
    
    DO_SAMPLE = args.sample
    TESTING=(len(args.testing) != 0)
    USE_FRACTIONAL_COUNTS = True
    CACHE_DB = args.cache

    sample_str="-sample" if DO_SAMPLE else ""
    if TESTING:
        testing_prefix = args.testing
    else:
        testing_prefix = ""

    if DO_SAMPLE: #A sample is *always* a test run
        TESTING = True
        if len(testing_prefix) > 0:
            testing_prefix += "_"
        testing_prefix += "sample"

    WEB_PREFIX = "https://privacypolicies.cs.princeton.edu/%s/policies" % testing_prefix
    WEB_DIR = os.path.expanduser("../../public_html/%s/policies" % testing_prefix)

    POLICIES_DB_FN = "../data/sqlite/policy%s.sqlite3" % sample_str
    CLEAN_DB_FN = "../data/sqlite/clean%s.sqlite3" % sample_str
    ANALYTICS_DB_FN = "../data/sqlite/analytics%s.sqlite3" % sample_str
    YS_GRAMS_DB_FN = "../data/sqlite/grams_%%s%%s%s.sqlite3" % sample_str


def get_file_suffix():
    return "".join(("_NP" if NO_PUNCTUATION else "",
                    "_NR" if NON_REDUNDANT else "",
                    "_MS" if MERGE_SIMILAR else "",
                    "_CL" if USE_CLEAN else ""
    ))

def iter_year_season():
    for year in range(2009, 2019+1):
        for season in ["A","B"]:
            yield year,season

def iter_yearseason():
    for year in range(2009, 2019+1):
        for season in ["A","B"]:
            yearseason = "%d%s" % (year,season)
            yield yearseason
        
def get_same_policy_list():
    global to_remove
    try:
        return to_remove
    except NameError:
        pass
    with open('../data/blacklist/removed_from_v5.pickle', 'rb') as f:
        to_remove = pickle.load(f)
        to_remove = set(map(tuple,to_remove))
        assert (('http://reedeu.com', 2010, 'B') in to_remove)

        return to_remove

def get_classifier_blacklist():
    import pickle
    ar = pickle.load(open("../data/blacklist/negative_results.pickle", "rb"))
    blacklist = set(ar)
    return blacklist

def get_blacklist():
    return get_classifier_blacklist()# | get_same_policy_list()

def get_domains_file(yearseas):
    if yearseas is None:
        return DOMAINS_FILE
    return YEARSEAS_DOMAINS_BASE % yearseas

def __init_domains_cache(yearseas):
    """
    We probably could have named this better, but this cache lists the domains that are similar
    """
    global domain_to_domainset_cache, domainsets_cache
    if yearseas in domain_to_domainset_cache and yearseas in domainsets_cache:
        return
    
    domain_to_domainset = {} #map str->int
    domainsets = {} #map int->list of string
    with open(get_domains_file(yearseas)) as json_file:
        data = json.load(json_file)
        for entry in data:
            domainsets[entry["id"]] = entry["domains"]
            for domain in entry["domains"]:
                domain_to_domainset[domain] = entry["id"]
    domain_to_domainset_cache[yearseas] = domain_to_domainset
    domainsets_cache[yearseas] = domainsets
    
    
def remove_grouped_domains(domains, yearseas=None):

    if not deduplicated:
        raise Exception("Please init deduplicated")
    return list(filter(lambda d: (d,yearseas) in deduplicated,domains))
    
    # __init_domains_cache(yearseas)
    # domainset_hits = {}
    # new_domains = []
    # domain_to_domainset = domain_to_domainset_cache[yearseas]
    # #pp.pprint(domain_to_domainset)
    # #Collect domains, identify those from the same set
    # for domain in domains:
    #     if domain in domain_to_domainset:
    #         #print("hit: " + domain)
    #         dId = domain_to_domainset[domain]
    #         if dId in domainset_hits:
    #             #Choose the lexicographically smallest as the representative
    #             domainset_hits[dId] = min(domainset_hits[dId], domain)
    #         else:
    #             domainset_hits[dId] = domain
    #     else:
    #         new_domains.append(domain)
    # #Added grouped domains by representative
    # for dId in domainset_hits:
    #     new_domains.append(domainset_hits[dId])
    # return sorted(new_domains)

def fetch_domains(phrase, cache=None, n=None,dedup=True):
    if cache is not None:
        if phrase in cache:
            return cache[phrase]
    if n is None:
        n = len(phrase.split(' '))
    all_doms = set()
    doms_by_ys = []
    for yearseason in iter_yearseason():
        found = False
        for t in ioutils.load_grams(n, yearseason):
            if t[1] == phrase:
                domains = set(t[2:])
                all_doms = all_doms.union(domains)
                doms_by_ys.append(domains)
                found = True
                break
        if not found:
            doms_by_ys.append(set())

    all_doms_l = list(all_doms)
        
    ret = [[0 for y in range(len(doms_by_ys))]for r in range(len(all_doms))]#np.zeros((len(all_doms),len(doms_by_ys)),dtype=np.int8)
    for domId in range(len(all_doms_l)):
        for ysId in range(len(doms_by_ys)):
            dom = all_doms_l[domId]
            if dom in doms_by_ys[ysId]:
                ret[domId][ysId] = 1
    if cache is not None:
        cache[phrase] = (ret, all_doms_l)
    return ret, all_doms_l

def fetch_domains_for_phrases(phrases, n, raw_data):
    if len(phrases) == 0: return ({},{})
    global MERGE_SIMILAR
    ms_tmp = MERGE_SIMILAR
    MERGE_SIMILAR = False
    all_doms = {p:set() for p in phrases}
    doms_by_ys = {p:[set() for yearseason in iter_yearseason()] for p in phrases}

    ys_idx_map = {ys:idx for idx,ys in enumerate(list(iter_yearseason()))}


    for p in phrases:
        all_doms[p] = all_doms[p].union(*[set(doms) for doms in raw_data[p]])
        doms_by_ys[p] = raw_data[p]
    
    all_doms_l = {p:list(all_doms[p]) for p in phrases}

    #Makes a 3-d heatmap that can be sliced into 2-d heatmaps
    ret = {p:[[0 for y in range(len(doms_by_ys[p]))]for r in range(len(all_doms[p]))] for p in phrases}#np.zeros((len(all_doms),len(doms_by_ys)),dtype=np.int8)
    for p in ret:
        for domId in range(len(all_doms_l[p])):
            for ysId in range(len(doms_by_ys[p])):
                dom = all_doms_l[p][domId]
                if dom in doms_by_ys[p][ysId]:
                    ret[p][domId][ysId] = 1

    MERGE_SIMILAR = ms_tmp
    
    return ret, all_doms_l

def count_by_sum(doms,yearseason=None,domain_norm_factor=None,**kwargs):
    return len(doms) * domain_norm_factor[yearseason]

def count_by_unique(doms,yearseason=None,policy_norm_factor=None,**kwargs):
    return len(remove_grouped_domains(doms, yearseas=yearseason)) * policy_norm_factor[yearseason]

def count_by_alexa(doms,yearseason=None,**kwargs):
    year = int(yearseason[:4])
    season = yearseason[4]
    return sum((alexa.get_estimated_traffic(year,season,domain) for domain in doms))


def load_deduplicated():
    global deduplicated
    url_interval_tuples = pickle.load(open(os.path.join(DATA_DIR,'deduplicated_snapshots.pickle'), 'rb'))
    deduplicated = frozenset([(d,"%s%s" % (y_s[:4],y_s[5])) for d,y_s in url_interval_tuples])

#def count_by_deduplicated(doms,yearseason=None,policy_norm_factor=None,**kwargs):
#    return len(doms2) * policy_norm_factor[yearseason]

load_deduplicated()

count_fxns = {
#    "total":(count_by_sum, "number of domains"),
    "unique":(count_by_unique, "unique policies"),
#    "unique":(count_by_deduplicated, "unique policies"),
#    "alexa":(count_by_alexa, "Alexa weighted domains")
}
