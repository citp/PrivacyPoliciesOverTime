import argparse
import math
import historical.util as util
import functools
import random
import numpy as np
from historical.find_near_documents import bfs
import pprint
import multiprocessing

PARALLEL = False

pp = pprint.PrettyPrinter(width=160)

dom_hist_cache = None

def pad_to_dense(M):
    """Appends the minimal required amount of zeroes at the end of each 
     array in the jagged array `M`, such that `M` looses its jagedness.
    https://stackoverflow.com/questions/37676539/numpy-padding-matrix-of-different-row-size"""

    maxlen = max(len(r) for r in M)
    
    Z = np.zeros((len(M), maxlen))
    for enu, row in enumerate(M):
        Z[enu, :len(row)] += row
    return Z

def prune_overlapping(originators):
    #Merge down originators
    for dom in originators:
        verbose = False #dom == "evernote.com"
        phrases = [t[0] for t in originators[dom]]
        rev_map, rep_map = get_all_overlapping(phrases)
        if verbose:
            pp.pprint(rev_map)
            pp.pprint(rep_map)
        for i in range(len(phrases)):
            if i >= len(phrases):
                break
            relatives = rep_map[rev_map[phrases[i]]]
            for j in reversed(range(i+1,len(phrases))):
                if verbose:
                    print('Comparing "%s" to "%s"' % (phrases[i], phrases[j]))
                if phrases[j] in relatives:
                    t1 = originators[dom][i]
                    t2 = originators[dom][j]
                    sc1 = len(t1[2])/len(t1[1])
                    sc2 = len(t2[2])/len(t2[1])
                    if sc2 > sc1:
                        if verbose:
                            print("Pruned " + str(originators[dom][i]))
                        originators[dom][i] = originators[dom][j] #FIXME should this be an append?
                        phrases[i] = phrases[j]
                    else:
                        if verbose:
                            print("Pruned " + str(originators[dom][j]))
                    #FIXME should this be done in the if?
                    del originators[dom][j]
                    del phrases[j]

def get_all_overlapping(phrases):
    tailgrams = {}
    for phrase in phrases:
        tail = ' '.join(phrase.split(' ')[:-1])
        if tail not in tailgrams:
            tailgrams[tail] = []
        tailgrams[tail].append(phrase)
    phrase_adj = {phrase: [] for phrase in phrases}
    for phrase in phrases:
        head = ' '.join(phrase.split(' ')[1:])
        if head in tailgrams:
            phrase_adj[phrase] += tailgrams[head]
    return bfs(phrase_adj)
            

def scan_for_phrase_doms(phrase, n):
    phrases = None
    if type(phrase) is list or type(phrase) is tuple:
        phrases = phrase
    else:
        phrases = [phrase]
    dom_hist = {phrase:{ys:[] for ys in util.iter_yearseason()} for phrase in phrases}
    for ys in util.iter_yearseason():
        unmet_phrases = set(phrases)
        for row in util.load_grams(n, ys):
            rphrase=row[1]
            if rphrase in unmet_phrases:
                unmet_phrases.remove(rphrase)
                doms=row[2:]
                dom_hist[rphrase][ys]=doms
            if len(unmet_phrases) == 0:
                break #No more phrases to be found this interval
    return dom_hist

def randomize_dom_hist(dom_hist):
    dom_hist_r = {}
    for phrase in dom_hist:
        phrase_doms = sum(dom_hist[phrase].values(), [])
        random.shuffle(phrase_doms)
        dom_hist_r[phrase] = {ys:[phrase_doms.pop() for dom in dom_hist[phrase][ys]] for ys in dom_hist[phrase]}
    return dom_hist_r

def _find_originators_for_phrase(args):
    return find_originators_for_phrase(*args)

def find_originators_for_phrase(phrase, dom_hist_phrase, thresh):
    originators = {}
    dom_set = functools.reduce(lambda x,y: x | y, (set(d) for d in dom_hist_phrase.values()))
    thresh_ct = int(math.ceil(thresh * len(dom_set)))
    if thresh_ct == 0:
        thresh_ct = 1
    #print("%s: %d" % (phrase, thresh_ct), end='')
    doms_so_far = set()
    for ys in util.iter_yearseason():
        doms_so_far |= set(dom_hist_phrase[ys])
        if len(doms_so_far) >= thresh_ct:
            #print(" %s" % len(doms_so_far))
            break
    for d in doms_so_far:
        if d not in originators:
            originators[d] = []
        originators[d].append((phrase, doms_so_far, dom_set, ys))
    return originators

def find_originators(n,metric,thresh,score_by=1,randomize=False,prune=False):
    global dom_hist_cache
    if PARALLEL:
        pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() // 4)
    if metric == "top":
        top_phrases = set()
        for yearseason in util.iter_yearseason():
            m = -1
            for t in util.load_grams(n,yearseason):
                num = int(t[0])
                if num < 10:
                    break
                top_phrases.add(t[1])
        phrases = list(top_phrases)
        del top_phrases
    else:            
        phrases = list(zip(*util.load_top_phrases(n,t=metric)))[0]
    if randomize and dom_hist_cache is not None: #We don't need to refetch the history for multiple runs
        dom_hist = dom_hist_cache
    else:
        if False:
            scan_for_phrase_doms_n_fixed = functools.partial(scan_for_phrase_doms, n=n)
            BATCH_SIZE = 10
            batches = (phrases[i:i+BATCH_SIZE] for i in range(0, len(phrases), BATCH_SIZE))
            dom_hist = {}
            dh_batches = pool.map(scan_for_phrase_doms_n_fixed, batches)
            for dh in dh_batches:
                dom_hist.update(dh)
        else:
            dom_hist = scan_for_phrase_doms(phrases, n)
    if randomize:
        dom_hist_cache = dom_hist #Don't save history unless we need to
        dom_hist = randomize_dom_hist(dom_hist)
    originators = {}
    for phrase in dom_hist:
        if PARALLEL:
            break
        originators_changes = find_originators_for_phrase(phrase, dom_hist[phrase], thresh)
        for d in originators_changes:
            if d not in originators:
                originators[d] = originators_changes[d]
            else:
              originators[d] += originators_changes[d]
    if PARALLEL:
        for originators_changes in pool.map(_find_originators_for_phrase, ((phrase,dom_hist[phrase],thresh) for phrase in dom_hist)):
            for d in originators_changes:
                if d not in originators:
                    originators[d] = originators_changes[d]
                else:
                    originators[d] += originators_changes[d]
    if(prune):
        prune_overlapping(originators)
    originators_l = []
    for d, l in originators.items():
        if score_by == 0:
            score = len(l)
        elif score_by == 1:
            score = sum([len(t[2])/len(t[1]) for t in l])
        else:
            raise Exception("Unrecognized metric %d" % score_by)
        originators_l.append((d, score, l))
    return sorted(list(originators_l), key=lambda x: -x[1])

if __name__ == "__main__":
    random.seed(0)
    parser = argparse.ArgumentParser(description='Find the originators for a set of phrases')
    parser.add_argument(dest="n", type=str,
                                            help='An integer for n-grams, or "w" or "s" for words and sentences respectively')
    parser.add_argument(dest="metric", type=str,
                                            help='A string indicating the ranking metric. "var" and "diff" are currently supported')

#    parser.add_argument('--low', dest="low", default='-1', type=int, help="If set, the lowest interval must have at most this many domains")
#    parser.add_argument('--high', dest="high", default='-1', type=int, help="If set, the highest interval must have at least this many domains")
    
    parser.add_argument('--thresh', dest="thresh", default=0.1, type=float,
                        help='Originators are those occur in the first "thresh" percent of domains to adopt the phrase. Rounds up to include the whole time interval.')
    parser.add_argument('--randomize', dest="randomize", default=0, type=int,
                        help='When set to 0, do not randomize. When set to 1 or more, number of rounds to run randomization trials to find a null hypothesis.')
    parser.add_argument('--score', dest="score_by", default=0, type=int,
                        help='Which metric to use for scoring results. 0: number of phrases originated; 1: number of inspired occurances, weighted by simulatneous creators')
    parser.add_argument('--prune', dest="prune", action='store_const', const=True, default=False, help='Prune similar n-grams')

    
    util.add_arguments(parser)

    args = parser.parse_args()
    util.process_arguments(args)

    randomize = args.randomize
    n = args.n
    metric = args.metric
    thresh = args.thresh
    score_by = args.score_by
    prune = args.prune
    
    pruned = "_PR" if prune else ""

    if randomize > 0:
        with open("../out/originators_NH_%s_%s_%f_%d%s%s.txt" % (n, metric, thresh, score_by, pruned, util.get_file_suffix()), "w+") as f:
            originators_ct_lists = []
            for i in range(randomize):
                ct_list = list(list(zip(*find_originators(n, metric, thresh, score_by=score_by, randomize=True, prune=prune)))[1])
                originators_ct_lists.append(ct_list)
            originators_ct_lists = pad_to_dense(originators_ct_lists)
            originators_ct_lists = np.transpose(originators_ct_lists)
            originators_list = np.average(originators_ct_lists, axis=1)
            #print(originators_ct_lists)
            for l in originators_list:
                f.write("%f\n" % (l))
    else:
        with open("../out/originators_%s_%s_%f_%d%s%s.txt" % (n, metric, thresh, score_by, pruned, util.get_file_suffix()), "w+") as f:
            originators_list = find_originators(n, metric, thresh, score_by=score_by, prune=prune)
            for t in originators_list:
                f.write("%s:\t%s\n" % (t[1],t[0]))
                for phrase, origins, adopters, ys in t[2]:
                    f.write("\t(%d -> %d) (%s) %s: %s\n" % (len(origins), len(adopters), ys, phrase, ",".join(origins)))
