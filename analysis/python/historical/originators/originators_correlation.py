import numpy as np
import re
import argparse
import historical.util as util
import os
import glob
import csv
from plotly import graph_objects as go
import plotly
from scipy.stats import stats

originator_re = re.compile(r"\d+\.\d+:\t(.*)")

def get_alexa_rankings(topX=1000):
    max_rank = {}
    for fn in glob.glob("%s/alexa-top1m-*.csv" % os.path.join(util.DATA_DIR, "rankings")):
        with open(fn) as f:
            ln = 0
            for l in csv.reader(f):
                if len(l) == 0:
                    continue
                rank, domain = l
                rank = ln + 1
                if domain not in max_rank:
                    max_rank[domain] = rank
                else:
                    max_rank[domain] = min(max_rank[domain], rank)
                if rank >= topX:
                    break
                ln += 1
    return max_rank

def get_originator_rankings(fn):
    doms = []
    with open(fn) as f:
        for l in f:
            m = originator_re.match(l)
            if m is not None:
                doms.append(m.group(1))
    return doms

def get_correlation(fn):
    a_ranks = get_alexa_rankings()
    o_ranks = get_originator_rankings(fn)
    a_rank_num = [a_ranks[dom] for dom in o_ranks]
    o_rank_num = list(range(1,len(o_ranks)+1))

    fn, url = util.get_web_fn("graphs", "originator_correlations", "%s.html" % os.path.basename(fn))

    if True:#not os.path.exists(fn):
        fig = go.Figure(data=go.Scatter(x=o_rank_num, y=o_rank_num, text=o_ranks, mode='lines',name="Originator Rank"))
        fig.add_trace(go.Scatter(x=o_rank_num, y=a_rank_num,
                                 text=o_ranks,
                                 mode='lines',
                                 name='Alexa Rank'))
        plotly.offline.plot(fig, filename=fn, auto_open=False)
    
        print("Published at %s" % url)
    
    return stats.pearsonr(a_rank_num, o_rank_num)
    
                

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find correlation between Alexa Rank and originators data')
    parser.add_argument(dest="originatorsFile", type=str,
                                            help='The file storing the originators data')
    #parser.add_argument(dest="n", type=str,
    #                                        help='An integer for n-grams, or "w" or "s" for words and sentences respectively')
    #parser.add_argument(dest="metric", type=str,
    #                                        help='A string indicating the ranking metric. "var" and "diff" are currently supported')

    #parser.add_argument('--thresh', dest="thresh", default=0.1, type=float,
                        #help='Originators are those occur in the first "thresh" percent of domains to adopt the phrase. Rounds up to include the whole time interval.')
    #parser.add_argument('--randomize', dest="randomize", default=0, type=int,
                        #help='When set to 0, do not randomize. When set to 1 or more, number of rounds to run randomization trials to find a null hypothesis.')
    #parser.add_argument('--score', dest="score_by", default=0, type=int,
                        #help='Which metric to use for scoring results. 0: number of phrases originated; 1: number of inspired occurances, weighted by simulatneous creators')
    
    util.add_arguments(parser)

    args = parser.parse_args()
    util.process_arguments(args)

    print(get_correlation(args.originatorsFile))
    
#    randomize = args.randomize
    #n = args.n
    #metric = args.metric
    #thresh = args.thresh
    #score_by = args.score_by
