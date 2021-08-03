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

def get_originator_rankings(fn):
    doms = []
    with open(fn) as f:
        for l in f:
            m = originator_re.match(l)
            if m is not None:
                doms.append(m.group(1))
    return doms

def get_correlation(fn1, fn2):
    o1_ranks = get_originator_rankings(fn1)
    o2_ranks = get_originator_rankings(fn2)

    if len(o1_ranks) > len(o2_ranks):
        tmp = o1_ranks, fn1
        o1_ranks = o2_ranks
        fn1 = fn2
        o2_ranks,fn2 = tmp
        print("Swapped")
        
    
    o2_rank_dict = {o2_ranks[i]: i+1 for i in range(len(o2_ranks))}
    o1_rank_num = list(range(1,len(o1_ranks)+1))
    o2_rank_num = [o2_rank_dict[d] if d in o2_rank_dict else len(o2_rank_dict) for d in o1_ranks]

    fn, url = util.get_web_fn("graphs", "co_originator_correlations", "%s-%s.html" % (os.path.basename(fn1),os.path.basename(fn2)))

    if True:#not os.path.exists(fn):
        fig = go.Figure(data=go.Scatter(x=list(range(len(o1_rank_num))), y=o1_rank_num,
                                        text=o1_ranks,
                                        mode='lines',
                                        name=os.path.basename(fn1)))
        fig.add_trace(go.Scatter(x=list(range(len(o2_rank_num))), y=o2_rank_num,
                                 text=o1_ranks,
                                 mode='lines',
                                 name=os.path.basename(fn2)))
        plotly.offline.plot(fig, filename=fn, auto_open=False)
    
        print("Published at %s" % url)

    print(len(o1_rank_num),len(o2_rank_num))
    print(o1_rank_num)
    print(o2_rank_num)
    return stats.pearsonr(o1_rank_num, o2_rank_num)
    
                

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find correlation between Alexa Rank and originators data')
    parser.add_argument(dest="originatorsFile1", type=str,
                        help='The file storing the originators data')
    parser.add_argument(dest="originatorsFile2", type=str,
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

    print(get_correlation(args.originatorsFile1, args.originatorsFile2))
    
#    randomize = args.randomize
    #n = args.n
    #metric = args.metric
    #thresh = args.thresh
    #score_by = args.score_by
