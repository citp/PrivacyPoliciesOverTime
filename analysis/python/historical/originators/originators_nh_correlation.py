import numpy as np
import re
import argparse
import historical.util as util
import os
import glob
import csv
import sys
from plotly import graph_objects as go
import plotly
from scipy.stats import stats

originator_re = re.compile(r"(\d+\.?\d*):\t(.*)")
originator_fn_re = re.compile(r"(.*/originators)(_\w+_\d*\.?\d*_\d_[\w_]*.txt)")

cap = -1
capN = ""

def get_nh_fn(fn):
    m = originator_fn_re.match(fn)
    return "%s_NH%s" % (m.group(1), m.group(2))

def get_originator_nh_vals(fn):
    fn = get_nh_fn(fn)
    doms = []
    with open(fn) as f:
        for l in f:
            l = l.strip()
            if l != "":
                doms.append(float(l))
                if len(doms) >= cap:
                    break
    return doms

def get_originator_vals(fn):
    doms = []
    with open(fn) as f:
        for l in f:
            m = originator_re.match(l)
            if m is not None:
                doms.append((float(m.group(1)),m.group(2)))
                
            if len(doms) >= cap:
                break
    return doms

def get_correlation(fn):
    nh_vals = get_originator_nh_vals(fn)
    o_vals,o_doms = zip(*get_originator_vals(fn))
    rank_num = list(range(min(len(o_vals),cap,len(nh_vals))))

    fn, url = util.get_web_fn("graphs", "originator_nh_correlations", "%s%s.html" % (os.path.basename(fn),capN))
    corr = stats.pearsonr(o_vals[:len(rank_num)], nh_vals[:len(rank_num)])
    
    if True:#not os.path.exists(fn):
        fig = go.Figure(data=go.Scatter(x=rank_num, y=o_vals[:len(rank_num)], text=o_doms,
                                        mode='lines',name="Originator"),
                        layout=go.Layout(title="Originator vs Null Hypothesis (Pearson R=%f, p=%f)" % corr,
                                         xaxis_title="Domains",
                                         yaxis_title="Influence score"))
        fig.add_trace(go.Scatter(x=rank_num, y=nh_vals[:len(rank_num)],
                                 mode='lines',
                                 name='Null Hypothesis'))
        plotly.offline.plot(fig, filename=fn, auto_open=False)
    
        print("Published at %s" % url)
    
    return corr
    
                

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find correlation between Alexa Rank and originators data')
    parser.add_argument(dest="originatorsFile", type=str,
                                            help='The file storing the originators data')
    parser.add_argument("--cap", dest="cap", type=int, default=-1,
                                            help='Cap for number of lines to examine')
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
    cap = args.cap if cap != -1 else sys.maxsize
    capN = ("_" + str(args.cap)) if args.cap != -1 else ""

    print(get_correlation(args.originatorsFile))
    
#    randomize = args.randomize
    #n = args.n
    #metric = args.metric
    #thresh = args.thresh
    #score_by = args.score_by
