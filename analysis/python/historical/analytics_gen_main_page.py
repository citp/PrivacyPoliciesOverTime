import csv
import math
import sys
import historical.util as util
import re
import os
import numpy as np
import heapq
import subprocess
import argparse
from sklearn.cluster import AgglomerativeClustering
import historical.draw_heatmap as draw_heatmap
import historical.draw_lines as draw_lines
import historical.alexa as alexa
import contextlib
import timer
import historical.get_mini_plot as miniplot
import functools
import itertools
import scipy.signal
import historical.analytics as analytics


BASE_METRICS = True
NELSON_RULES = False

SENTENCES = None
WORDS = None

friendly_names = {
    "n": "Numbers",
    "w": "Words",
    "s": "Sentences",
    "u": "URLs",
    "e": "Entities",
    "m": "Emails"
}
def get_policy_counts():
    domain_counts = {}
    policy_counts = {}
    util.USE_CLEAN = False
    for dom, yearseas in util.get_policy_names():
        if yearseas not in policy_counts:
            policy_counts[yearseas] = [dom]
            domain_counts[yearseas] = 1
        else:
            policy_counts[yearseas] += [dom]
            domain_counts[yearseas] += 1
    for yearseas in policy_counts:
        policy_counts[yearseas] = len(util.remove_grouped_domains(policy_counts[yearseas], yearseas=yearseas))
    util.USE_CLEAN = True
    return domain_counts, policy_counts

def windows(arr,l):
    """
    Breaks an array into windows/shingles of length l
    """
    return [arr[i:i+l] for i in range(len(arr) - l + 1)]

nelson_rules = [
    lambda arr: any((
        abs(x) >= 3 for x in arr
    )), #Rule 1
    lambda arr: any(( #Rule 2
        all((x > 0 for x in win))
        or all((x < 0 for x in win))
        for win in windows(arr,9)
    )),
    lambda arr: any(( #Rule 3
        all((win[i] > win[i+1] for i in range(len(win)-1)))
        or all((win[i] < win[i+1] for i in range(len(win)-1)))
        for win in windows(arr,6)
    )),
    lambda arr: any(( #Rule 4
        all((
            (win[i] > win[i+1]) != (win[i+1] > win[i+2])
            for i in range(len(win)-2)))
        for win in windows(arr,14)
    )),
    lambda arr: any(( #Rule 5
        sum((1 if x > 2 else 0 for x in win)) >= 2
        or sum((1 if x > 2 else 0 for x in win))  >= 2
        for win in windows(arr,3)
    )),
    lambda arr: any(( #Rule 6
        sum((1 if x > 1 else 0 for x in win)) >= 4
        or sum((1 if x > 1 else 0 for x in win)) >= 4
        for win in windows(arr,5)
    )),
    lambda arr: any(( #Rule 7
        all((x < 1 and x > -1 for x in win))
        for win in windows(arr,15)
    )),
    lambda arr: any(( #Rule 8
        all((abs(x) > 1 for x in win))
        and any((x < 0 for x in win))
        and any ((x > 0 for x in win))
        for win in windows(arr,8)
    )),
]

metrics = analytics.metrics

def nelson_rules(arr):
    """
    Checks the array for compliance with the nelson rules
    Returns an array, where ret[i] is true iff the input is flagged for abnormality under rule i
    """
    sigma = np.std(arr)
    if sigma == 0: sigma = 0.001
    mean = np.mean(arr)
    norm_arr = [(x - mean)/sigma for x in arr]
    return [rule(norm_arr) for rule in nelson_rules]

def cluster(hits, gram_freq, group_ct):
    """
    Cluster phrases based on usage patterns
    """
    data = np.array([gram_freq[hit] for hit in hits])
    clusters = AgglomerativeClustering(n_clusters=group_ct,affinity='cosine',linkage='average').fit_predict(data)
    groups = [[] for i in range(group_ct)]
    for gid,hit in zip(clusters,hits):
        groups[gid].append(hit)
    return groups

def get_data_dump_url(folders,basename,**kwargs):
    """
    Has the same call signature and return value as create_data_dump,
    so can be a drop-in replacement for testing
    """
    nwebfn, nurl = util.get_web_fn(*folders, "%s.html" % (basename))
    return nurl

def create_data_dump(n, folders, basename, groups, gram_freq, gram_freq_raw, score_name_str, scores=None, gen_figs=True):
    """
    Take the given phrases and create:
    - A table of phrase, score (if available), occurances
    - Line graphs for phrase occurance
    - Heatmaps for phrases
    """
    nwebfn, nurl = util.get_web_fn(*folders, "%s.html" % (basename))
    group_to_graph = {}
    hit_to_graph = {}

    #Part one -- create CSV data files, heatmaps, and line graphs
    for group_num in range(len(groups)):
        group_hits = groups[group_num]
        nfn = os.path.join(util.OUT_DIR, "%s_%d.csv" % (basename, group_num))
        if gen_figs:
            #Line graph
            line_url = draw_lines.draw_lines(nfn, -1, ((hit,gram_freq[hit]) for hit in group_hits))
            print("\033[KDone drawling lines for group %d" % group_num, end='\r')
            #Heatmp
            if n == 'w':
                html_heatmap_urls = draw_heatmap.get_multiple_html_heatmap_urls(n,group_hits)
            else:
                heatmap_urls, html_heatmap_urls = draw_heatmap.draw_html_and_reg_heatmaps(n,group_hits,gram_freq_raw)
            print("\033[KDone drawing heatmaps for group %d" % group_num, end='\r')
        else: #Skip drawing
            line_url = draw_lines.get_lines_url(nfn, -1)
            heatmap_urls = draw_heatmap.get_multiple_heatmap_urls(n,group_hits)
            html_heatmap_urls = draw_heatmap.get_multiple_html_heatmap_urls(n,group_hits)
        group_to_graph[group_num] = line_url
        with open(nfn, "w+") as f: #Write CSV data file for later use
            writer = csv.writer(f)
            for hit in group_hits:
                vals_str = ["%0.2f" % f for f in gram_freq[hit]]
                writer.writerow([hit.replace("\\", "\\\\").replace("\n","\\n"), *vals_str])
                #heatmap_url = heatmap_urls[hit]
                heatmap_url = html_heatmap_urls[hit]
#                print(heatmap_url)
                hit_to_graph[hit] = heatmap_url
        print("\033[KDone drawing group %d" % group_num, end='\r')

    #Part two -- create usable web page to explore
    with open(nwebfn, "w+") as nwebf:
        nwebf.write("""<html>
<head>
<style>
table, th, td {
  border: 1px solid black;
  border-collapse: collapse;
}
</style>
</head>
<body>
""")
        pool = util.get_pool()
        all_hits = list(itertools.chain(*[[hit for hit in group] for group in groups]))
        #print(all_hits)
        
        plots = pool.starmap(miniplot.get_plot_as_img,
                             [(ys_list,gram_freq[hit]) for hit in all_hits])
        plots = list(plots)
        print("\033[KDone drawing plots", end='\r')

        #Break up by group
        pNum = 0
        for group, gid in zip(groups,range(len(groups))):
            if len(group) == 0: continue
            line_graph_url = group_to_graph[gid]
            nwebf.write('<h3>%s</h3>' % (score_name_str))
            nwebf.write('<h4><a href="%s" target="_blank">Line graph for group %d</a></h4>\n' % (line_graph_url, gid))
            nwebf.write('<table>\n')
            headers = ["Score", "Phrase","Bar Plot"] + list(util.iter_yearseason())
            if scores is None:
                headers = headers[1:]
            nwebf.write('<tr><th>%s</th></tr>' % '</th><th>'.join(headers))
            #Then phrase
            for hit in group:
                nwebf.write('<tr>\n')
                if scores is not None:
                    if abs(scores[hit]) < 0.01 and scores[hit] != 0:
                        nwebf.write('<td>%0.2E</td>' % scores[hit])
                    else:
                        nwebf.write('<td>%0.2f</td>' % scores[hit])
                nwebf.write('<td width="30%">\n')
                #vals_str = ["%0.2f" % f for f in gram_freq[hit]]
                vals_str = [("%0.2E" if (abs(gram_freq[hit][ysid]) != 0 and abs(gram_freq[hit][ysid]) < 0.01) else "%0.2f") % (gram_freq[hit][ysid]) for ysid in range(num_intervals)]
                
                heatmap_graph_url = hit_to_graph[hit]
                hit_link = '<a href="%s" target="_blank">%s</a>' % (heatmap_graph_url,hit.replace("\\", "\\\\").replace("\n","\\n"))

                plot = plots[pNum]# miniplot.get_plot_as_img(ys_list,gram_freq[hit])
                pNum += 1
                
                nwebf.write('</td><td>'.join([hit_link, plot, *vals_str])) #TODO add miniplot
                nwebf.write('</td></tr>\n')
            print("\033[KDone with table for group %d" % gid, end='\r')

            nwebf.write('</table>\n')
        nwebf.write("</body></html>")
    return nwebfn, nurl


def get_slope_thresh(gram_freq, base_count, prob=0.001):
    """
    Model observations as emissions from domains, so...
    Model as Poisson distribution
    """

    return 0.005
    #entries = sum((len(ar) for ar in gram_freq.values()))
    #return math.log(entries)/entries
    #return 0.001 #Since all metrics represent a proportion of the population... for anything to be significant, it must occur in 

    total = sum((sum((f for f in ar if f != 0)) for ar in gram_freq.values()))
    entries = sum((len(ar) for ar in gram_freq.values()))
    avg = total / entries
    return avg * 10

    #all_emissions = sum(([f for f in ar if f != 0] for ar in gram_freq.values()), [])
    return sorted(all_emissions)[int(len(all_emissions) * prob)]
#    num_phrases = len(gram_freq)
#    slots = num_phrases * num_intervals * base_count
#    total_emissions = sum((sum(ar) for ar in gram_freq.values()))

    return 

    emission_rate = total_emissions / slots
    cdf = np.exp(-emission_rate)
    i = 0
    print(emission_rate, cdf)
    while cdf < prob:
        cdf += emission_rate ** i / math.factorial(i)
        i += base_count
    return i

@timer.time_me("Load grams")
def load_top_grams(n, domain_norm_factor, policy_norm_factor, domain_counts):    
    #Step 1 -- filter top phrases
    top_phrases = set()

    tp_cutoffs = {ys: dc * 0.005 for ys, dc in domain_counts.items()}
    limit=min(tp_cutoffs.values())
    for yearseason,t in util.load_grams_parallel(n,limit=limit):
        if t[0] >= tp_cutoffs[yearseason]:
            top_phrases.add(t[1])
    
    print("\033[K%s-grams: Identified %d top phrases" % (n,len(top_phrases)))

    #Sanity check
    if len(top_phrases) == 0:
        sys.stderr.write("Found no top phrases for grams: %s\n" % str(n))
        return None

    #Step 2 -- find counts for top phrases
    gram_freq_by_countfxn = {count_name:{s:[0] * num_intervals for s in top_phrases} for count_name in util.count_fxns}
    gram_freq_by_countfxn["raw"]={s:[[] for _ in range(num_intervals)]  for s in top_phrases}
    ys_idx_map = {ys:idx for idx,ys in enumerate(list(util.iter_yearseason()))}
    ct = 0
    for yearseason, (counts,s,*doms) in util.load_grams_parallel(n,search_for=frozenset(top_phrases),recount=True,domain_norm_factor=domain_norm_factor,policy_norm_factor=policy_norm_factor):
        #s=t[1]
        
        if ct % 100 == 0:
            print("\033[KLoaded %d phrase-intervals" % ct,end='\r')
        ct += 1
        
        #doms = t[2:]
        ys_idx = ys_idx_map[yearseason]

        gram_freq_by_countfxn["raw"][s][ys_idx] = doms

        for count_name, count in counts.items():
            gram_freq_by_countfxn[count_name][s][ys_idx] = count
        #Iterate counting methods i.e. unique, total, or alexa-weighted domains
        #for count_name, (countf, count_friendly_name) in util.count_fxns.items(): #FIXME
        #    gram_freq_by_countfxn[count_name][s][ys_idx] = countf(doms,yearseason=yearseason,domain_norm_factor=domain_norm_factor,policy_norm_factor=policy_norm_factor)
            
    
    print("\033[KData loaded",end="\r")
    return gram_freq_by_countfxn

def do_analytics():
    global SLOPE_COUNT_THRESH

    
    gram_list = list(range(start,stop))
    if SENTENCES:
        gram_list.append("s")
    if ENTITIES:
        gram_list.append("e")
        gram_list.append("u")
        gram_list.append("m")
        #gram_list.append("n")
    if WORDS:
        gram_list.append("w")
    
    webfn, weburl = util.get_web_fn("raw", "analytics.html")
    print("\033[KAccess at %s" % weburl)
    with open(webfn, "w+") as webf:
        webf.write("""
<html>
    <head>
        <link rel="stylesheet" type="text/css" href="/styles/style.css"/>
    </head>
<body>
""")
        webf.write("""

<div class="query">
<iframe id="queryframe" src="https://privacypolicies.cs.princeton.edu/search_policies.php?basedir=%s">
</iframe>
</div>

<div class="metricsd"><table class="metricst">
        """  % util.testing_prefix)

        #Iterate phrase type
        for n in gram_list:
            ngram_name = "%s-grams" % n if n not in friendly_names else friendly_names[n]
            webf.write("""
<tr class="nhtr">
<td><h3>Data for %s</h3></td>
</tr>
<tr class="ntr">""" % ngram_name)
            #Iterate counting methods i.e. unique, total, or alexa-weighted domains
            for count_name, (countf, count_friendly_name) in util.count_fxns.items():

                print("\033[K%s-grams:" % n)
                webf.write('<td class="dataCb">\n')
                #webf.write("<h4>Data for %s counted by %s</h4>\n" % (ngram_name, count_friendly_name))
                webf.write("<h4>Counted by %s</h4>\n" % (count_friendly_name))

                score_name_str = "Top %s by %%s counted by %s</h4>\n" % (ngram_name, count_friendly_name)

                #Dump data
                if BASE_METRICS:
                    #webf.write("Basic metrics:<br/>\n")
                    for mname, (fsc,faux,hname) in metrics.items():
                        nurl = get_data_dump_url(["metrics", mname], "%s-grams_top_%s_%s%s" % (n, mname, count_name, util.get_file_suffix()))
                        webf.write('<span style="margin-left:2em"><a href="%s" target="_blank">%s</a></span></br>\n' % (nurl, hname))
                        print("\033[K\t\tGraphs created",end="\r")
                        webf.flush()

                        
                #We can't combine these dumps because Nelson rules don't provide a score, whereas all of our metrics do
                if NELSON_RULES:
                    webf.write("Nelson Rules:<br/>\n")
                    for i in range(len(rule_hits)):

                        rnum = i+1 #1 indexed num
                        gen_page = (len(rule_hits[i]) <= 10 * topN)
                        gen_figs = (len(rule_hits[i]) <= 5 * topN)
                        if gen_page:

                            print("\033[K\tNelson rule %d; Hits: %d" % (rnum,len(rule_hits[i])))

                            hits = sorted(rule_hits[i])
                            print("\033[K\t\tSorted",end="\r")

                            #Might be better if we cluster everything?
                            group_ct = math.ceil(len(hits) / 10)

                            groups = cluster(hits, gram_freq, group_ct)
                            print("\033[K\t\tClustered",end="\r")

                            nwebfn, nurl = create_data_dump(["nelson"], "%s-grams_nelson-%d_%s%s" % (n, rnum, count_name, util.get_file_suffix()), groups, gram_freq_raw, score_name_str % hname, gen_figs=gen_figs)
                            print("\033[K\t\tGraphs created",end="\r")
                        else:
                            print("\033[K\tNelson rule %d; Hits: %d (skipped)" % (rnum,len(rule_hits[i])))
                            nurl = get_data_dump_url(["nelson"], "%s-grams_nelson-%d_%s%s" % (n, rnum, count_name, util.get_file_suffix()))
                        webf.write('<span style="margin-left:2em"><a href="%s" target="_blank">Nelson rule %d; Hits: %d</a></span></br>\n' % (nurl, rnum, len(rule_hits[i])))

                webf.write('<br/></td>\n')
                
                #Best to force a flush for partial readouts
                webf.flush()
                os.fsync(webf)

            
            webf.write('</tr>')
            util.close_pool()



        webf.write("""
</table>
</div>
</body>
        </html>""")
        print("\033[KDone")


num_intervals = len(list(util.iter_yearseason()))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Runs basic analytics over pre-sorted n-grams')
    parser.add_argument(dest="MIN", type=int,
                                            help='Analyze n-grams with n>=MIN')
    parser.add_argument(dest="MAX", type=int,
                                            help='Analyze n-grams with n<=MAX')
    parser.add_argument(dest="N", type=int,
                                            help='Number of output phrases per metric')
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
    topN = args.N
    SENTENCES = args.sentences
    WORDS = args.words
    ENTITIES = args.entities

    util.process_arguments(args)
    
    ys_list = list(util.iter_yearseason())
                 
    
    do_analytics()
    
