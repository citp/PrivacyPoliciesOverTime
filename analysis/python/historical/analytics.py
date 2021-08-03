import csv
import math
import sys
import re
import os
import numpy as np
import heapq
import subprocess
import argparse
from sklearn.cluster import AgglomerativeClustering
from datetime import datetime
import contextlib
import timer
import functools
import itertools
import scipy.signal
import logging

import historical.util as util
import historical.ioutils as ioutils
import historical.drawing.get_mini_plot as miniplot
import historical.drawing.draw_heatmap as draw_heatmap
import historical.drawing.draw_lines as draw_lines
import historical.alexa as alexa
from historical.metrics import metrics

MEM_DEBUG = False
if MEM_DEBUG:
    import tracemalloc

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

def mem_count():
    if MEM_DEBUG:
        logging.info("Memory: current: %s, peak: %s" % tuple((tracemalloc._format_size(m,False) for m in tracemalloc.get_traced_memory())))


def mem_trace():
    if MEM_DEBUG:
        mem_count()
        
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('traceback')

        logging.info("[ Top 10 mallocs ]")
        for stat in top_stats[:10]:
            logging.info("%s memory blocks: %.1f KiB" % (stat.count, stat.size / 1024))
            for line in stat.traceback.format():
                logging.info(line)
            logging.info("")

def get_policy_counts():
    domain_counts = {}
    policy_counts = {}
    for dom, yearseas in ioutils.get_policy_names(clean=True):
        if yearseas not in policy_counts:
            policy_counts[yearseas] = [dom]
            domain_counts[yearseas] = 1
        else:
            policy_counts[yearseas] += [dom]
            domain_counts[yearseas] += 1
    for yearseas in policy_counts:
        policy_counts[yearseas] = len(util.remove_grouped_domains(policy_counts[yearseas], yearseas=yearseas))
    return domain_counts, policy_counts

def windows(arr,l):
    """
    Breaks an array into windows/shingles of length l
    """
    return [arr[i:i+l] for i in range(len(arr) - l + 1)]

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

def create_data_dump(n, folders, basename, groups, gram_freq, gram_freq_raw, score_name_str, scores=None, gen_figs=False):
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
            logging.info("\t\tDone drawling lines for group %d" %
                  (group_num))
            #Heatmp
            if n == 'w':
                html_heatmap_urls = draw_heatmap.get_multiple_html_heatmap_urls(n,group_hits)
            else:
                heatmap_urls, html_heatmap_urls = draw_heatmap.draw_html_and_reg_heatmaps(n,group_hits,gram_freq_raw)
            logging.info("\033[K\t\tDone drawing heatmaps for group %d" %
                  (group_num))
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
        logging.info("\t\tDone drawing group %d" % (group_num))

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
        
        plots = pool.starmap(miniplot.get_plot_as_img,
                             [(ys_list,gram_freq[hit]) for hit in all_hits])
        plots = list(plots)
        logging.info("\t\tDone drawing plots")

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
            logging.info("\t\tDone with table for group %d" % (gid))

            nwebf.write('</table>\n')
        nwebf.write("</body></html>")
    mem_count()
    return nwebfn, nurl


def get_slope_thresh(gram_freq, base_count, prob=0.001):
    #FIXME not a magic numberx
    return 0.005

@timer.timeme("Load grams")
def load_top_grams(n, domain_norm_factor, policy_norm_factor, domain_counts):    
    #Step 1 -- filter top phrases
    top_phrases = set()

    
    #if util.DO_SAMPLE:
    #    tp_cutoffs = {ys: dc * 0.01 for ys, dc in domain_counts.items()}
    #else:
    #    tp_cutoffs = {ys: dc * 0.005 for ys, dc in domain_counts.items()}
    if util.DO_SAMPLE:
        tp_cutoffs = {ys: 0.01 for ys, dc in domain_counts.items()}
    else:
        tp_cutoffs = {ys: 0.005 for ys, dc in domain_counts.items()}
    limit=min(tp_cutoffs.values())
    for yearseason,t in ioutils.load_grams_parallel(n,recount=True,limit=limit,domain_norm_factor=domain_norm_factor,policy_norm_factor=policy_norm_factor):
        if t[0]["unique"] >= tp_cutoffs[yearseason]:
            top_phrases.add(t[1])
    
    logging.info("\t%s-grams: Identified %d top phrases" % (n,len(top_phrases)))
    mem_count()
    
    #Sanity check
    if len(top_phrases) == 0:
        logging.error("Found no top phrases for grams: %s\n" % str(n))
        return None

    #Step 2 -- find counts for top phrases
    gram_freq_by_countfxn = {count_name:{s:[0] * num_intervals for s in top_phrases} for count_name in util.count_fxns}
    gram_freq_by_countfxn["raw"]={s:[[] for _ in range(num_intervals)]  for s in top_phrases}
    ys_idx_map = {ys:idx for idx,ys in enumerate(list(util.iter_yearseason()))}
    ct = 0
    for yearseason, (counts,s,*doms) in ioutils.load_grams_parallel(n,search_for=frozenset(top_phrases),recount=True,domain_norm_factor=domain_norm_factor,policy_norm_factor=policy_norm_factor):
        if ct % 100 == 0:
            print("\tLoaded %d phrase-intervals" % ct,end='\r')
        ct += 1
        
        ys_idx = ys_idx_map[yearseason]

        gram_freq_by_countfxn["raw"][s][ys_idx] = doms

        for count_name, count in counts.items():
            gram_freq_by_countfxn[count_name][s][ys_idx] = count
            
    print("\033[K",end="\r")
    logging.info("\tData loaded")
    mem_count()
    return gram_freq_by_countfxn

def do_analytics(gram_list):
    global SLOPE_COUNT_THRESH, SLOPE_PSEUDOCOUNT

    webfn, weburl = util.get_web_fn("raw", "analytics.html")
    logging.info("Access at %s" % weburl)

    #logging.info("Pre-loading Alexa results")
    #alexa.load()

    logging.info("Starting Counting intervals")
    num_intervals = len(list(util.iter_yearseason()))
    logging.info("Starting counting policies")
    domain_counts, policy_counts = get_policy_counts()
    
    average_policy_count = sum(policy_counts.values()) / len(policy_counts)
    policy_norm_factor = {yearseason: 1 / policy_counts[yearseason] for yearseason in util.iter_yearseason()}

    average_domain_count = sum(domain_counts.values()) / len(domain_counts)
    domain_norm_factor = {yearseason: 1 / domain_counts[yearseason] for yearseason in util.iter_yearseason()}
    

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
            logging.info("Loading top grams for %s-grams" % (n))
            gram_freq_by_countfxn = load_top_grams(n, domain_norm_factor, policy_norm_factor, domain_counts)
            logging.info("Done loading top grams for %s-grams" % (n))
            mem_trace()
            if gram_freq_by_countfxn is None:
                logging.error("No grams")
                continue
            gram_freq_raw = gram_freq_by_countfxn["raw"]
            #Iterate counting methods i.e. unique, total, or alexa-weighted domains
            for count_name, (countf, count_friendly_name) in util.count_fxns.items():
                logging.info("\tBeginning scoring with %s" % (count_name))
                #Score based on various metrics
                gram_freq = gram_freq_by_countfxn[count_name]

                base_count = None
                if count_name == "total":
                    base_count = 1 / average_domain_count
                elif count_name == "unique":
                    base_count = 1 / average_policy_count
                elif count_name == "alexa":
                    base_count = alexa.average_traffic
                slope_count_thresh = get_slope_thresh(gram_freq, base_count)
                slope_pseudocount = base_count
                logging.info("\tSlope thresh for round is: %0.4E" % slope_count_thresh)
                logging.info("\tSlope pseudocount for round is: %0.4E" % slope_pseudocount)
                gram_scores = {mname: [] for mname in metrics}

                #Identifying phrases of interest based on metrics & rules
                for s in gram_freq:

                    vals = gram_freq[s]
                    for (mname,(score_fxn,hname)) in metrics.items():
                        heap = gram_scores[mname]
                        score = score_fxn(vals,slope_pseudocount,slope_count_thresh)
                        if score == -100000: continue
                        if len(heap) >= topN:
                            heapq.heappushpop(heap, (score,s))
                        else:
                            heapq.heappush(heap, (score,s))

                logging.info("\tDone scoring")
                webf.write('<td class="dataCb">\n')
                webf.write("<h4>Counted by %s</h4>\n" % (count_friendly_name))

                score_name_str = "Top %s by %%s counted by %s</h4>\n" % (ngram_name, count_friendly_name)

                #Dump data
                for mname, (fsc,hname) in metrics.items():
                    heap = gram_scores[mname]
                    logging.info("\tSorting top values for %s" % (mname))

                    #Heaps aren't sorted, we need to sort the heap
                    #Taking advantage of the heap structure doesn't help us here... pop is log(n), and we need n iterations
                    heap = sorted(heap, reverse=True)
                    phrases = [s for sc,s in heap]

                    groups = [phrases[10*i:min(10*(i+1),len(phrases))] for i in range(math.ceil(len(phrases)/10))]
                    scores = {s:sc for sc,s in heap}

                    logging.info("\tStarting data dump for %s" % (mname))
                    nwebfn, nurl = create_data_dump(n,["metrics", mname], "%s-grams_top_%s_%s%s" % (n, mname, count_name, util.get_file_suffix()), groups, gram_freq, gram_freq_raw, score_name_str % hname, scores=scores)
                    webf.write('<span style="margin-left:2em"><a href="%s" target="_blank">%s</a></span></br>\n' % (nurl, hname))
                    print("\033[K\t\tGraphs created",end="\r")
                    webf.flush()

                webf.write('<br/></td>\n')
                
                #Best to force a flush for partial readouts
                webf.flush()
                os.fsync(webf)
                mem_trace()
                print("\033[K",end="\r")
                logging.info("\tDone dumping data")
                mem_count()

            
            webf.write('</tr>')
            util.close_pool()



        webf.write("""
</table>
</div>
</body>
        </html>""")
        logging.info("Done")


num_intervals = len(list(util.iter_yearseason()))

def main():
    global start,stop,topN,SENTENCES,WORDS,ENTITIES,ys_list

    if MEM_DEBUG:
        tracemalloc.start(10)
    
    parser = argparse.ArgumentParser(description='Runs basic analytics over pre-sorted n-grams')
    parser.add_argument(dest="N", type=int,
                                            help='Number of output phrases per metric')
    parser.add_argument(dest="grams",type=str,nargs="+",help="Gram types to include. Numerical or any of 'emnsuw'")
    parser.add_argument('-s', dest="sentences", action='store_const', const=True, default=False, help='Examine sentences')
    parser.add_argument('-w', dest="words", action='store_const', const=True, default=False, help='Examine words')
    parser.add_argument('-e', dest="entities", action='store_const', const=True, default=False, help='Examine entities')

    util.add_arguments(parser)

    args = parser.parse_args()
    
    #Arguments:
    #analytics.py <MIN> <MAX> <N> (sw)
    #Finds the top N n-grams for each n \in [MIN .. MAX]
    #"s" in the last argument indicates including sentences, "w" words. Blank for nothing
    topN = args.N


    gram_list = []
    for gram in args.grams:
        try:
            #If it's an integer, add it
            gram_list.append(int(gram))
        except:
            for char in list(gram):
                if char not in "swenum":
                    raise Exception("Illegal gram: %s" % char)
                gram_list.append(char)

    util.process_arguments(args)
    util.CACHE_DB = False
    
    ys_list = list(util.iter_yearseason())
                 
    
    do_analytics(gram_list)
    

if __name__ == "__main__":
    logging.getLogger('matplotlib.font_manager').disabled = True
    time = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    fh = logging.FileHandler("../logs/analytics_%s.txt" % time)
    sh = logging.StreamHandler()
    logging.getLogger().addHandler(sh)
    logging.getLogger().addHandler(fh)
    logging.getLogger().setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)
    main()
