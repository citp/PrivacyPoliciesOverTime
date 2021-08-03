import plotly.graph_objects as go
import plotly.figure_factory as ff
import plotly
import csv
import sys
import historical.util as util
import os
import re
import multiprocessing as mp
from scipy.cluster.hierarchy import dendrogram, linkage
from scipy.spatial.distance import pdist, squareform
import numpy as np
import unicodedata

CLUSTERING_MODE = False

#https://stackoverflow.com/questions/295135/turn-a-string-into-a-valid-filename
#https://github.com/django/django/blob/master/django/utils/text.py
def slugify(value, allow_unicode=False):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    value = re.sub(r'[^\w\s-]', '', value.lower()).strip()
    return re.sub(r'[-\s]+', '-', value)
    
def sort_row_data(ar, label):
    if CLUSTERING_MODE:
        npar = np.array(ar)
        fig_tmp = ff.create_dendrogram(npar, orientation='right')
        leaves = list(map(int,fig_tmp['layout']['yaxis']['ticktext']))
        return [ar[i] for i in leaves],  [label[i] for i in leaves]
    else:
        to_sort = [("".join([str(e) for e in ar[i]]), ar[i], label[i]) for i in range(len(label))]
        to_sort.sort()
        return [t[1] for t in to_sort], [t[2] for t in to_sort]

def draw_heatmap(n,phrase):

    ar, l = util.fetch_domains(phrase, n=n) #VERY slow -- we're loading each time

    return draw_heatmap_from_data(n,phrase,ar,l)



def wrapper_draw_html_heatmap_from_data(n,p,ar,l):
    return p, draw_html_heatmap_from_data(n,p,ar,l)

def wrapper_draw_heatmap_from_data(n,p,ar,l):
    return p, draw_heatmap_from_data(n,p,ar,l)

def draw_html_and_reg_heatmaps(n,phrases,raw_data):
    print("\033[KFinding pre-drawn heatmaps", end='\r')
    r_urls = {p: get_heatmap_url(n,p) for p in phrases if os.path.exists(get_heatmap_fn(n,p))}
    r_phrases_filtered = set([p for p in phrases if not os.path.exists(get_heatmap_fn(n,p))])

    h_urls = {p: get_html_heatmap_url(n,p) for p in phrases if os.path.exists(get_html_heatmap_fn(n,p))}
    h_phrases_filtered = set([p for p in phrases if not os.path.exists(get_html_heatmap_fn(n,p))])

    print("\033[KFiltering out pre-drawn heatmaps", end='\r')
    phrases = list(r_phrases_filtered.union(h_phrases_filtered))
    #print("Making heatmaps for %d phrases" % len(phrases))

    
    print("\033[KFetching heatmap data", end='\r')
    ars,ls = util.fetch_domains_for_phrases(phrases, n, raw_data)
    hm_html_args = []
    hm_reg_args = []

    
    print("\033[KBuilding args", end='\r')
    for p in phrases:
        
        ar = ars[p]
        l = ls[p]
        
        if p in h_phrases_filtered:
            #h_urls[p] = draw_html_heatmap_from_data(n,p,ar,l)
            hm_html_args.append((n,p,ar,l))
        if p in r_phrases_filtered:
            #r_urls[p] = draw_heatmap_from_data(n,p,ar,l)
            hm_reg_args.append((n,p,ar,l))

    pool = util.get_pool()
    
    print("\033[KDrawing HTML heatmaps", end='\r')
    for p, url in pool.starmap(wrapper_draw_html_heatmap_from_data,hm_html_args):
        h_urls[p] = url
    print("\033[KDrawing graphical heatmaps", end='\r')
    for p, url in pool.starmap(wrapper_draw_heatmap_from_data,hm_reg_args):
        r_urls[p] = url
    
    print("\033[KDone drawing heatmaps", end='\r')
    return r_urls,h_urls

def draw_multiple_heatmaps(n,phrases):
    
    urls = {p: get_heatmap_url(n,p) for p in phrases if os.path.exists(get_heatmap_fn(n,p))}
    phrases_filtered = [p for p in phrases if not os.path.exists(get_heatmap_fn(n,p))]
    phrases = phrases_filtered
    #print("Making heatmaps for %d phrases" % len(phrases))

    
    ars,ls = util.fetch_domains_for_phrases(phrases, n=n)
    for p in phrases:
        ar = ars[p]
        l = ls[p]
        urls[p] = draw_heatmap_from_data(n,p,ar,l)
    return urls

def draw_multiple_html_heatmaps(n,phrases):    
    urls = {p: get_html_heatmap_url(n,p) for p in phrases if os.path.exists(get_html_heatmap_fn(n,p))}
    phrases_filtered = [p for p in phrases if not os.path.exists(get_html_heatmap_fn(n,p))]
    phrases = phrases_filtered
    #print("Making html_heatmaps for %d phrases" % len(phrases))

    
    ars,ls = util.fetch_domains_for_phrases(phrases, n=n)
    for p in phrases:
        ar = ars[p]
        l = ls[p]
        urls[p] = draw_html_heatmap_from_data(n,p,ar,l)
    return urls

def draw_heatmap_from_data(n,phrase,ar,l):
    opts = np.get_printoptions()
    np.set_printoptions(threshold=sys.maxsize)
    ar,l = sort_row_data(ar, l)

    fn, url = util.get_web_fn("graphs", "heatmaps", slugify(phrase))

    yss = [ys for ys in util.iter_yearseason()]
    
    fig = go.Figure(data=go.Heatmap(
        z=ar,
        y=l, x=yss, colorscale=[(0,"#d0d3d4"),(1,"#1a5276")], showscale=False)
    )
    plotly.offline.plot(fig, filename=fn, auto_open=False)
    np.set_printoptions(opts)
    return url

def draw_html_heatmap_from_data(n,phrase,ar,l):
    opts = np.get_printoptions()
    np.set_printoptions(threshold=sys.maxsize)
    ar,l = sort_row_data(ar, l)
    ar = list(reversed(ar))
    l = list(reversed(l))

    fn, url = util.get_web_fn("graphs", "html_heatmaps", slugify(phrase))

    yss = [ys for ys in util.iter_yearseason()]


    if len(ar) == 0:
        sys.err.writeln("%s,%s has no values\n" % (n,phrase))
        return url
    

    with open(fn, "w+") as f:
        f.write("""<html>
<head>
<style>
table, th, td {
  border: 1px solid black;
  border-collapse: collapse;
}
tr:nth-child(even) {background-color: #d2d2d2;
</style>
</head>
<body>
""")

        f.write('<h3>Phrase: %s</h3>' % (phrase,))
        f.write('<h4><a href="%s" target="_blank">Graphical Heatmap</a></h4>\n' % (get_heatmap_url(n,phrase),))
        f.write('<table>\n')
        headers = ["Domain"] + list(util.iter_yearseason())
        f.write('<tr><th>%s</th></tr>' % '</th><th>'.join(headers))
        for domId in range(len(ar)):
            f.write('<tr>\n')
            f.write('<td>%s</td>\n' % l[domId])
            for ysId in range(len(ar[0])):
                if ar[domId][ysId] == 1:
                    policy_url = "https://privacypolicies.cs.princeton.edu/fetch_policy.php?domain=%s&interval=%s_%s" % (l[domId], yss[ysId][:4], yss[ysId][4])#Policy URL
                    f.write('<td><a href="%s">%s</a></td>\n' % (policy_url, yss[ysId]))
                else:
                    f.write('<td></td>\n')

        f.write('</table>\n')
        f.write("</body></html>")
        
    np.set_printoptions(opts)
    return url


def get_multiple_heatmap_urls(n,phrases):
    return {phrase:get_heatmap_loc(n,phrase)[1] for phrase in phrases}

def get_heatmap_url(n,phrase):
    return get_heatmap_loc(n,phrase)[1]

def get_heatmap_fn(n,phrase):
    return get_heatmap_loc(n,phrase)[0]

def get_heatmap_loc(n,phrase):
    """
    Placeholder for faster runtime until we have a more efficient heatmap generator
    """
    fn, url = util.get_web_fn("graphs", "heatmaps", slugify(phrase))
    return fn,url


def get_multiple_html_heatmap_urls(n,phrases):
    return {phrase:get_html_heatmap_loc(n,phrase)[1] for phrase in phrases}

def get_html_heatmap_url(n,phrase):
    return get_html_heatmap_loc(n,phrase)[1]

def get_html_heatmap_fn(n,phrase):
    return get_html_heatmap_loc(n,phrase)[0]

def get_html_heatmap_loc(n,phrase):
    """
    Placeholder for faster runtime until we have a more efficient heatmap generator
    """
    fn, url = util.get_web_fn("graphs", "html_heatmaps", slugify(phrase))
    return fn,url

    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Draw a heatmap')


    parser.add_argument(dest="n", type=str, help='n')
    parser.add_argument(dest="phrase", type=str, help='phrase')
    parser.add_argument('--cluster', dest="cluster", action='store_const', const=True, default=False, help='Use hierarchical clustering?')


    util.add_arguments(parser)
    args = parser.parse_args()
    util.process_arguments(args)

    phrase = args.phrase
    n = args.n
    CLUSTERING_MODE = args.cluster
    url = draw_heatmap(n,phrase)
    print("Published to: %s " % url)
