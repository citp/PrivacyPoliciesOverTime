#!/bin/python3
import nltk
import simhash
import traceback
import sys
import os
import csv
import ctypes
import lzma
import pprint
import json
import itertools
import argparse
import random
import subprocess

from fuzzywuzzy import fuzz
from multiprocessing import Pool
from pathlib import Path

from ncd_graphs.NCD_optimized import ncd_pure as ncd
from historical import util
from historical import ioutils



CROSS_YEAR_ONLY = False
seasonToOrd = {'A': 0, 'B': 1}
dist_cache = {}

USE_NCD = False
SIMHASH_THRESH = 3
NCD_THRESH = 0.3
FUZZ_THRESH = 0 #FIXME
SAMPLE = False

pp = pprint.PrettyPrinter(indent=4)


def hash_text(t):
    text, domain, year, season = t
    tokens = nltk.tokenize.word_tokenize(text)

    #https://github.com/seomoz/simhash-py/issues/47
    # A generator for ' '-joined strings of consecutive tokens
    shingles = (' '.join(shingle) for shingle in simhash.shingle(tokens, 4))
    # They need to be unsigned 64-bit ints
    h= simhash.compute([ctypes.c_ulong(hash(shingle)).value for shingle in shingles])

    return (h, text, domain, year, season)

def get_undirected(adj_list):
    ajl = {}
    for n in adj_list:
        if n not in ajl: ajl[n] = set()
        for o in adj_list[n]:
            if o not in ajl: ajl[o] = set()
            ajl[n].add(o)
            ajl[o].add(n)
    #pp.pprint(ajl)
    return ajl

def bfs(adj_list):
    ajl = get_undirected(adj_list)
    nodes = set(ajl)
    #print("Nodes: " + str(nodes))

    rev_map = {}#{n: None for n in nodes}
    rep_map = {}
    while len(nodes) > 0:
        start = nodes.pop()
        #print("Starting at %s" % start)
        rev_map[start] = start
        rep_map[start] = set([start])
        to_visit = ajl[start].copy()
        for n in to_visit:
            if start != n:
                nodes.remove(n)
        while len(to_visit) > 0:
            n = to_visit.pop()
            #print("\tVisiting %s" % n)
            rev_map[n] = start
            rep_map[start].add(n)
            for o in ajl[n]:
                if o not in nodes:
                    continue
                nodes.remove(o)
                to_visit.add(o)
    return rev_map, rep_map
    
    

def get_all(d):
    if type(d) is dict:
        return sum((get_all(n) for n in d.values()), [])
    return [d]

def get_one(d):
    if type(d) is dict:
        n = next(d.values().__iter__())
        return get_one(n)
    return d

def strify(obj):
    s = str(obj)
    s = s.replace('\n', '\\n')
    s = s.replace('\r', '\\r')
    return s

#https://stackoverflow.com/questions/3229419/how-to-pretty-print-nested-dictionaries
def pretty(d, indent=0):
    for key, value in d.items():
        print('\t' * indent + str(key))
        if isinstance(value, dict):
            pretty(value, indent+1)
        else:
            print('\t' * (indent+1) + str(value))

def check_integrity(simhashes):
    for h in simhashes:
        vals = get_all(simhashes[h])
        if len(set(vals)) != 1:
            pretty(simhashes)
            pretty(simhashes[h])
            print(h)
            print(vals)
            raise Exception()
    print("Integrity verified")

def check_distance(lId, rId, sentences_inv, filters):
    if lId in dist_cache and rId in dist_cache[lId]:
        return dist_cache[lId][rId]
    s1 = sentences_inv[lId].encode("utf-8")
    s2 = sentences_inv[rId].encode("utf-8")
    dist = ncd(lId, rId, s1,s2, filters) if USE_NCD else fuzz.ratio(s1, s2)
    if lId not in dist_cache:
        dist_cache[lId] = {}
    dist_cache[lId][rId] = dist
    return dist


def make_textsim_graph(filtername):    
    try:
        print("making dirs: %s" % ("../data/text_sim/%s/" % filtername))
        os.mkdirs("../data/text_sim/%s/" % filtername)
    except:
        pass
    
    p = util.get_pool()
    args = []
    for data,cols in ioutils.load_all_policies(limit=-1, filtername=filtername):
        text=data["policy_text"]
        domain=data["site_url"]
        year=str(data["year"])
        season=data["season"]
        args.append((text, domain, int(year), season))
    print("Total docs is %d" % len(args))
            
    simhashes = {}
    all_hashes = []
    sentences = {}
    for h, sentence, domain, year, season in p.map(hash_text, args):
        if sentence not in sentences:
            sentId = len(sentences)
            sentences[sentence] = sentId
        else:
            sentId = sentences[sentence]

        if h not in simhashes:
            simhashes[h] = {}
        simhashes[h][(domain,year,season)] = sentId
        all_hashes.append(h)

    matches = simhash.find_all(all_hashes,SIMHASH_THRESH+1,SIMHASH_THRESH)


    sentence_inv = {}
    for s in sorted(sentences, key=lambda x:sentences[x]):
        i = sentences[s]
        sentence_inv[i] = s
    del sentences


    lzma_filters = my_filters = [
        {
            "id": lzma.FILTER_LZMA2, 
            "preset": 9 | lzma.PRESET_EXTREME, 
            "dict_size": 100000, #~10k words in english speaker's vocab, x10 for good measure
            "lc": 3,
            "lp": 0,
            "pb": 0, # assume ascii
            "mode": lzma.MODE_NORMAL,
            "nice_len": 273,
            "mf": lzma.MF_BT4
        }
    ]
        
    adj = {}
    adj_sen = {}
    adj_sen_dom = {}
    self_match = [(h,h) for h in simhashes if len(simhashes[h]) > 1]

    if SAMPLE:
        dist_bins = [[] for i in range(10)]

    accepted = 0
    rejected = 0
    rejected_low_pass = 0
    for l,r in itertools.chain(matches, self_match):
        lpols = simhashes[l].keys()
        rpols = simhashes[r].keys()
        ldomains = set((dom for dom, _, _ in simhashes[l]))
        rdomains = set((dom for dom, _, _ in simhashes[r]))
        domains = ldomains.union(rdomains)
        if l == r or len(domains) > max(len(ldomains),len(rdomains)):
            for ld, ly, ls in lpols:
                for rd, ry, rs in rpols:
                    lt = ly * 10 + seasonToOrd[ls]
                    rt = ry * 10 + seasonToOrd[rs]
                    if lt == rt:
                        if CROSS_YEAR_ONLY:
                            continue
                        else:
                            first = "%d%s_%s" % (ly, ls, ld)
                            second = "%d%s_%s" % (ry, rs, rd)
                            pass
                    elif lt < rt:
                        first = "%d%s_%s" % (ly, ls, ld)
                        second = "%d%s_%s" % (ry, rs, rd)
                    else:
                        first = "%d%s_%s" % (ry, rs, rd)
                        second = "%d%s_%s" % (ly, ls, ld)
                    if first not in adj:
                        adj[first] = []
                    adj[first].append(second)

                    lId = simhashes[l][ld,ly,ls]
                    rId = simhashes[r][rd,ry,rs]
                    if FUZZ_THRESH == 0 and not USE_NCD:
                        #Anything will pass, no need to compute
                        comp_dist = 100
                    else:
                        comp_dist = check_distance(lId,rId,sentence_inv, lzma_filters)
                    if USE_NCD:
                        if len(sentence_inv[lId]) + len(sentence_inv[rId]) < 200:
                            comp_dist -= 0.3 #Magic offset because NCD doesn't work well on small text
                        if comp_dist > NCD_THRESH:
                            print("Ruled out %s x %s (%f, %s, %s)" % (ld, rd, comp_dist, hex(l), hex(r)))
                            rejected += 1
                            continue
                    else:
                        if SAMPLE:
                            if comp_dist != 100 and comp_dist >= 90:
                                dist_bins[100 - (comp_dist + 1)].append((lId,rId,comp_dist))
                        if comp_dist < 90:
                            rejected_low_pass += 1
                        if comp_dist < FUZZ_THRESH:
#                            print("Ruled out %s x %s (%f, %s, %s)" % (ld, rd, comp_dist, hex(l), hex(r)))
                            rejected += 1
                            continue
                    accepted += 1

                    if lId not in adj_sen_dom:
                        adj_sen_dom[lId] = set()
                    adj_sen_dom[lId].add(first)
                    adj_sen_dom[lId].add(second)

                    if lId not in adj_sen:
                        adj_sen[lId] = set()
                    adj_sen[lId].add(rId)
                    if rId not in adj_sen:
                        adj_sen[rId] = set()
                    adj_sen[rId].add(lId)

    adj_rev, adj_rep = bfs(adj)

    print("Accepted: %d, rejected: %d, low pass: %d" % (accepted, rejected, rejected_low_pass))

    if SAMPLE:
        for i in range(len(dist_bins)):
            with open("../data/text_sim/sample_0_%d.txt" % i, "w+") as f:
                if len(dist_bins[i]) <= 50:
                    sample = dist_bins[i]
                else:
                    sample = random.sample(dist_bins[i],10)
                for lId,rId,comp_dist in sample:
                    #print(lId,rId,comp_dist)
                    s1 = sentence_inv[lId]
                    s2 = sentence_inv[rId]
                    with open("../data/text_sim/s1_tmp.txt","w+") as f1: f1.write(s1)
                    with open("../data/text_sim/s2_tmp.txt","w+") as f1: f1.write(s2)
                    try:
                        diff = subprocess.check_output("echo \"diff -y <(fold -s -w72 ../data/text_sim/s1_tmp.txt) <(fold -s -w72 ../data/text_sim/s2_tmp.txt) -W 200; exit 0\" | bash", shell=True)
                    except subprocess.CalledProcessError as e:
                        if e.returncode == 2:
                            print(e.output)
                            sys.exit(1)
                        diff = e.output
                    diff = diff.decode()
                    f.write("%s\n%d\n%s\n" % ("="*40,comp_dist,"-"*40))
                    f.write("%s\n" % (diff))

    with open("../data/text_sim/%s/policy_links.json" % filtername, "w+") as f:
        write_obj = []
        i = 0
        for s in adj_rep:
            l = [dom[6:] for dom in adj_rep[s]]
            write_obj.append({"id": i, "domains": l})
            i += 1
        json.dump(write_obj, f)

def make_dirs(intervals):
    for ys in intervals:
        Path("../data/text_sim/%s/" % ys).mkdir(parents=True, exist_ok=True)

    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Find and flag duplicate documents')

    parser.add_argument('intervals', type=str, nargs='+',
                                            help='Which intervals to process over. "all" scans all intervals in sequence')
    parser.add_argument('--sample', dest="sample_fdd", action='store_const', const=True, default=False, help='Sample')

    util.add_arguments(parser)
    args = parser.parse_args()
    intervals = args.intervals
    SAMPLE = args.sample_fdd
    util.process_arguments(args)
    if intervals[0] == "all":
        intervals = list(util.iter_yearseason())

    make_dirs(intervals)
        
    for interval in intervals:
        make_textsim_graph(interval)
    util.close_pool()
