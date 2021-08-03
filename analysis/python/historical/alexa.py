#!/bin/python
"""
This file allows us to identify the Alexa ranking for a given domain
"""

import glob
import os
import re
from collections import defaultdict
#import sympy
import csv
import sys

alexa_re = re.compile(r"alexa-top1m-(\d{4})-(\d{2})-(\d{2}).csv")
protocol_re = re.compile(r"https?\:\/\/(.+)")
subdomain_re = re.compile(r"[^\.]+\.(.+)")
txt_re = re.compile(r"(.+)\.txt")

alexa_rankings = None

class NoRankingException(Exception):
    def __init__(self,year,season,domain):
        self.year = year
        self.season = season
        self.domain = domain
        super().__init__("No alexa ranking for %s %s%s" % (domain, year, season))

def __load_alexa_rankings():
    global alexa_rankings, ALEXA_DIR
    if alexa_rankings is not None:
        return

    import historical.util as util
    ALEXA_DIR = "%s" % os.path.join(util.DATA_DIR, "rankings")
    
    alexa_rankings = defaultdict(lambda: defaultdict(dict))
    sum_traffic = 0
    hits = 0
    for fn in os.listdir(ALEXA_DIR):
        match = alexa_re.match(fn)
        if not match:
            continue
        year = match.group(1)
        month = match.group(2)
        year = int(year)
        season = "A" if int(month) < 7 else "B"
        
        with open(os.path.join(ALEXA_DIR, fn)) as f:
            for l in csv.reader(f):
                if len(l) == 0:
                    continue
                if len(l) > 2:
                    l = l[:2]
                try:
                    rank, domain = l
                    domain = domain.split('/')[0]
                except ValueError:
                    print("Error loading line from file %s" % fn)
                    print(l)
                    continue
                rank = int(rank)
                if domain not in alexa_rankings[year][season]:
                    alexa_rankings[year][season][domain] = rank
                else:
                    alexa_rankings[year][season][domain] = min(alexa_rankings[year][season][domain], rank)
                sum_traffic += get_estimated_traffic_for_rank(rank)
                hits += 1
    global average_traffic
    average_traffic = sum_traffic / hits


def get_alexa_rank(year,season,domain,default_rank=None):
    """
    Searches the cached Alexa top 1m file for year,season for domain, returns rank.
    If the domain is not found, throws NoRankingException
    If default rank is specified, returns default rank instead
    """
    domain_tmp = domain

    #We need to strip https?://
    match = protocol_re.match(domain)
    if match:
        domain = match.group(1)

    #For some reason, some domains have a .txt extension. Strip this
    match = txt_re.match(domain)
    if match:
        domain = match.group(1)


    if domain in alexa_rankings[year][season]:
        return alexa_rankings[year][season][domain]
    elif default_rank is None:
        raise NoRankingException(year,season,domain_tmp)
    else:
        return default_rank
        
        
    # while domain not in alexa_rankings[year][season]:
    #     match = subdomain_re.match(domain)
    #     if not match:
    #         if default_rank is None:
    #             raise NoRankingException(year,season,domain_tmp)
    #         else:
    #             return default_rank
    #     domain = match.group(1)
    # return alexa_rankings[year][season][domain]

def get_estimated_traffic(year,season,domain):
    #CITE
    global H
    s=1.099
    try:
        k=get_alexa_rank(year,season,domain)
    except NoRankingException as e:
        k=1000001 #Don't give no traffic, assume lower than any other site
    return get_estimated_traffic_for_rank(k)
        
def get_estimated_traffic_for_rank(k):
    #CITE
    global H
    s=1.099
    try:
        H
    except NameError:
        #H = 14.392726 #Hardcoded s=1
        H = 8.11283 #Hardcoded s=1.099
        #H = sympy.harmonic(1000000,s)
    return 1/(k**s * H)



def load():
    __load_alexa_rankings()
