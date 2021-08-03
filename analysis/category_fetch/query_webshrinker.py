#!/usr/bin/python

import requests
import os
import collections
import sys
from base64 import urlsafe_b64encode
import pandas as pd
import json
import datetime
import logging
import sqlite3
import itertools
import time
import traceback

no_policy = "_nopolicy"
no_policy = ""

def setup_query_db():
    #Open DB
    db = sqlite3.connect("../data/sqlite/webshrinker%s_queries.sqlite3" % no_policy)

    #Make a database to hold all of our access history, in case we need it later
    db.cursor().execute('''
    CREATE TABLE IF NOT EXISTS queries (
        domain TEXT PRIMARY KEY,
        request_url TEXT,
        status INT,
        text TEXT
    );
    ''')
    return db

def setup_category_db():
    #Open DB
    db = sqlite3.connect("../data/sqlite/webshrinker%s_category.sqlite3" % no_policy)

    #Make a database to hold all of our access history, in case we need it later
    db.cursor().execute('''
    CREATE TABLE IF NOT EXISTS category_pairs (
        domain TEXT,
        category TEXT
    );
    ''')
    db.cursor().execute('''
    CREATE TABLE IF NOT EXISTS category_lists (
        domain TEXT PRIMARY KEY,
        categories TEXT
    );
    ''')

    db.cursor().execute('''
    CREATE TABLE IF NOT EXISTS labels (
        category TEXT PRIMARY KEY,
        label TEXT
    );
    ''')
    return db


def get_rows(db):
    #Keep the default row factory -- we use it elsewhere
    old_factory = db.row_factory
    #Use the Row row_factory which gives a dict
    db.row_factory = sqlite3.Row

    #Pull out all the query items
    query = "SELECT * from queries"
    curs = db.execute(query)
    yield from curs

    #Replace the old one
    db.row_factory = old_factory

def get_existing_doms(db, query_opts = ""):
    #Make the query
    query = "SELECT domain FROM queries" + query_opts

    #Run the query
    curs = db.cursor()
    curs = curs.execute(query)

    #Extract first column
    urls = (cols[0] for cols in curs)
    #urls = (domain_regex.match(url).group(1) for url in urls)
    urls = set(urls)
    return urls


def insert_row(db, domain, request_url, status, text):
    headers = ("domain", "request_url", "status", "text")
    #Form insertion query template
    query = "INSERT INTO queries (%s) VALUES (%s);" % (",".join(headers), ",".join(('?' for _ in headers)))
    
    #Insert data
    params = (domain, request_url, status, text)
    
    db.execute(query, params)


def make_api_query(db, target_website, key, secret_key):
    api_url = 'https://api.webshrinker.com/categories/v3/%s?taxonomy=webshrinker' % urlsafe_b64encode(target_website.encode()).decode('utf-8')

    response = requests.get(api_url, auth=(key, secret_key))
    status_code = response.status_code
    text = response.text

    insert_row(db, target_website, api_url, status_code, text)

def get_categories(row):
    #Turn a sqlite3 row into categories

    #Singleton to hold labels
    global category_labels
    try:
        category_labels
    except NameError:
        category_labels = {}

    #Needed columns
    target_website = row["domain"]
    text = row["text"]
    status_code = row["status"]

    #Load to json
    data = json.loads(text)

    categories = []

    if status_code == 200:
        category_data = data['data'][0]['categories']
        
        for cat in category_data:
            #print(target_website + ',' + cat['label'])
            categories.append(cat['id'])
            category_labels[cat['id']] = cat['label']
        return target_website, categories
    elif status_code == 202:
        print(target_website + ',' + 'None')
        return target_website, categories
    else:
        print(target_website + ',' + 'Error')
        return None


def make_queries():
    domains_fn = sys.argv[2]
    key_fn = sys.argv[3]
    sk_fn = sys.argv[4]

    #Read domains, keys
    with open(domains_fn) as f: domains = f.read().strip().split('\n')
    with open(key_fn) as f: key = f.read().strip()
    with open(sk_fn) as f: secret_key = f.read().strip()
    
    try:
        db = setup_query_db()
        #Get existing domains so we don't double download
        existing = get_existing_doms(db)
        count = 0
        for domain in domains:
            #Skip double downloads
            if domain in existing:
                continue
            count += 1

            #Make query
            logging.info("Querying API for %s" % domain)
            make_api_query(db, domain, key, secret_key)

            #Commit to disk every 100 iterations
            if count % 100 == 0:
                logging.info("Writing to disk & Sleeping for 10 seconds")
                db.commit()
                time.sleep(10)
    finally:
        db.commit()
        db.close()

def store_processed_data(dcm):

    try:
        db = setup_category_db()

        #Drop old values so we don't get duplicates
        for table in ("category_pairs", "category_lists", "labels"):
            #Form drop command
            drop_cmd = "DELETE FROM %s" % table
            db.execute(drop_cmd) #execute

        #All domain-category pairs
        dc_pairs = itertools.chain.from_iterable(
            ((domain,category) for category in categories)
            for domain,categories in dcm.items()
        )
        dc_pairs = list(dc_pairs)
        insert_query = "INSERT INTO category_pairs (domain,category) VALUES (?,?)"
        db.executemany(insert_query,dc_pairs)

        #All domain-category lists
        dc_lists = (
            (domain, ";".join(categories))
            for domain,categories in dcm.items()
        )
        dc_pairs = list(dc_pairs)
        insert_query = "INSERT INTO category_lists (domain,categories) VALUES (?,?)"
        db.executemany(insert_query,dc_lists)
        

        #All the category labels
        label_pairs = category_labels.items()
        insert_query = "INSERT INTO labels (category,label) VALUES (?,?)"
        db.executemany(insert_query,label_pairs)
    finally:
        #Clean up
        db.commit()
        db.close()

def process():

    #Turn requests -> map of domain -> list of category
    domain_to_category_map = {}
    try:
        db = setup_query_db()
        for row in get_rows(db):
            try:
                dc_pair = get_categories(row)
            except:
                logging.error("Unable to parse categories for domain %s" % row["domain"])
                traceback.print_exc()
                dc_pair = None
                
            if dc_pair is None:
                logging.error("Error for domain: %s" % row["domain"])
                continue
            domain, categories = dc_pair
            domain_to_category_map[domain] = categories
            
    finally:
        db.commit()
        db.close()

    #Get the reverse map i.e. category -> list of domains
    category_to_domain_map = collections.defaultdict(list)
    for domain, categories in domain_to_category_map.items():
        for category in categories:
            category_to_domain_map[category].append(domain)
    category_to_domain_map = dict(category_to_domain_map)

    #Store data
    store_processed_data(domain_to_category_map)

    return domain_to_category_map, category_to_domain_map

        
if __name__ == '__main__':

    method = sys.argv[1]
    
    starttime = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    formatter = logging.Formatter('%(asctime)s: %(message)s')

    sh = logging.StreamHandler(sys.stdout)
    logging.getLogger().addHandler(sh)
    sh.setFormatter(formatter)
    
    fh = logging.FileHandler("../logs/webshrinker_%s_%s.txt" % (method, starttime))
    logging.getLogger().addHandler(fh)
    logging.getLogger().setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    
    if method == "query":
        logging.info("Making queries")
        make_queries()
    elif method == "process":
        logging.info("Processing")
        dcm, cdm = process()
        with open("../data/webshrinker/domains_to_categories.json", "w+") as f: f.write(json.dumps(dcm))
