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
import multiprocessing

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


def make_api_queries(target_website_queue, response_queue, key, secret_key):
    while True:
        try:
            target_website = target_website_queue.get_nowait()
        except:
            break
        api_url = 'https://api.webshrinker.com/categories/v3/%s?taxonomy=webshrinker' % urlsafe_b64encode(target_website.encode()).decode('utf-8')

        i = 0
        while i < 10:
            i += 1
            try:
                response = requests.get(api_url, auth=(key, secret_key))
                status_code = response.status_code
                if status_code != 429:
                    break
                logging.error("Got 429 code")
            except requests.exceptions.ConnectionError as e:
                logging.exception("Connection failed")
            logging.error("Backing off for 10s")
            time.sleep(10)

        if i == 10:
            logging.error("Failed too many times, skipping %s" % target_website)
            continue
            
            
        text = response.text

        response_queue.put((target_website, api_url, status_code, text))

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
        

        #Producer/Consumer queues
        results_queue = multiprocessing.Queue()
        domains_queue = multiprocessing.Queue()

        #Fill queue
        for domain in domains:
            #Skip double downloads
            if domain in existing:
                continue
            domains_queue.put(domain)

        
        num_processes = 4
        proc_args = (domains_queue,results_queue,key,secret_key)
        processes = [multiprocessing.Process(target=make_api_queries, args=proc_args)
                     for i in range(num_processes)]
        for process in processes:
            process.start()
        
        count = 0
        #Consume responses
        while any((p.is_alive() for p in processes)):
            #Access consumer queue
            try:
                #Make sure we time out to avoid deadlock
                res = results_queue.get(True,10)
            except:
                continue

            logging.info("Inserting %s" % str(res))
            
            count += 1

            #Insert to DB
            insert_row(db,*res)

            #Commit to disk every 100 iterations
            if count % 100 == 0:
                logging.info("Writing to disk & Sleeping for 10 seconds")
                db.commit()
                time.sleep(10)
    finally:
        db.commit()
        db.close()
        
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
    else:
        print("Invalid method")
