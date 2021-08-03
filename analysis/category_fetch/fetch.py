import argparse
import sys
import awis
import sqlite3
import os.path
import re
import logging
import datetime

#Request All The Groups!
base_opts = "&ResponseGroup=Rank,RankByCountry,UsageStats,AdultContent,Speed,Language,LinksInCount,SiteData,Categories&Url=%s"

domain_regex = re.compile(r"^(?:https?://)(.*)$")

def setup_db():
    #Open DB
    db = sqlite3.connect("../data/sqlite/alexa_queries.sqlite3")

    #Make a database to hold all of our access history, in case we need it later
    db.cursor().execute('''
    CREATE TABLE IF NOT EXISTS queries (
        domain TEXT PRIMARY KEY,
        request_url TEXT,
        status TEXT,
        text TEXT
    );
''')
    return db

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

def insert_response(db, dom, response):
    headers = ("domain", "request_url", "status", "text")
    #Form insertion query template
    query = "INSERT INTO queries (%s) VALUES (%s);" % (",".join(headers), ",".join(('?' for _ in headers)))
    
    #Add domain to be inserted
    response["domain"] = dom

    #Insert data
    params = tuple((response[header] for header in headers))
    
    db.execute(query, params)

def fetch_for_doms(domains=None,user=None,key=None):
    #Setup database
    db = setup_db()
    try:
        #We need to remove domains we've already scanned for
        doms = set(domains) - get_existing_doms(db)
        logging.info("Pruned domains, only fetching %d" % len(doms))
        for dom in doms:
            options = base_opts % dom
            #Query
            logging.info("Making API call for %s" % dom)
            response = awis.make_api_call(action="urlInfo", user=user,key=key,options=options)
            #Store
            logging.info("Storing results for %s" % dom)
            insert_response(db,dom,response)
    finally:
        #Make sure DB always closes so we write to disk
        db.commit()
        db.close()

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
        
def main(domains):

    #Get API key
    key_path = os.path.expanduser("~/.keys/alexa_api_key.txt")
    with open(key_path) as f: key = f.read().strip()

    #Get username
    user_path = os.path.expanduser("~/.keys/alexa_api_user.txt")
    with open(user_path) as f: user = f.read().strip()

    #Fetch API results
    fetch_for_doms(domains=domains,user=user,key=key)

if __name__ == "__main__":
    time = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    fh = logging.FileHandler("../logs/fetch_%s.txt" % time)
    logging.getLogger().addHandler(fh)
    logging.getLogger().setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    
    #Load domains
    if len(sys.argv) == 2:
        domains_file = sys.argv[1]
        with open(domains_file) as f: domains = f.read().strip().split('\n')
    else:
        #Use hardcoded domains
        domains=["cnn.com", "alexa.com", "google.com", "kohls.com", "reddit.com", "gnu.org"]

    logging.info("Loading %d domains" % len(domains))
    main(domains)
