import sys
import sqlite3
import multiprocessing as mp
import queue
import pandas as pd

import logging
import historical.util as util

#GLOBALS
ys_grams_db_map = {}

def __init_db(year=None,season=None):
    global policies_db, clean_db, ys_grams_db_map
    if year is not None:
        if (year,season) in ys_grams_db_map:
            return ys_grams_db_map[(year,season)]
        ys_grams_db = load_grams_db(year,season)
        ys_grams_db_map[(year,season)] = ys_grams_db
        return ys_grams_db
    try:
        if policies_db is not None:
            return
    except NameError:
        pass
    policies_db = load_raw_policies_db()
    try:
        clean_db = load_clean_policies_db()
    except:
        print("Clean DB is locked, cannot open")

def load_grams_db(year=None,season=None):
    """
    Manually load the database
    """
    ys_grams_db = sqlite3.connect(util.YS_GRAMS_DB_FN % (year,season))
    ys_grams_db.execute("""
    CREATE TABLE IF NOT EXISTS grams
                 (phrase TEXT,
                 count INTEGER,
                 adopters TEXT,
                 n TEXT,
                 year INTEGER,
                 season TEXT
                 );
    """)
    return ys_grams_db

def load_raw_policies_db():
    policies_db = sqlite3.connect(util.POLICIES_DB_FN)
    policies_db.execute("CREATE INDEX IF NOT EXISTS index_url_year_season ON policy_texts(site_url,year,season);")

    if util.CACHE_DB:
        logging.info("Caching policies DB")
        cache_db = sqlite3.connect(':memory:')
        __backup(policies_db,cache_db)
        policies_db = cache_db
    
    return policies_db

def load_clean_policies_db():
    clean_db = sqlite3.connect(util.CLEAN_DB_FN)

    clean_db.execute("""
    CREATE TABLE IF NOT EXISTS policy_texts
                 (crawl_time TEXT,
                 site_url TEXT,
                 homepage_snapshot_url TEXT,
                 policy_snapshot_url TEXT,
                 year INTEGER,
                 season TEXT,
                 alexa_rank TEXT,
                 policy_text TEXT,
                 policy_source TEXT,
                 raw_source_path TEXT,
                 policy_filetype TEXT,
                 link_text TEXT,
                 exact_match TEXT,
                 matching_pattern TEXT,
                 homepage_snapshot_redirected_url TEXT,
                 visit_info TEXT,
                 entities TEXT,
                 emails TEXT,
                 urls TEXT,
                 nums TEXT
                 );
    """)
     
    if util.CACHE_DB:
        logging.info("Caching cleaned DB")
        cache_db = sqlite3.connect(':memory:')
        __backup(clean_db,cache_db)
        clean_db = cache_db
    
    return clean_db

def clean_up(db,index_policy=False,index_grams=False):
    if index_grams:
        db.execute("""
        CREATE INDEX IF NOT EXISTS index_nc ON grams(n,count);
        """)
    if index_policy:
        db.execute("""
        CREATE INDEX IF NOT EXISTS index_url_year_season ON policy_texts(site_url,year,season);
        """)
    db.commit()
    db.close()
    
        
def close_db(year=None,season=None):
    global clean_db,policies_db
    if year is not None:
        db = ys_grams_db_map[(year,season)]
#        db.execute("""
#        CREATE INDEX IF NOT EXISTS index_nc ON grams(n,count);
#        """)
        db.commit()
        db.close()
        del ys_grams_db_map[(year,season)]
        return
    if policies_db is not None:
        clean_db.execute("""
        CREATE INDEX IF NOT EXISTS index_url_year_season ON policy_texts(site_url,year,season);
        """)
        policies_db.close()
        clean_db.commit()
        clean_db.close()
        clean_db = None
        policies_db = None
        for ys, db in ys_grams_db_map.items():
            db.commit()
            db.close()
            del ys_grams_db_map[ys]
            
def flush_db():
    if policies_db is not None:
        clean_db.commit()
        for ys, db in ys_grams_db_map.items():
            db.commit()
        
def write_policy(data_ar, cols):
    cmd = """
    INSERT INTO policy_texts(%s)
              VALUES(%s);
    """ % (",".join(cols),",".join("?"*len(cols)))
    def iter_rows():
        for data in data_ar:
            for tag in ["entities", "emails", "urls", "nums"]:
                data[tag] = util.serialize_str_array(data[tag])
            yield [data[c] for c in cols]
    clean_db.executemany(cmd, iter_rows())

        
def impute(impute_tuples, cols, cache=None):
    db = clean_db
    db.row_factory = sqlite3.Row

    insert_query = "INSERT INTO policy_texts(%s) VALUES(%s);" % (",".join(cols),",".join("?"*len(cols)))
    
    def iter_impute():

        for domain,most_recent,missing_intervals in impute_tuples:

            start_year, start_season = most_recent

            if cache is not None:
                data = cache[(domain,start_year,start_season)]
            else:
                curs = db.execute("""
                    SELECT * FROM policy_texts WHERE site_url == '%s' AND year == %d AND season == '%s' ;
                """ % (domain, start_year, start_season))
                for row in curs:
                    data = row
                    break

            for y,s in missing_intervals:
                values = []
                for c in cols:
                    if c == "year":
                        v = y
                    elif c == "season":
                        v = s
                    else:
                        v = data[c]
                    values.append(v)
                yield values
    
    db.executemany(insert_query,iter_impute())
    flush_db()

            
def get_policy_names(clean=False):
    """
    if clean is True and USE_CLEAN is true, will use the cleaned db. Otherwise uses the raw policies db
    """
    __init_db()
    if clean and util.USE_CLEAN:
        db = clean_db
    else:
        db = policies_db
    for row in db.execute("SELECT site_url,year,season FROM policy_texts WHERE year >= 2009;"):
        yield row[0], "%s%s" % (row[1],row[2])

    
def count_num_policies():
    __init_db()
    return policies_db.execute("SELECT Count() FROM policy_texts;").fetchone()[0]

def remove_clean_policies():
    __init_db()
    clean_db.execute("DELETE FROM policy_texts;")



def load_phrase(n,year,phrase,season=None):
    if season is None:
        season = year[-1]
        year = int(year[:-1])
    db = __init_db(year=year,season=season)
    query = """
    SELECT phrase,count,adopters FROM grams WHERE 
    n == '%s' and phrase == ?;
    """ % (n)
    curs = db.execute(query, (phrase,))
    line = curs.fetchone()
    while line is not None:
        phrase = line[0]
        count = line[1]
        adopters = util.deserialize_str_array(line[2])
        yield (count,phrase,*adopters)
        line = curs.fetchone()

def remove_grams(*args):
    if len(args) == 0:
        for year,season in util.iter_year_season():
            try:
                os.remove(YS_GRAMS_DB_FN % (year,season))
            except:
                pass
    else:
        raise Exception()
        
def load_grams(n,year,season=None,limit=None):
    if season is None:
        season = year[-1]
        year = int(year[:-1])
        
    to_remove = util.get_blacklist()
    
    db = __init_db(year=year,season=season)
    if limit is not None:
        query = """
        SELECT phrase,count,adopters FROM grams WHERE 
        n == '%s' AND count >= %d;
        """ % (n,limit)
    else:
        query = """
        SELECT phrase,count,adopters FROM grams WHERE 
        n == '%s';
        """ % (n)
    curs = db.execute(query)
    line = curs.fetchone()
    while line is not None:
        phrase = line[0]
        count = line[1]
        adopters = util.deserialize_str_array(line[2])
        adopters = filter(lambda d: d not in to_remove, adopters)
        adopters = [(dom[:-4] if dom.endswith(".txt") else dom) for dom in adopters]
        yield (count,phrase,*adopters)
        line = curs.fetchone()

def write_grams(grams,n,year,season,db=None,clean=False,nopunct=False,nonredundant=False,merge_similar=False):
    if db is None:
        db = __init_db(year=year,season=season)
    cols = ["phrase","count","adopters","n","year","season"]
    cmd = """
    INSERT INTO grams(%s)
              VALUES(%s);
    """ % (",".join(cols),",".join("?"*len(cols)))
    #Iterator so we don't buffer all rows
    def iter_rows():
        for count,gram,domains in grams:
            adopters = util.serialize_str_array(domains)
            yield [gram,count,adopters,n,year,season]
    db.executemany(cmd, iter_rows())

    

def load_grams_parallel(n,limit=None,search_for=None,recount=False,domain_norm_factor=None,policy_norm_factor=None):
    util.get_blacklist() #Ensure this is loaded first

    def queue_grams(q,n,yearseas,limit,search_for):
        for row in load_grams(n,yearseas,limit=limit):
            if search_for is not None:
                s=row[1]
                if s not in search_for:
                    continue

            if recount:
                counts = {}
                doms = row[2:]
                for count_name, (countf, count_friendly_name) in util.count_fxns.items():
                    counts[count_name] = countf(doms,yearseason=yearseas,domain_norm_factor=domain_norm_factor,policy_norm_factor=policy_norm_factor)
                row = (counts, *row[1:])
            q.put((yearseas,row))
            
    q = mp.Queue()
    procs = []
    queuefxn = queue_grams
    for yearseas in util.iter_yearseason():
        p = mp.Process(target=queuefxn,args=(q,n,yearseas,limit,search_for))
        p.start()
        procs.append(p)

    while any((p.is_alive() for p in procs)):
        try:
            yield q.get(True,10)
        except queue.Empty:
            pass
            #if any((p.is_alive() for p in procs)):
                #sys.stderr.write("Unexpected empty queue. Trying again.\n")

    q.close()


def load_all_policies(db=None,limit=-1,filtername="",entities=False,clean=True):
    #Load the database if it's not provided
    if db is None:
        __init_db()

        #Always load the database into memory
        global cache_clean_db
        global cache_policies_db
        try:
            if clean and util.USE_CLEAN:
                db = cache_clean_db #check if we have it already
            else:
                db = cache_policies_db
        except NameError:
            #Otherwise load it
            cache_db = sqlite3.connect(':memory:')
            if clean and util.USE_CLEAN:
                db = clean_db
                cache_clean_db = cache_db
            else:
                db = policies_db
                cache_policies_db = cache_db
            if util.CACHE_DB:
                __backup(db,cache_db)
                db = cache_db
    
    db.row_factory = sqlite3.Row
    limit_s = "LIMIT %d" % limit if limit > 0 else ""
    if filtername != "":
        year = int(filtername[:4])
        season = filtername[4]
        if season not in "AB": raise Exception("Illegal interval")
        
        curs = db.execute("""
            SELECT * FROM policy_texts WHERE year == %d AND season == '%s' %s;
        """ % (year, season, limit_s))
    else:
        curs = db.execute("""
            SELECT * FROM policy_texts %s;
        """ % limit_s)
    for row in curs:
        cols = row.keys()
        data = row
        yield data, cols

        
def __backup(db1,db2):
    for line in db1.iterdump():
        db2.execute(line)
