import math
import sqlite3

if __name__ == "__main__":
    
    db1 = sqlite3.connect("../data/sqlite/policy.sqlite3")
    db2 = sqlite3.connect("../data/sqlite/policy-sample.sqlite3")

    db2.execute("""DROP TABLE IF EXISTS policy_texts;""")
    
    db2.execute("""CREATE TABLE policy_texts
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
                 visit_info TEXT
                 );""")

    curs = db1.execute("select DISTINCT(site_url) from policy_texts limit 999;")
    doms = list(set(l[0] for l in curs.fetchall()))

    #sqlite3 has a max argument count of 999
    #We can batch queries into 500 domains at a time to avoid this
    for i in range(math.ceil(len(doms) / 500)):
        
        start_idx = i * 500
        stop_idx = min((i+1)*500,len(doms))
        to_fetch = doms[i * 500:]
        curs = db1.execute("select * from policy_texts where site_url in (%s);" % (",".join("?"*len(to_fetch))), to_fetch)
        cols = [t[0] for t in curs.description]
        line = curs.fetchone()
        while line:
            db2.execute("INSERT INTO policy_texts(%s) VALUES(%s);" % (",".join(cols),",".join("?"*len(cols))),
                        line
            )
            db2.commit()
            line = curs.fetchone()

    db2.execute("CREATE INDEX index_url_year_season ON policy_texts(site_url,year,season);")
            
    db1.close()
    db2.close()

    
    
    
