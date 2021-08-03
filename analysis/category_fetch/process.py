import sqlite3
import defusedxml.ElementTree as ElementTree
import logging
import fetch
import datetime

def setup_db():
    db = sqlite3.connect("../data/sqlite/alexa_data.sqlite3")

    #Make a database for just stripped out/organized ata
    db.cursor().execute('''
    CREATE TABLE IF NOT EXISTS alexa_data (
        domain TEXT NOT NULL PRIMARY KEY,
        language TEXT,
        adult_content INT,
        links_in_count INT,
        title TEXT,
        description TEXT,
        online_since TEXT,
        load_time INT,
        load_percentile INT,
        category_titles TEXT,
        category_paths TEXT
    );
''')
    return db
    
def parse_awis_response(xml_str):

    #Extract major elements
    root = ElementTree.fromstring(xml_str)
    alexa_e = root.find("Results").find("Result").find("Alexa")
    traffic_data = alexa_e.find("TrafficData")
    content_data = alexa_e.find("ContentData")
    categories = alexa_e.find("Related").find("Categories")

    #Get the domain we contacted
    argument_es = alexa_e.find("Request").find("Arguments").findall("Argument")
    argument_es = list(filter(lambda e: e.find("Name").text == "url", argument_es))
    if len(argument_es) != 1:
        logging.error("Not exaclty 1 URL argument")
        return None
    argument_e = argument_es[0]
    domain = argument_e.find("Value").text

    results = {"domain": domain}

    #Get language
    try:
        results["language"] = content_data.find("Language").find("Locale").text
    except:
        logging.error("Could not get Language for %s" % domain)

    #Get adult content
    try:
        adult = content_data.find("AdultContent").text
        #Store as 1 or 0 for true/false since sqlite3 doesn't have a boolean datatype
        if adult == "yes":
            adult_bool = 1
        elif adult == "no":
            adult_bool = 0
        else:
            #Unexpected result
            raise Exception()
        results["adult_content"] = adult_bool
    except:
        logging.error("Could not get AdultContent for %s" % domain)

    #Get LinksInCount
    try:
        linksincount = int(content_data.find("LinksInCount").text)
        results["links_in_count"] = linksincount
    except:
        logging.error("Could not get LinksInCount for %s" % domain)

    #Get Title, Description, OnlineSince
    try:
        site_data = content_data.find("SiteData")
    except Exception as e:
        logging.exception("Could not get SiteData for %s" % domain)
    #Title
    try:
        title = site_data.find("Title").text
        results["title"] = title
    except:
        logging.error("Could not get Title for %s" % domain)
    #Description
    try:
        description = site_data.find("Description").text
        results["description"] = description
    except:
        logging.error("Could not get Description for %s" % domain)
    #OnlineSince
    try:
        online_since = site_data.find("OnlineSince").text
        results["online_since"] = online_since
    except:
        logging.error("Could not get OnlineSince for %s" % domain)

    #Get Speed statitics
    try:
        speed = content_data.find("Speed")
    except:
        logging.error("Could not get Speed for %s" % domain)
    try:
        load_time = speed.find("MedianLoadTime").text
        results["load_time"] = load_time
    except:
        logging.error("Could not get MedianLoadTime for %s" % domain)
    try:
        load_percentile = speed.find("Percentile").text
        results["load_percentile"] = load_percentile
    except:
        logging.error("Could not get Percentile for %s" % domain)
    

    #Get Categories
    try:
        category_titles = []
        category_paths = []
        for category_data in categories.findall("CategoryData"):
            title = category_data.find("Title").text
            path = category_data.find("AbsolutePath").text
            category_titles.append(title)
            category_paths.append(path)
            if ';' in title or ';' in path: #If there's a semicolon, separation below won't work
                logging.error("We have a semicolon in a title or path for %s" % domain)

        if len(category_titles) == 0:
            logging.error("Zero Categories for %s" % domain)
        else:
            #Append semicolon separated strings
            results["category_titles"] = ';'.join(category_titles)
            results["category_paths"] = ';'.join(category_paths)
    except:
        logging.error("Could not get Categories for %s" % domain)

    return results

def get_existing_doms(db, query_opts = ""):
    #Make the query
    query = "SELECT domain FROM alexa_data" + query_opts

    #Run the query
    curs = db.cursor()
    curs = curs.execute(query)

    #Extract first column
    urls = (cols[0] for cols in curs)
    urls = set(urls)
    print(urls)
    return urls

def insert_results(data_db, results):
    headers = ("domain","language","adult_content","links_in_count","title","description","online_since","load_time","load_percentile","category_titles","category_paths")

    query_str = "INSERT INTO alexa_data(%s) VALUES (%s)" % (
        ",".join(headers),
        ",".join("?"*len(headers))
    )

    params = [results[header] if header in results else None for header in headers]
    data_db.execute(query_str, params)

def parse_query_db(query_db, data_db):
    #Find existing domains in DB
    existing_doms = get_existing_doms(data_db)

    #Go through all the queries we have
    for row in fetch.get_rows(query_db):
        #Make sure we don't have it already processed
        domain = row["domain"]
        if domain in existing_doms:
            continue

        #Process the query result
        text = row["text"]
        results = parse_awis_response(text)
        if results["domain"] != domain:
            logging.error("Domain mismatch: %s vs %s" % (results["domain"], domain))
        insert_results(data_db, results)
        logging.info("Completed %s" % domain)

def main():
    try:
        query_db = fetch.setup_db()
        data_db = setup_db()
        parse_query_db(query_db,data_db)
    finally:
        query_db.commit()
        query_db.close()
        data_db.commit()
        data_db.close()

if __name__ == "__main__":
    time = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
    formatter = logging.Formatter('%(asctime)s: %(message)s')
    fh = logging.FileHandler("../logs/process_%s.txt" % time)
    logging.getLogger().addHandler(fh)
    logging.getLogger().setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    main()
