import base64
import sqlite3
import re
import sys
import json
import magic

from glob import glob
from os.path import join, dirname, isfile
from detect_links import EXACT_POLICY_TITLES, PARTIAL_POLICY_TITLES
from collections import Counter
from util import get_historic_alexa_ranks


def parse_log_line(log_line):
    prefix_and_message, visit_info_str = log_line.split(" VisitInfo: ")
    try:
        visit_info = json.loads(visit_info_str)
    except Exception:
        sys.exit("Cannot parse visit_info JSON %s" % log_line)
    day, time, message = prefix_and_message.split(" ", 2)
    timestamp = "%s %s" % (day, time)
    return timestamp, message, visit_info


def should_exclude_policy(visit_info):
    if visit_info["policy_snapshot_url"].\
            endswith("id_/https%3A//web.archive.org"):
        print("Will exclude. Bad policy snapshot URL",
              visit_info["policy_snapshot_url"])
        return True
    return False


def should_exclude_policy_by_text(extracted_txt_path, policy_txt):

    if not len(policy_txt):
        print("Will exclude. Empty policy_txt file", extracted_txt_path)
        return True

    policy_txt_stripped = policy_txt.strip()
    if not len(policy_txt_stripped):
        print("Will exclude. Policy text is empty or composed of whitespace",
              extracted_txt_path)
        return True

    file_type = magic.from_file(extracted_txt_path)
    txt_lower = policy_txt.lower()
    if (file_type.startswith("HTML document") and
            ("<html" in txt_lower or "<!doctype" in txt_lower)):
        print("Will exclude. HTML inside policy_txt file", extracted_txt_path)
        return True
    return False


def process_crawl_data(root_data_dir):
    """Build an sqlite database using crawl data, logs and extracted texts."""
    root_extracted_txt_dir = join(root_data_dir, "extracted_text")
    processed_snapshots = {}
    extracted_policies = []
    readability_fails = set()
    excluded_policies = set()
    policy_links = {}
    link_pattern_matches = Counter()
    link_texts = Counter()
    n_total_policies = 0
    site_ranks = get_historic_alexa_ranks()
    db_path = "policy.sqlite3"
    db_conn = create_db(db_path)
    log_files = reversed(sorted(
        glob(join(root_data_dir, "crawl*/data-*/logs/puppet_downloader.log"))))
    for log_file in log_files:
        print("Will process %s" % log_file)
        crawler_subdir = log_file.replace(root_data_dir, "").\
            replace("/logs/puppet_downloader.log", "")
        crawl_data_dir = join(dirname(dirname(log_file)), "out")
        crawl_policy_html_dir = join(crawl_data_dir, "policy_html")
        crawl_readable_html_dir = join(crawl_data_dir, "readable_policy_html")
        crawl_pdf_dir = join(crawl_data_dir, "policy_pdf")
        crawl_txt_dir = join(root_extracted_txt_dir, crawler_subdir)
        crawl_no, crawler_no = crawler_subdir.split("/")

        for l in open(log_file):
            log_line = l.rstrip()
            # only process log lines that include one of the following
            if "OK. Successfully saved the policy page" in log_line:
                log_type = ".html"
            elif "OK. Successfully saved the policy PDF" in log_line:
                log_type = ".pdf"
            elif "ERR-403: Readability script failed" in log_line:
                log_type = "readability_fail"
            elif "Success: found policy link" in log_line:
                log_type = "policy_link"
            else:
                continue

            timestamp, message, visit_info = parse_log_line(log_line)
            # build a visit_key to uniquely identify visits
            visit_key = (
                crawl_no, crawler_no, visit_info["homepage_snapshot_url"],
                visit_info["attempt_no"])
            if log_type == "readability_fail":
                readability_fails.add(visit_key)
                continue
            if log_type == "policy_link":
                policy_link_details_str = message.split(
                    "Success: found policy link  ")[-1]
                policy_link_details = json.loads(policy_link_details_str)
                raw_link_text = policy_link_details['link_text']
                homepage_snapshot_redirected_url =\
                    policy_link_details['current_page_url']
                link_text = ' '.join(raw_link_text.strip().split())
                lowercase_link_text = link_text.lower()
                matching_pattern = ""
                exact_match = False
                if lowercase_link_text in EXACT_POLICY_TITLES:
                    exact_match = True
                else:
                    for matching_pattern in PARTIAL_POLICY_TITLES:
                        if matching_pattern in lowercase_link_text:
                            break
                    else:
                        sys.exit("Unknown link match %s" % log_line)

                policy_links[visit_key] = [
                    link_text, exact_match, matching_pattern,
                    homepage_snapshot_redirected_url]
                continue
            if should_exclude_policy(visit_info):
                # print("Will skip %s" % visit_info)
                excluded_policies.add(visit_key)
                continue
            if (visit_info["homepage_url"], visit_info["year"],
                    visit_info["season"]) in processed_snapshots:
                print("Already processed, will skip - multiple visits",
                      processed_snapshots[
                          (visit_info["homepage_url"], visit_info["year"],
                           visit_info["season"])])
                continue

            saved_policy_filename = message.split(" ")[-1]
            if log_type is ".html":
                policy_file_type = "html"
                policy_html_path = join(crawl_policy_html_dir,
                                        saved_policy_filename)
                raw_source_path = policy_html_path.replace(root_data_dir, "")
                if not isfile(policy_html_path):
                    sys.exit("Missing policy_html file %s\n%s" % (
                        policy_html_path, log_line))

                readable_html_file = re.sub(
                    r'(.*)_privacy.html', r'\1_readable.html',
                    saved_policy_filename)
                readable_html_path = join(crawl_readable_html_dir,
                                          readable_html_file)
                if not isfile(readable_html_path):
                    if visit_key in readability_fails:
                        continue
                    sys.exit("Missing readable_html file %s\n%s" % (
                        policy_html_path, log_line))

                extracted_file = readable_html_file
                extracted_file_path = readable_html_path
                raw_policy_source = open(readable_html_path).read()
            elif log_type is ".pdf":
                policy_file_type = "pdf"
                policy_pdf_path = join(crawl_pdf_dir, saved_policy_filename)
                raw_source_path = policy_pdf_path.replace(root_data_dir, "")
                if not isfile(policy_pdf_path):
                    sys.exit("Missing policy_pdf file %s\n%s" % (
                        policy_pdf_path, log_line))

                file_type = magic.from_file(policy_pdf_path)
                if not file_type.startswith("PDF document"):
                    print("Bad PDF %s %s %s" % (
                        file_type, policy_pdf_path, log_line))
                    continue
                extracted_file = saved_policy_filename
                extracted_file_path = policy_pdf_path
                with open(policy_pdf_path, 'rb') as f:
                    raw_policy_source = base64.b64encode(f.read())

            extracted_txt_filename = re.sub(
                r'(.*).%s' % policy_file_type, r'\1.txt', extracted_file)
            extracted_txt_path = join(crawl_txt_dir, extracted_txt_filename)
            if not isfile(extracted_txt_path):
                print("Missing policy_txt file %s\nSource: %s\n%s" % (
                        extracted_txt_path, extracted_file_path, log_line))
                continue

            policy_txt = open(extracted_txt_path).read()
            if should_exclude_policy_by_text(extracted_txt_path, policy_txt):
                excluded_policies.add(visit_key)
                continue

            visit_info["crawl_no"] = crawl_no
            visit_info["crawler_no"] = crawler_no
            link_text, exact_match, matching_pattern, \
                homepage_snapshot_redirected_url = policy_links[visit_key]
            link_pattern_matches[
                    link_text if exact_match else matching_pattern] += 1
            link_texts[link_text] += 1
            processed_snapshots[
                (visit_info["homepage_url"],
                 visit_info["year"],
                 visit_info["season"])] = (crawl_no, crawler_no)

            year = int(visit_info["year"])
            interval = "%s_%s" % (year, visit_info["season"])
            domain = visit_info["homepage_url"].split("/")[-1]
            alexa_rank = None
            if interval in site_ranks:
                alexa_rank = site_ranks[interval].get(domain, None)

            extracted_policies.append(
                [timestamp,
                 visit_info["homepage_url"],
                 visit_info["homepage_snapshot_url"],
                 visit_info["policy_snapshot_url"],
                 year,
                 visit_info["season"],
                 alexa_rank,
                 policy_txt,
                 raw_policy_source,
                 raw_source_path,
                 policy_file_type,
                 link_text,
                 exact_match,
                 matching_pattern,
                 homepage_snapshot_redirected_url,
                 str(visit_info)
                 ])
        print("Will insert %d records" % (len(extracted_policies)))
        db_conn.executemany(
            "INSERT INTO policy_texts "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            extracted_policies)
        n_total_policies += len(extracted_policies)
        extracted_policies = []

    db_conn.commit()
    db_conn.close()
    print("Total num of policies %d" % n_total_policies)
    print("Matching policy link patterns")
    print(link_pattern_matches)
    print("Top 100 policy link text")
    print(link_texts.most_common(100))
    print("Total no of excluded %d" % len(excluded_policies))


def create_db(db_path):
    db_conn = sqlite3.connect(db_path)
    # Create table
    db_conn.execute('''
    CREATE TABLE policy_texts
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
                 )''')
    return db_conn


if __name__ == '__main__':
    root_data_dir = "../data/crawl/"
    process_crawl_data(root_data_dir)
