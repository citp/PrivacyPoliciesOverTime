import json
import pandas as pd

from glob import glob
from os.path import join, isdir
from common import get_visit_info_from_log_line
from tld import get_fld

## Use crawl logs to analyze crawl failures

def serialize_visit_info(visit_info):
    v = visit_info
    return [
        v.homepage_url,
        v.homepage_snapshot_url,
        v.policy_snapshot_url,
        int(v.year),
        v.season,
        "%s_%s" % (v.year, v.season),
        int(v.attempt_no),
        int(v.url_id)]


LOAD_TIME_MSGS = [
    "Loaded in",
    "Loaded policy page in",
    "OK: Successfully crawled in",
    "CDX query took:"
    ]


def get_load_time_from_log_line(log_line, load_msg):
    assert load_msg in log_line
    return float(log_line.split(load_msg)[-1].split()[0].rstrip("s"))


def get_msg_token_from_log_str(log_str):
    """Convert log strings to one-word, lower case tokens"""
    return "_".join(log_str.lower().split())


def get_next_string_in_log(log_line, log_str, offset=0):
    return log_line.split(log_str)[-1].split()[offset]


IGNORABLE_LOG_MSGS = [
    "Will download policy html",
    "PDF link, will download",
    "Detector is not able to detect the language reliably.",
    "Will download",
    "Out-of-bounds redirection",
    "Absolute policy link:",
    "javascript policy link:",
    "Will skip non-english site",
    "Will crawl domains in",
    "Will skip already crawled snapshot",
    "Broken policy link"
    ]

ERR_MSGS = [
    "Exception: net::ERR_TOO_MANY_REDIRECTS",
    "Exception: net::ERR_ABORTED",
    "Exception: net::ERR_INVALID_RESPONSE",
    "Exception: net::ERR_CONNECTION_REFUSED",
    "Exception: net::ERR_SPDY_PROTOCOL_ERROR",
    "Exception: net::ERR_SPDY_SERVER_REFUSED_STREAM",
    "Exception: net::ERR_UNEXPECTED_PROXY_AUTH",
    "Exception: net::ERR_CONNECTION_CLOSED",
    "Exception: Navigation Timeout Exceeded",
    "Exception: SoftTimeLimitExceeded",
    "Redirected to error page",
    "Task was destroyed but it is pending",
    "Task exception was never retrieved",
    "Future exception was never retrieved",
    "Redirected to page with invalid timestamp",
    "Soft time limit (240s) exceeded for",
    "HTTP Err 429/503: Will decrement the attempt count",
    "Error: NavigationBlocked",
    "Error: PDF download error:",
    "Exception: Exception while downloading the PDF",
    "ERROR OSError: [Errno 104] Connection reset by peer",
    "ERROR OSError: [Errno 2] No such file or directory",
    "ERROR OSError: [Errno 17] File exists: '/home/ubuntu/.local/share/pyppeteer/.dev_profile'",
    ]


def analyze_wayback_crawl_logs(log_paths_or_root_dir):
    already_processed = {}
    skipped_snapshots = set()
    blocked_document_urls = []
    line_no = 0
    log_msgs = []
    n_domains = 0
    n_snapshots = 0

    if isdir(log_paths_or_root_dir):
        log_paths = gen_log_files(log_paths_or_root_dir)
    else:
        log_paths = log_paths_or_root_dir

    for log_path in log_paths:
        print("Will process %s" % log_path)
        log_name = "/".join(log_path.rsplit("/", 4)[-4:])
        line_no = 0
        for l in open(log_path):
            line_no += 1
            log_line = l.rstrip()
            # multiline log, e.g. html code
            if not log_line.startswith("[20") and not log_line.startswith("20"):
                continue

            if any(ignorable_msg in log_line for ignorable_msg in IGNORABLE_LOG_MSGS):
                continue

            if "INFO Queued" in log_line and "snapshots from" in log_line:
                n_snapshots += int(get_next_string_in_log(log_line, "INFO Queued"))
                n_domains += int(get_next_string_in_log(log_line, "snapshots from"))
                continue

            if "Blocked non-archived resource" in log_line:
                document_blocked_msg = "Blocked non-archived resource (document):"
                if document_blocked_msg in log_line:
                    blocked_url = get_next_string_in_log(log_line, document_blocked_msg)
                    blocked_document_urls.append(blocked_url)
                continue

            visit_info, err_code, timestamp = get_visit_info_from_log_line(log_line)
            if visit_info is None:
                print("TODO - handle", log_line)
                continue
            # handle this in pandas to avoid overwriting successful visits
            SKIP_REPEATED_VISITS = False
            if SKIP_REPEATED_VISITS:
                visit_key = (visit_info.homepage_url, visit_info.year,
                             visit_info.season)
                if visit_key in already_processed:
                    if already_processed[visit_key] != log_name:
                        # print("Will skip an repated visit from", visit_key, visit_info.timestamp)
                        skipped_snapshots.add((visit_info.homepage_snapshot_url, log_name))
                        continue
                else:
                    already_processed[visit_key] = log_name

            if any(err_msg in log_line for err_msg in ERR_MSGS):
                for err_msg in ERR_MSGS:
                    if err_msg in log_line:
                        msg_token = get_msg_token_from_log_str(err_msg)
                        log_msgs.append((timestamp, "error", msg_token, "", *serialize_visit_info(visit_info), log_name, line_no))
                        break
            # check all load time messages
            elif any(load_msg in log_line for load_msg in LOAD_TIME_MSGS):
                for load_msg in LOAD_TIME_MSGS:
                    if load_msg in log_line:
                        if load_msg == "Loaded in":
                            msg = get_next_string_in_log(log_line,
                                                         "Current url:")
                        else:
                            load_time = get_load_time_from_log_line(log_line, load_msg)
                            msg = "%0.1f" % load_time
                        msg_type = get_msg_token_from_log_str(load_msg)
                        log_msgs.append((timestamp, msg_type, msg, "", *serialize_visit_info(visit_info), log_name, line_no))
                        break
            elif "New crawl task" in log_line:
                log_msgs.append((timestamp, "new_crawl_task", "", "", *serialize_visit_info(visit_info), log_name, line_no))
            elif "OK. Successfully saved the policy " in log_line:
                policy_file_type = get_next_string_in_log(log_line, "OK. Successfully saved the policy")
                log_msgs.append((timestamp, "policy_saved", policy_file_type, "", *serialize_visit_info(visit_info), log_name, line_no))
            elif "Error: HttpStatusError: Status code" in log_line:
                status_code = get_next_string_in_log(log_line, "Status code:")
                msg_token = "http_status_error_%s" % status_code
                log_msgs.append((timestamp, "error", msg_token, "", *serialize_visit_info(visit_info), log_name, line_no))
            elif "Exception: get_snapshot_url" in log_line:
                n_try = get_next_string_in_log(log_line, "get_snapshot_url. nTry:")
                msg_token = "get_snapshot_url_try_%s" % n_try
                log_msgs.append((timestamp, "error", msg_token,
                                 "", *serialize_visit_info(visit_info), log_name, line_no))
            elif "CDX - IndexError" in log_line:
                if "<title>Internet Archive: Scheduled Maintenance</title>" in log_line:
                    log_msgs.append((timestamp, "error", "cdx_err_ia_scheduled_maintenance", "", *serialize_visit_info(visit_info), log_name, line_no))
                elif "<h1>Too Many Requests</h1>" in log_line:
                    log_msgs.append((timestamp, "error", "cdx_err_too_many_requests", "", *serialize_visit_info(visit_info), log_name, line_no))
                elif "<h1>408 Request Time-out</h1>" in log_line:
                    log_msgs.append((timestamp, "error", "cdx_err_request_time_out", "", *serialize_visit_info(visit_info), log_name, line_no))
                else:
                    print("CDX - IndexError (TODO)", log_line)
            elif "Success: found policy link" in log_line:
                policy_json_str = log_line.split("Success: found policy link  ")[-1].split(" VisitInfo: ")[0]
                policy_details = json.loads(policy_json_str)
                msg_token = policy_details["policy_abs_url"]
                log_msgs.append((timestamp, "policy_link_found", msg_token, "", *serialize_visit_info(visit_info), log_name, line_no))
            elif err_code:
                policy_url = ""
                if err_code == 305:
                    policy_url = get_next_string_in_log(
                        log_line,
                        "Policy page is not archived during interval")
                    policy_domain = get_fld(policy_url, fail_silently=True)
                    # error postprocessing
                    # if policy url is not archived because it's malformed
                    # (e.g. http://privacy.htm)
                    # relabel it as a different error
                    # TODO: assign a different err code (399) in the crawler
                    if policy_domain is None:
                        err_code = 399
                log_msgs.append((timestamp, "error", "err_%s" % err_code, policy_url, *serialize_visit_info(visit_info), log_name, line_no))
            else:
                if "ERROR Exception: " in log_line:
                    print("ERROR Exception: ", log_line)
                    continue
                raise ValueError("Unexpected log format %s" % log_line)
    print("No of queued domains", n_domains)
    print("No of queued snapshots", n_snapshots)
    # blocked_cnts = Counter(blocked_document_urls)
    # print("n_blocked_document_urls", len(blocked_document_urls))
#     print(blocked_cnts.most_common(10))
    return pd.DataFrame(
        log_msgs, columns=[
            'timestamp', 'log_type', 'log_info',
            'policy_url',
            'site_url',
            'homepage_snapshot_url',
            'policy_snapshot_url',
            'year', 'season',
            'interval',
            'attempt_no', 'url_id',
            'log_file_name', 'line_no']), blocked_document_urls, skipped_snapshots


def gen_log_files(root_data_dir):
    return reversed(sorted(
        glob(join(root_data_dir, "crawl*/data-*/logs/puppet_downloader.log"))))


if __name__ == '__main__':
    # use the following to analyze the individual log files
    # analyze_wayback_crawl_logs([LOG_PATH,])
    root_data_dir = "../data/crawl/"

    analyze_wayback_crawl_logs(gen_log_files(root_data_dir))
