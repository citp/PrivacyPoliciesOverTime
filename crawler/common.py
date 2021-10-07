import json
import sys
from os.path import join

DISPLAY_W = 1366
DISPLAY_H = 768
VIRT_DISPLAY_DIMS = (DISPLAY_W, DISPLAY_H)


OUT_DIR = "../out"
POLICY_TEXT_DIR = join(OUT_DIR, "policy_text")
POLICY_HTML_DIR = join(OUT_DIR, "policy_html")
READABLE_POLICY_HTML_DIR = join(OUT_DIR, "readable_policy_html")
POLICY_PDF_DIR = join(OUT_DIR, "policy_pdf")


class HttpStatusError(Exception):
    pass


class VisitInfo():
    """Hold data about a visit."""
    def __init__(self, homepage_url, attempt_no, url_id, year,
                 season, timestamp, homepage_snapshot_url,
                 lang_check=False,
                 policy_snapshot_url=""):
        self.homepage_url = homepage_url
        self.homepage_snapshot_url = homepage_snapshot_url
        self.attempt_no = attempt_no
        self.url_id = url_id
        self.year = year
        self.season = season
        self.stage = "load_homepage"
        self.timestamp = timestamp
        self.lang_check = lang_check
        self.policy_snapshot_url = policy_snapshot_url


# TODO: move this to a log_util file
def get_visit_info_from_log_line(log_line):
    if not log_line.startswith("[20") and not log_line.startswith("20"):
        return None, None, None

    if " VisitInfo: " not in log_line:
        return None, None, None

    log_line = log_line.rstrip()

    prefix_and_message, visit_info_str = log_line.split(" VisitInfo: ")
    try:
        visit_info = json.loads(visit_info_str)
    except:
        sys.exit("Cannot parse visit_info JSON %s" % log_line)
    # prefix, message = prefix_and_message.split("] ", 1)
    day, time, _, message = prefix_and_message.split(" ", 3)
    timestamp = "%s %s" % (day, time)
    err_code = 0
    if message.startswith("ERR-"):
        msg_items = message.split()
        err_code = int(msg_items[0].rstrip(":").split("ERR-")[-1])

    # TODO: add prefix parsing to get log time
    visit_info_obj = VisitInfo(
        visit_info["homepage_url"], visit_info["attempt_no"],
        visit_info["url_id"], visit_info["year"],
        visit_info["season"], visit_info["timestamp"],
        visit_info["homepage_snapshot_url"],
        visit_info["lang_check"],
        visit_info["policy_snapshot_url"])

    return visit_info_obj, err_code, timestamp
