import urllib.parse
import requests
import json
from datetime import datetime
from detect_links import find_privacy_policy_link
from common import HttpStatusError

from os.path import dirname, join
READABILITY_JS_PATH = join(dirname(__file__), 'js/readability/Readability.js')
READABILITY_SRC = open(READABILITY_JS_PATH).read()

PAGE_LOAD_TIMEOUT = 90000


async def load_page(page, target_url, url_id=-1, timeout=PAGE_LOAD_TIMEOUT):
    response = await page.goto(target_url, {
        'waitUntil': ['networkidle0', 'domcontentloaded'],
        'timeout': timeout})
    if response is None or not response.ok:  # 40X, 50X etc.
        status = response.status if response else "Unknown"
        if page.url == "chrome-error://chromewebdata/":
            raise HttpStatusError(
                "NavigationBlocked: Status code: %s %s (id: %s)" % (
                    status, target_url, url_id))
        else:
            raise HttpStatusError(
                "HttpStatusError: Status code: %s %s (id: %s)" % (
                    status, target_url, url_id))
    return response


async def get_page_text(page):
    return await page.evaluate('''() => {
            return {text: document.body.innerText}
        }''')


async def get_readable_html(page):
    """Execute mozilla/Readability script to declutter HTML."""
    return await page.evaluate('''() => {
        %s;
        let reader = new Readability(window.document);
        const article = reader.parse();
        return article && article.content;
    }''' % READABILITY_SRC)


async def get_page_links(page):
    js_result = await page.evaluate('''() => {
        let links = document.getElementsByTagName('a');
        return {
            links: Array.from(links).map(x => [x.text, x.href]),
        }
        }''')
    if js_result is None or "links" not in js_result:
        return None
    return [{"text": link[0].strip(), "url": link[1].strip()}
            for link in js_result["links"]]


async def get_policy_link(page):
    links = await get_page_links(page)
    return find_privacy_policy_link(links, page.url, cc_links=False)


def get_cdx_url(cdx_params):
    CDX_BASE_ADDRESS = "web.archive.org/cdx/search/cdx"
    USE_HTTPS = False
    if USE_HTTPS:
        CDX_BASE_URL = "https://%s" % CDX_BASE_ADDRESS
    else:
        CDX_BASE_URL = "http://%s" % CDX_BASE_ADDRESS
    return "%s?%s" % (CDX_BASE_URL, urllib.parse.urlencode(cdx_params))


ERR_OK = 0
ERR_BLOCKED_SITE = -1
ERR_EMPTY_RESPONSE = -2
ERR_INVALID_TIMESTAMP = -3
ERR_WAYBACK_EXCEPTION = -4
ERR_UNKNOWN_FAILURE = -5


HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/76.0.3809.100 Chrome/76.0.3809.100 Safari/537.36',  # noqa
    'Accept-Language': 'en-US,en;q=0.5',
    'Cache-Control': 'no-cache',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',  # noqa
    'Pragma': 'no-cache'
}


def load_cdx_page(cdxurl=None, cdx_params=None, logger=None):
    if cdxurl is None:
        cdxurl = get_cdx_url(cdx_params)
    r = requests.get(cdxurl, headers=HTTP_HEADERS)
    body = r.text if r is not None else ""
    if r and r.status_code == requests.codes.ok:  # noqa
        if body:
            return body, r.status_code, ERR_OK
        else:
            if logger:
                logger.warning("ERR-498: Empty CDX response (no archive matching"
                               " the query): %s" % (cdxurl))
            return body, r.status_code, ERR_EMPTY_RESPONSE
    elif "Blocked Site Error" in body:  # adult sites etc.
        if logger:
            logger.warning(
                "ERR-202: CDX Blocked site: %s %s" % (cdxurl, body.strip()))
        return None, None, ERR_BLOCKED_SITE
    elif ("org.archive.wayback.exception" in body or
          "java.net.SocketTimeoutException" in body):
        if logger:
            logger.error("ERR-201: CDX error: %s %s" % (cdxurl, body))
        raise HttpStatusError(
            "ERR-201: CDX error%s %s Body: %s" % (cdxurl, r.status_code, body))
    else:
        if logger:
            logger.error(
                "HTTP request error %s %s %s" % (cdxurl, r.status_code, body))
        raise HttpStatusError("%s %s " % (cdxurl, r.status_code))


def is_valid_wb_timestamp(ts):
    try:
        # int(ts)
        datetime.strptime(ts, '%Y%m%d%H%M%S')
        return True
    except ValueError:
        return False
    except TypeError:
        return False


def read_snapshots(fname, deduplicate_by_hostname=False):
    """Extract domain->snapshot-timestamp mappings from
    celery_get_wayback_timestamps logs."""
    domain_snapshots = {}
    for l in open(fname):
        # timestamp logs are prefixed with "TIMESTAMPS"
        if "TIMESTAMPS" not in l:
            continue
        items = l.split(" ")
        if len(items) != 9 and not (
                items[0] == "TIMESTAMPS:" and len(items) == 3):
            print("Unexpected log format", len(items))
            continue
        domain = items[-2]
        if deduplicate_by_hostname:
            domain = domain.split("/")[0]
            if domain in domain_snapshots:
                continue
        # timestamps are separated by comma
        timestamps = items[-1].strip().split(",")
        domain_snapshots[domain] = timestamps
    return domain_snapshots


def get_visit_info_from_log_line(log_line):
    visit_info_str = log_line.split(" VisitInfo: ")[-1]
    return json.loads(visit_info_str)


def read_lang_detect_logs(lang_detect_log):
    english_sites = set()
    non_english_sites = set()
    all_sites = set()
    for log_line in open(lang_detect_log):
        if "New crawl task" in log_line:
            visit_info = get_visit_info_from_log_line(log_line)
            all_sites.add(visit_info['homepage_url'])
        elif "Detected English page" in log_line:
            visit_info = get_visit_info_from_log_line(log_line)
            english_sites.add(visit_info["homepage_url"])
        elif "Non-english page" in log_line:
            visit_info = get_visit_info_from_log_line(log_line)
            non_english_sites.add(visit_info["homepage_url"])

    detected_sites = english_sites.union(non_english_sites)
    unknown_sites = all_sites.difference(detected_sites)
    return english_sites, non_english_sites, unknown_sites



FETCH_TIMEOUT = 20


def fetch_url(url, timeout=FETCH_TIMEOUT, headers=HTTP_HEADERS):
    """Fetch and return the contents of a given URL.

    Retry with cert verification disabled if we get an
    SSLError. Some servers don't send intermediate certificates, which causes
    `requests.get` to fail with an SSLError.
    https://github.com/requests/requests/issues/3212

    Standard browsers would download the missing certs, without users
    noticing anything wrong.
    """
    try:
        r = requests.get(url, timeout=timeout, headers=headers)
        content = r.content if url.endswith('.pdf') else r.text
        return content, r.url, r.status_code
    except requests.exceptions.SSLError:
        r = requests.get(url, timeout=timeout, headers=headers,
                         verify=False)
        print("Downloaded the policy over insecure connection %s" % url)
        content = r.content if url.endswith('.pdf') else r.text
        return content, r.url, r.status_code


if __name__ == '__main__':
    pass
