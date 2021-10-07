import re
import json
import random
import asyncio
import pyppeteer

import logging
import sys
import urllib.parse
import requests

from collections import defaultdict
from os.path import join, isfile
from time import time, sleep
from celery import Celery
from celery.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded

from polyglot.detect import Detector
from polyglot.detect.base import UnknownLanguage
from tld import get_fld


from common import (VisitInfo, POLICY_HTML_DIR,
                    POLICY_PDF_DIR,
                    READABLE_POLICY_HTML_DIR,
                    DISPLAY_W, DISPLAY_H, HttpStatusError)

from util import write_to_file, mkdir, safe_filename_from_url
from crawl_util import (fetch_url, get_readable_html, get_policy_link,
                        get_visit_info_from_log_line, read_lang_detect_logs,
                        load_page, get_page_text, get_cdx_url,
                        is_valid_wb_timestamp, read_snapshots,
                        ERR_OK, ERR_BLOCKED_SITE, ERR_EMPTY_RESPONSE,
                        ERR_INVALID_TIMESTAMP, ERR_WAYBACK_EXCEPTION,
                        ERR_UNKNOWN_FAILURE, PAGE_LOAD_TIMEOUT)


READABILTY_FAILURE_STR = '<div class="reader-message" style="display: block;">Failed to load article from page</div>'  # noqa


class PdfDownloadError(Exception):
    pass


class WaybackRedirectionError(Exception):
    pass


# To fix intermittent websocket errors (code=1006)
# https://github.com/miyakogi/pyppeteer/pull/160#issuecomment-448886155
def patch_pyppeteer():
    import pyppeteer.connection
    original_method = pyppeteer.connection.websockets.client.connect

    def new_method(*args, **kwargs):
        kwargs['ping_interval'] = None
        kwargs['ping_timeout'] = None
        return original_method(*args, **kwargs)

    pyppeteer.connection.websockets.client.connect = new_method


app = Celery('celery_crawl_wayback',
             broker='pyamqp://guest@localhost//')


# throttle to cope with wayback machine's obscure rate limits
# We haven't used rate-limiting in the last policy crawl
# Instead we've used EC2 instances with limited resources
MAX_NUM_OF_TASKS_PER_MIN = 120
app.control.rate_limit(
    'celery_crawl_wayback.crawl_wayback_snapshot',
    '%d/m' % MAX_NUM_OF_TASKS_PER_MIN)


logger = logging.getLogger('puppet_downloader')
hdlr = logging.FileHandler('puppet_downloader.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)

ch = logging.StreamHandler()
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

DEBUG = False
USE_INCOGNITO_MODE = True
ENABLE_JS_LINK_DOWNLOADS = False  # TODO


def is_date_within_bounds(date, redirected_date):
    target_year = int(date[:4])
    target_month = int(date[4:6])
    redirected_year = int(redirected_date[:4])
    redirected_month = int(redirected_date[4:6])
    if redirected_year != target_year:
        return False
    if target_month <= 6:
        return redirected_month <= 6
    else:
        return redirected_month > 6


def get_abs_url_from_wb_url(policy_url, page_wb_url):
    """Return the original URL given an archive link."""
    page_url = "http" + page_wb_url.split("/http", 1)[-1]
    page_base_url = page_url.rsplit("/", 1)[0]
    if policy_url.lower().startswith("javascript:"):
        logger.warning("javascript policy link: %s %s" % (
            policy_url, page_wb_url))
        return None
    if not policy_url.startswith("http://") and\
            not policy_url.startswith("https://"):
        # return page_url + policy_url
        return urllib.parse.urljoin(page_base_url, policy_url)

    if policy_url.startswith("https://web.archive.org/web/"):
        if "id_/http" in policy_url:
            # +4 is to offset `id_/`
            return policy_url[policy_url.index('id_/http') + 4:]
        elif "id_/" in policy_url:
            relative_url = policy_url.split("id_/", 1)[-1]
            return urllib.parse.urljoin(page_base_url, relative_url)
        else:
            relative_url = re.sub(
                r'https://web.archive.org/web/[0-9]*/', '', policy_url)
            if relative_url.startswith("http"):
                return relative_url
            else:
                return urllib.parse.urljoin(page_base_url, relative_url)
            # return policy_url[policy_url.index('/http')+1:]
    WEB_ARCHIVE_ORG_HTTPS_BASE_URL = "https://web.archive.org/"
    WEB_ARCHIVE_ORG_HTTP_BASE_URL = "http://web.archive.org/"
    WEB_ARCHIVE_ORG_HTTPS_WWW_BASE_URL = "https://www.archive.org/"
    WEB_ARCHIVE_ORG_HTTP_WWW_BASE_URL = "http://www.archive.org/"

    WB_BASE_URLS = [
        WEB_ARCHIVE_ORG_HTTPS_BASE_URL,
        WEB_ARCHIVE_ORG_HTTP_BASE_URL,
        WEB_ARCHIVE_ORG_HTTPS_WWW_BASE_URL,
        WEB_ARCHIVE_ORG_HTTP_WWW_BASE_URL]

    for base_url in WB_BASE_URLS:
        if policy_url.startswith(base_url):
            relative_url = policy_url.replace(base_url, "")
            # return page_url + policy_url
            return urllib.parse.urljoin(page_base_url, relative_url)

    logger.warning("Absolute policy link: %s %s" % (
        policy_url, page_wb_url))
    return policy_url


async def get_printable_text(page):
    js_result = await get_page_text(page)
    text = js_result["text"]
    printable_text = ''.join(ch for ch in text
                             if ch.isprintable() or ch == "\n")
    return printable_text.strip()


async def get_text_language(page_text):
    try:
        detector = Detector(page_text)
    except UnknownLanguage as exc:
        return "un", exc

    return detector.languages[0].code, None


# don't download images to minimize the crawler's bandwidth footprint
# on wayback machine servers
DISABLE_IMAGES = True
ENABLE_HEADLESS = True
# don't download resources from live web pages
BLOCK_NON_ARCHIVED_RESOURCES = True
ARCHIVE_PREFIX = "https://web.archive.org/"


async def launch_browser(
    headless=ENABLE_HEADLESS, ignoreHTTPSErrors=False,
    use_incognito=True, disable_images=DISABLE_IMAGES,
        block_non_archived_resources=BLOCK_NON_ARCHIVED_RESOURCES):

    browser = await pyppeteer.launch(
        {'headless': headless,
         'ignoreHTTPSErrors': ignoreHTTPSErrors,
         'autoClose': False}
         )
    if use_incognito:
        context = await browser.createIncognitoBrowserContext()
        page = await context.newPage()
    else:
        page = await browser.newPage()
    await page.setViewport({'width': DISPLAY_W,
                            'height': DISPLAY_H})

    # TODO - separate image blocking and non_archived_resource blocking
    if disable_images:
        await page.setRequestInterception(True)

        async def block_images_and_non_archived_content(req):
            if req.resourceType == 'image':
                await req.abort()
            elif (block_non_archived_resources and
                  not req.url.startswith(ARCHIVE_PREFIX)):
                log_("debug", logger, None,
                     msg="Blocked non-archived resource (%s): %s" % (
                         req.resourceType, req.url))
                await req.abort()
            else:
                await req.continue_()

        page.on(
            'request', lambda req: asyncio.ensure_future(
                block_images_and_non_archived_content(req)))

    return browser, page


async def close_browser(browser, page):
    try:
        await page.close()
    except Exception:
        pass
    await browser.close()


async def is_english_site(url, timestamps, url_id):
    browser, page = await launch_browser()
    target_url = get_snapshot_url_by_timestamp(url, timestamps[-1])

    try:
        await load_page(page, target_url, url_id, timeout=PAGE_LOAD_TIMEOUT)
        if is_invalid_redirection(page, target_url):
            return False

        await page.content()
        page_text = await get_printable_text(page)
        if not len(page_text):
            return False

        text_lang, detection_err = await get_text_language(page_text)
        if detection_err is not None:
            return False

        if text_lang == "en":
            return True
    except Exception:
        return False
    finally:
        await close_browser(browser, page)


async def download_policy(visit_info):
    target_url = visit_info.homepage_snapshot_url
    year = visit_info.year
    season = visit_info.season
    url_id = visit_info.url_id
    log_("info", logger, visit_info, "Will download %s" % target_url)

    t0 = time()
    browser, page = await launch_browser()
    try:
        await load_page(page, target_url, url_id, timeout=PAGE_LOAD_TIMEOUT)
        load_time = time() - t0
        log_("info", logger, visit_info, "Loaded in %0.1fs Current url: %s" % (
            load_time, page.url))
        invalid_redirection, reason = is_invalid_redirection(page, target_url,
                                                             visit_info)
        if invalid_redirection:
            log_("error", logger, visit_info,
                 "ERR-301: Invalid redirection while loading homepage: %s "
                 "Current url: %s" % (reason, page.url))
            return

        # await for the page content
        await page.content()
        page_text = await get_printable_text(page)
        if not len(page_text):
            log_("error", logger, visit_info, "ERR-309: Blank homepage")
            return

        text_lang, detection_err = await get_text_language(page_text)
        if detection_err is not None:
            log_("error", logger, visit_info,
                 "ERR-308: Error detecting page language: %s text_len: %s %s" %
                 (detection_err, len(page_text), page.url))
            return

        if text_lang != "en":
            log_("error", logger, visit_info,
                 "ERR-302: Non-english page %s" % text_lang)
            return

        if visit_info.lang_check:
            log_("info", logger, visit_info,
                 "Detected English page %s" % text_lang)
            return

        policy_wb_link = await get_policy_link(page)
        if not policy_wb_link:
            log_("debug", logger, visit_info,
                 "ERR-303: No policy found on %s" % page.url)
            return None
        archived_policy_url, link_text = policy_wb_link
        # We found a privacy policy link
        policy_url = get_abs_url_from_wb_url(archived_policy_url, page.url)
        privacy_link_domain = get_fld(policy_url, fail_silently=True)
        if privacy_link_domain == "archive.org":
            log_("debug", logger, visit_info,
                 "Broken policy link %s" % policy_url)
            return

        policy_link_details = {
            "policy_abs_url": policy_url,
            "current_page_url": page.url,
            "archived_policy_url": archived_policy_url,
            "link_text": link_text
            }

        log_("debug", logger, visit_info,
             "Success: found policy link  %s" %
             json.dumps(policy_link_details))

        # TODO combine this with if privacy_link_domain == "archive.org"
        if policy_url == "https://web.archive.org":
            log_("debug", logger, visit_info,
                 "ERR-314: Cannot get the policy link  %s %s"
                 % (policy_url, page.url))
            return

        if not policy_url:
            log_("debug", logger, visit_info,
                 "ERR-304: Cannot get the absolute URL for policy link  %s %s"
                 % (policy_wb_link[0], page.url))
            return

        start, end = get_start_end_for_season(year, season)
        policy_snapshot_url, err_code = get_snapshot_url(
            policy_url, start, end, visit_info)
        if not policy_snapshot_url:
            # find snapshots with non-200 codes only if the site isn't blocked
            if err_code != ERR_BLOCKED_SITE:
                policy_snapshot_url, err_code = get_snapshot_url(
                    policy_url, start, end, visit_info, only_200_ok=False)
            if not policy_snapshot_url:
                log_(
                    "debug", logger, visit_info,
                    "ERR-305: Policy page is not archived during interval %s" %
                    policy_url)
                return
            log_("debug", logger, visit_info,
                 "ERR-356: No policy snapshots with status=200, "
                 "fell back to non-200 %s" %
                 policy_url)

        visit_info.policy_snapshot_url = policy_snapshot_url
        if policy_snapshot_url.endswith(".pdf"):
            download_policy_pdf(policy_snapshot_url, visit_info)
        else:
            await download_policy_html(visit_info, page)
        # take a screenshot for debugging
        # await page.screenshot({'path': '%s.png' % domain})

    finally:
        try:
            await page.close()
        except Exception:
            pass
        await browser.close()


def get_wb_file_name(url, year, season, ext, url_id=0):
    safe_path = safe_filename_from_url(url)
    return "%s_%s_%s_%s_%s" % (url_id, year, season, safe_path, ext)


def download_policy_pdf(url, visit_info):
    log_("info", logger, visit_info, "PDF link, will download")
    try:
        content, url, status_code = fetch_url(url)
    except Exception as exc:
        raise PdfDownloadError("Exception while downloading the PDF %s" % exc)
    if content and status_code == requests.codes.ok:  # noqa
        safe_filename = get_wb_file_name(
            url, visit_info.year, visit_info.season, "privacy.pdf",
            visit_info.url_id)
        write_to_file(join(POLICY_PDF_DIR, safe_filename), content, mode='wb')
        log_("info", logger, visit_info,
             "OK. Successfully saved the policy PDF %s" % safe_filename)
    else:
        # we get a non-OK response while downloading the policy PDF
        raise HttpStatusError("PDF download error: %s" % status_code)


def get_date_from_wb_url(url):
    try:
        return url.split("/")[4].replace("id_", "")
    except IndexError:
        return None


WB_HOME_URL = "https://web.archive.org/"

# sometimes we are redirected to this page
WB_HOME_URL_2 = "https://web.archive.org/?z"


def is_invalid_redirection(page, target_url, visit_info=None):
    if page.url.rstrip("/") == target_url.rstrip("/"):
        return False, None

    if page.url == WB_HOME_URL:
        return True, "Wayback home page"

    if page.url == WB_HOME_URL_2:
        return True, "Wayback home page (2)"

    redirected_date = get_date_from_wb_url(page.url)
    if not is_valid_wb_timestamp(redirected_date):
        return True, "Invalid timestamp"
    target_date = get_date_from_wb_url(target_url)
    # ignore date-based redirection for lang check crawls
    if not visit_info.lang_check:
        if not is_date_within_bounds(target_date, redirected_date):
            return True, "Out-of-bound date"

    return False, None  # not an invalid redirection


async def download_policy_html(visit_info, page):
    target_url = visit_info.policy_snapshot_url
    year = visit_info.year
    season = visit_info.season
    url_id = visit_info.url_id
    visit_info.stage = "policy_download"

    log_("info", logger, visit_info, "Will download policy html")
    t0 = time()
    safe_filename = safe_filename_from_url(target_url)
    await load_page(page, target_url, url_id, timeout=PAGE_LOAD_TIMEOUT)
    load_time = time() - t0
    log_("info", logger, visit_info, "Loaded policy page in %0.1f" % load_time)

    invalid_redirection, reason = is_invalid_redirection(
        page, target_url, visit_info)
    if invalid_redirection:
        log_("error", logger, visit_info,
             "ERR-401: Invalid redirection while loading policy: %s "
             "Current url: %s" % (reason, page.url))
        return

    content = await page.content()
    page_text = await get_printable_text(page)
    if not len(page_text):
        log_("error", logger, visit_info, "ERR-409: Blank policy page")
        return

    text_lang, detection_err = await get_text_language(page_text)
    if detection_err is not None:
        log_("error", logger, visit_info,
             "ERR-408: Error detecting policy language: %s %s %s" % (
                 len(page_text), page.url, detection_err))
        return

    if text_lang != "en":
        log_("error", logger, visit_info,
             "ERR-402: Non-english policy page %s" % text_lang)
        return

    readable_html = await get_readable_html(page)
    # await page.screenshot({'path': '%s.png' % domain})

    if readable_html and READABILTY_FAILURE_STR not in readable_html:
        safe_filename = get_wb_file_name(target_url, year, season,
                                         "readable.html", url_id)
        write_to_file(join(READABLE_POLICY_HTML_DIR, safe_filename),
                      readable_html)
    else:
        log_("info", logger, visit_info,
             "ERR-403: Readability script failed for policy page")
    safe_filename = get_wb_file_name(
        target_url, year, season, "privacy.html", url_id)
    write_to_file(join(POLICY_HTML_DIR, safe_filename), content)
    log_("info", logger, visit_info,
         "OK. Successfully saved the policy page %s" % safe_filename)


def get_snapshot_url(url, start=None, end=None, visit_info=None,
                     only_200_ok=True):
    """Return a snapshot url for a given url and time interval."""

    cdx_params = [
        ("url", url),
        ("limit", "-2"),
        ("fastLatest", "true"),
        ("filter", "!length:-")]
    if only_200_ok:
        cdx_params.append(("filter", "statuscode:200"))

    if start is not None:
        cdx_params.append(("from", start))
    if end is not None:
        cdx_params.append(("to", end))

    cdxurl = get_cdx_url(cdx_params)
    n_tries = 0
    MAX_TRIES = 5
    MAX_BACKOFF = 7200
    while n_tries < MAX_TRIES:
        n_tries += 1
        try:
            t0 = time()
            body = requests.get(cdxurl).text.strip()
            if not body:
                return "", ERR_EMPTY_RESPONSE
            elif "Blocked Site Error" in body:  # adult sites etc.
                log_("debug", logger, visit_info,
                     "ERR-202: CDX Blocked site: %s %s" %
                     (cdxurl, body.replace("\n", "\\n")))
                return "", ERR_BLOCKED_SITE
            # we need to retry when we get a timeout error
            elif "java.net.SocketTimeoutException" in body:
                log_("debug", logger, visit_info,
                     "ERR-201: CDX Timeout error: %s %s" %
                     (cdxurl, body.replace("\n", "\\n")))
                raise HttpStatusError("SocketTimeoutException")
            # non-timeout error, no need to retry
            elif "org.archive.wayback.exception" in body:
                log_("debug", logger, visit_info, "ERR-203: CDX error: %s %s" %
                     (cdxurl, body.replace("\n", "\\n")))
                return "", ERR_WAYBACK_EXCEPTION

            last_row = body.split("\n")[-1].split(" ")
            last_row_ts = last_row[1]
            if not is_valid_wb_timestamp(last_row_ts):
                log_("debug", logger, visit_info,
                     "ERR-204: CDX error - timestamp: %s %s %s" % (
                         cdxurl, body.replace("\n", "\\n"), last_row_ts))
                return "", ERR_INVALID_TIMESTAMP
            log_("debug", logger, visit_info, "CDX query took: %0.1f %s" % (
                time() - t0, cdxurl))
            return get_snapshot_url_by_timestamp(url, last_row_ts), ERR_OK

        except IndexError:
            err_details = {"body": body, "cdxurl": cdxurl}
            log_("debug", logger, visit_info, "CDX - IndexError: %s" %
                 json.dumps(err_details))
            return "", ERR_INVALID_TIMESTAMP
        except SoftTimeLimitExceeded as tl_exc:
            raise tl_exc
        except Exception as exc:
            pause = min(((2**n_tries) * 5), MAX_BACKOFF)  # 5, 10...
            log_("error", logger, visit_info,
                 "Exception: get_snapshot_url. nTry: %s Err: %s Pause: %s" % (
                     n_tries, exc, pause))
            sleep(pause)
            continue
    log_("error", logger, visit_info,
         "Fatal: Cannot determine if the url is archived: %s %s %s" % (
             url, start, end))
    return "", ERR_UNKNOWN_FAILURE


# we divide the year into two 6-month intervals/seasons
SEASON_A = "A"
SEASON_B = "B"
VALID_SEASONS = [SEASON_A, SEASON_B]


def get_start_end_for_season(year, season):
    """Return the start and end of a "season"."""
    assert season in VALID_SEASONS
    if season == SEASON_A:
        start = "%s0101" % year
        end = "%s0630" % year
    elif season == SEASON_B:
        start = "%s0701" % year
        end = "%s1231" % year
    return start, end


def log_(log_type, logger, visit_info, msg):
    """Helper function to have more structured and parseable logs."""
    v = visit_info
    if v is None:
        log_str = msg
    else:
        log_str = "%s VisitInfo: %s" % (msg, json.dumps(vars(v)))

    if log_type == "info":
        logger.info(log_str)
    elif log_type == "warning":
        logger.warning(log_str)
    elif log_type == "debug":
        logger.debug(log_str)
    elif log_type == "error":
        logger.error(log_str)
    elif log_type == "exception":
        logger.exception(log_str)


def get_snapshot_url_by_timestamp(url, timestamp):
    return "https://web.archive.org/web/%sid_/%s" % (
        timestamp, urllib.parse.quote(url))


# try to download the policy twice
MAX_DOWNLOAD_ATTEMPTS = 2

# Sleep some time to throttle after getting a 429 (Too many request) or 503
# (No server is available to handle this request) errors
RATE_LIMIT_SLEEP_DURATION = 30


@app.task(soft_time_limit=240, time_limit=300,
          throws=(pyppeteer.errors.TimeoutError,
                  pyppeteer.errors.NetworkError),
          autoretry_for=(Exception, ),
          max_retries=MAX_DOWNLOAD_ATTEMPTS-1, default_retry_delay=10)
def crawl_wayback_snapshot(url, timestamp, url_id=-1, lang_check=False):
    t0 = time()
    attempt_no = crawl_wayback_snapshot.request.retries + 1
    year = timestamp[:4]
    season = SEASON_A if int(timestamp[4:6]) <= 6 else SEASON_B
    homepage_snapshot_url = get_snapshot_url_by_timestamp(url, timestamp)
    visit_info = VisitInfo(url, attempt_no, url_id, year, season,
                           timestamp, homepage_snapshot_url, lang_check)
    log_("info", logger, visit_info, "New crawl task")
    try:
        ################################################
        asyncio.get_event_loop().run_until_complete(
            download_policy(visit_info))
        ################################################
    except (SoftTimeLimitExceeded, TimeLimitExceeded,
            pyppeteer.errors.TimeoutError,
            pyppeteer.errors.PageError,
            PdfDownloadError,
            WaybackRedirectionError) as texc:
        log_("error", logger, visit_info, "Exception: %s" % texc)
        if attempt_no < MAX_DOWNLOAD_ATTEMPTS:
            raise texc
    except (HttpStatusError) as exc:
        log_("error", logger, visit_info, "Error: %s" % exc)
        if "Status code: 429" in repr(exc) or "Status code: 503" in repr(exc):
            log_("info", logger, visit_info,
                 "HTTP Err 429/503: Will decrement the attempt count %s" %
                 repr(exc))
            crawl_wayback_snapshot.request.retries -= 1
            sleep(RATE_LIMIT_SLEEP_DURATION)
            if attempt_no < MAX_DOWNLOAD_ATTEMPTS:
                raise exc
    except OSError as exc:
        log_("error", logger, visit_info, "OSError: %s" % exc)
    else:
        log_("info", logger, visit_info,
             "OK: Successfully crawled in %0.1f" % (time() - t0))


# log file with the language detection results
LANG_DETECTION_CRAWL_LOG = "top100K_1996_2019_lang_detect.log"

# whether to use cached language detection results from the
# LANG_DETECTION_CRAWL_LOG file
SKIP_LANG_CHECK = False

# if defined it will skip the snapshots from this log file
# SKIP_ALREADY_CRAWLED_SNAPSHOTS = "run_100K_TS.log"
SKIP_ALREADY_CRAWLED_SNAPSHOTS = ""


async def crawl_wayback_for_domains(domains_txt, lang_check=False):
    """Queue homepage snapshots for crawling."""
    n_domains = 0
    n_snapshots = 0
    queued_urls = set()
    english_sites = set()
    non_english_sites = set()
    domain_snapshots = read_snapshots(domains_txt)
    # read the past crawl logs
    if isfile(LANG_DETECTION_CRAWL_LOG):
        english_sites, non_english_sites, unknown_sites = \
            read_lang_detect_logs(LANG_DETECTION_CRAWL_LOG)

    if SKIP_ALREADY_CRAWLED_SNAPSHOTS and \
            isfile(SKIP_ALREADY_CRAWLED_SNAPSHOTS):
        logger.info("Will skip already crawled snapshots in %s" %
                    SKIP_ALREADY_CRAWLED_SNAPSHOTS)

        # crawled_snapshots = read_snapshots(SKIP_ALREADY_CRAWLED_SNAPSHOTS)
        crawled_snapshots = get_crawled_snapshots_from_crawl_logs(
            SKIP_ALREADY_CRAWLED_SNAPSHOTS)

    for domain, timestamps in domain_snapshots.items():
        n_domains += 1
        domain = domain.split("/")[0]
        url = "http://%s" % domain
        if url in queued_urls:
            continue

        if lang_check:
            if url in english_sites or url in non_english_sites:
                logger.info(
                    "Language already detected for %s (id: %d)" % (
                        url, n_domains))
                continue
            queued_urls.add(url)
            n_snapshots += 1
            crawl_wayback_snapshot.delay(
                url, random.choice(timestamps), n_domains, lang_check)
        else:
            if SKIP_ALREADY_CRAWLED_SNAPSHOTS:
                if domain in crawled_snapshots:
                    crawled_timestamps = crawled_snapshots[domain]
                else:
                    crawled_timestamps = []

            # only download policies from english sites
            if not SKIP_LANG_CHECK:
                if url not in english_sites:
                    logger.info("Will skip non-english site %s (id: %d)" % (
                        url, n_domains))
                    continue
            queued_urls.add(url)
            for ts in timestamps:
                if SKIP_ALREADY_CRAWLED_SNAPSHOTS and ts in crawled_timestamps:
                    logger.info(
                        "Will skip already crawled snapshot %s %s (id: %d)" % (
                            domain, ts, n_domains))
                    continue
                n_snapshots += 1
                crawl_wayback_snapshot.delay(url, ts, n_domains)

    logger.info(
        "Queued %d snapshots from %d domains" % (
            n_snapshots, len(queued_urls)))


async def test(test_url):
    browser, page = await launch_browser(headless=False)
    await load_page(page, test_url)
    await close_browser(browser, page)


def get_crawled_snapshots_from_crawl_logs(crawl_log):
    crawled_snapshots = defaultdict(set)
    for log_line in open(crawl_log):
        if "New crawl task" in log_line:
            visit_info = get_visit_info_from_log_line(log_line)
            crawled_snapshots[visit_info['homepage_url']].\
                add(visit_info['timestamp'])
    return crawled_snapshots


def create_crawl_dirs():
    mkdir(POLICY_HTML_DIR)
    mkdir(READABLE_POLICY_HTML_DIR)
    mkdir(POLICY_PDF_DIR)


TESTING = False


if __name__ == '__main__':
    patch_pyppeteer()
    create_crawl_dirs()
    if TESTING:
        crawl_wayback_snapshot("http://naturalnews.com", "20171001013444",
                               1, False)
        sys.exit(0)

    assert len(sys.argv) > 3
    domains_txt = sys.argv[1]

    # the following arguments are not used; i.e. we didn't limit the crawl to
    # start_year and end_year.
    start_year = int(sys.argv[2])
    end_year = int(sys.argv[3])

    lang_check = False  # only determine page languages
    if len(sys.argv) > 4 and sys.argv[4] == "lang_check":
        lang_check = True

    if lang_check:
        logger.info("Will determine English pages in %s" % domains_txt)
    else:
        logger.info("Will crawl domains in %s" % domains_txt)
    if not isfile(sys.argv[1]):
        logger.error("The first argument should be a file")
        sys.exit(1)

    asyncio.get_event_loop().run_until_complete(
        crawl_wayback_for_domains(domains_txt, lang_check))
