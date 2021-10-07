import sys
import logging
from celery import Celery
from os.path import isfile
from time import time
from datetime import datetime
from crawl_util import (is_valid_wb_timestamp, load_cdx_page,
                        ERR_BLOCKED_SITE, ERR_EMPTY_RESPONSE)


# retry tasks failed with exception
MAX_ATTEMPTS = 5

# throttle to limit wayback machine's obscure rate limits
MAX_NUM_OF_TASKS_PER_MIN = 150


logger = logging.getLogger('wayback_ts')
hdlr = logging.FileHandler('puppeteer_ts.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.DEBUG)

app = Celery('celery_get_wayback_timestamps',
             broker='pyamqp://guest@localhost//')

app.control.rate_limit(
    'celery_get_wayback_timestamps.get_wayback_timestamps_for_domain',
    '%d/m' % MAX_NUM_OF_TASKS_PER_MIN)


def get_timestamps(domain, start_year=0, end_year=0):
    ok_timestamps = []
    non_ok_timestamps = []
    CDX_BASE_PARAMS = [
        ("url", domain),
        ("filter", "statuscode:200|301|302|warc/revisit"),
        ("filter", "!length:-"),
    ]
    cdx_params = CDX_BASE_PARAMS
    if start_year:
        cdx_params.append(("from", "%s0101" % start_year))
    if end_year:
        cdx_params.append(("to", "%s1231" % end_year))
    body, status, err_code = load_cdx_page(cdx_params=cdx_params, logger=logger)
    if err_code:
        if err_code == ERR_EMPTY_RESPONSE:
            logger.warning("ERR-657: Not archived %s" % domain)
        elif err_code == ERR_BLOCKED_SITE:
            logger.warning("ERR-658: Domain blocked %s" % domain)
        else:
            logger.error("ERR-000: Unknown error %s %s" % (err_code, domain))
        return None

    for l in body.split("\n"):
        if not l:
            continue
        items = l.rstrip().split(" ")
        timestamp = items[1]
        if not is_valid_wb_timestamp(timestamp):
            logger.debug("ERR-656: Invalid timestamps %s %s" % (
                domain, timestamp))
            continue
        status_code = items[-3]
        if status_code == "200":
            ok_timestamps.append(timestamp)
        else:
            non_ok_timestamps.append(timestamp)
    return select_timestamps(domain, ok_timestamps, non_ok_timestamps,
                             start_year, end_year)


def select_timestamp_for_interval(domain, year, season, ok_timestamps_datetime,
                                  non_ok_timestamps_datetime):
    if season == "A":
        middle_point = date_from_ts_str("%s0401000000" % year)
        min_point = date_from_ts_str("%s0101000000" % year)
        max_point = date_from_ts_str("%s0630235959" % year)
    else:
        middle_point = date_from_ts_str("%s1001000000" % year)
        min_point = date_from_ts_str("%s0701000000" % year)
        max_point = date_from_ts_str("%s1231235959" % year)

    if len(ok_timestamps_datetime):
        ts = min(ok_timestamps_datetime, key=lambda x:
                 abs((x-middle_point).total_seconds()))
        if ts > min_point and ts < max_point:
            return ts.strftime('%Y%m%d%H%M%S')

    # at this point we couldn't find a snapshot with status=200
    # return if we don't have any non-200 snapshots
    if not len(non_ok_timestamps_datetime):
        # logger.debug("ERR-457: No snapshots in this interval %s %s %s" % (
        #    domain, year, season))
        return None

    ts = min(non_ok_timestamps_datetime, key=lambda x: abs(
        (x-middle_point).total_seconds()))
    if ts > min_point and ts < max_point:
        logger.debug(
            "ERR-456: No snapshots with status=200, fell back to non-200 "
            "results %s %s %s" % (domain, year, season))
        # print(ts, domain, year, season, middle_point)
        return ts.strftime('%Y%m%d%H%M%S')
    else:
        # logger.debug("ERR-457: No snapshots in this interval %s %s %s" % (
        #    domain, year, season))
        return None


def date_from_ts_str(ts_str):
    return datetime.strptime(ts_str, '%Y%m%d%H%M%S')


def select_timestamps(domain, ok_timestamps, non_ok_timestamps,
                      start_year, end_year):
    selected = []
    if not len(ok_timestamps) and not len(non_ok_timestamps):
        return selected

    ok_timestamps_datetime = [date_from_ts_str(ts) for ts in ok_timestamps]
    non_ok_timestamps_datetime = [
        date_from_ts_str(ts) for ts in non_ok_timestamps]
    for year in range(start_year, end_year+1):
        for season in ["A", "B"]:
            ts = select_timestamp_for_interval(
                domain, year, season, ok_timestamps_datetime,
                non_ok_timestamps_datetime)
            if ts is not None:
                selected.append(ts)

    return selected


@app.task(soft_time_limit=180, time_limit=240, autoretry_for=(Exception, ),
          max_retries=MAX_ATTEMPTS-1, retry_backoff=True)
def get_wayback_timestamps_for_domain(domain, start_year, end_year):
    t0 = time()
    attempt_no = get_wayback_timestamps_for_domain.request.retries + 1
    logger.info(
        "New task: Will get snapshot timestamps for %s. Attempt: %s" % (
            domain, attempt_no))
    snapshot_timestamps = get_timestamps(domain, start_year, end_year)
    duration = time() - t0
    if snapshot_timestamps is None:
        logger.warning("ERR-846: Finished in %0.1f No timestamps found: %s"
                       % (duration, domain))
    else:
        logger.info("Finished in %0.1f TIMESTAMPS: %s %s" % (
            duration, domain, ",".join(snapshot_timestamps)))


TESTING = False


if __name__ == '__main__':
    if TESTING:
        get_wayback_timestamps_for_domain("000a.biz", 1999, 2019)
        sys.exit()
    assert len(sys.argv) > 3
    domains_file = sys.argv[1]
    start_year = int(sys.argv[2])
    end_year = int(sys.argv[3])
    if not isfile(domains_file):
        print("%s is not a file" % domains_file)
        sys.exit()
    print("Will crawl domains in %s for the range:[%s, %s]" %
          (domains_file, start_year, end_year))
    for domain in open(domains_file):
        get_wayback_timestamps_for_domain.delay(domain.rstrip(),
                                                start_year, end_year)
