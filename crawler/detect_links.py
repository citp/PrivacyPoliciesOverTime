from util import get_tld_or_host


try: # Python 2
    from urlparse import urlparse, urljoin
    iteritems = "iteritems"
except ImportError: # Python 3
    iteritems = "items"
    from urllib.parse import urljoin, urlparse


PARTIAL_POLICY_TITLES = [
    'policy', 'policies', 'cookie', 'security',
    'statement', 'terms', 'notice']

EXACT_POLICY_TITLES = [
    "privacy policy", "privacy statement", "privacy", "privacy notice",
    "cookie policy", "your privacy", "your privacy rights"]


def get_abs_link(href, top_url, sanitize=True):
    if href == "#":
        return href
    if sanitize:
        href = sanitize_link(href)
    try:
        return urljoin(top_url, href)
    except ValueError:
        pass


def remove_white_space(text):
    """Remove new lines and tabs."""
    return text.strip().replace("\r", "").replace("\n", "").replace("\t", "")


def sanitize_link(href):
    # some sites link to both site.com/privacy and site.com/privacy/
    href = href.rstrip('/').strip()
    return remove_white_space(href)


def is_internal_http_link(href, page_domain):
    if href.startswith("tel:") or href == "#":
        return False
    try:
        parsed = urlparse(href)
    except ValueError:
        return False
    if not parsed.scheme:  # relative link
        return True
    if parsed.scheme in ["http", "https"]:  # absolute http(s) link
        return get_tld_or_host(href) == page_domain


def filter_common_crawl_links(links):
    return [
        link for link in links
        if "text" in link and "path" in link and
           link["path"] == "A@/href" and
           link["url"] and
           not link["text"].startswith("http")
    ]


def get_partial_matching_link(links_dict):
    privacy_links = {text: link for text, link in  getattr(links_dict,iteritems)()
        if "privacy" in text}

    if not privacy_links:
        return None

    for keyword in PARTIAL_POLICY_TITLES:
        tmp_links = [
            link for text, link in  getattr(privacy_links,iteritems)() if keyword in text]
        if tmp_links:
            return tmp_links[0]
    return None


def get_matching_link(links_dict):
    for link_text in EXACT_POLICY_TITLES:
        if link_text in links_dict:
            return links_dict[link_text]
    return None


def get_non_policy_link(links_dict, page_url):
    page_domain = get_tld_or_host(page_url)
    for text, link in getattr(links_dict,iteritems)():
        if "privacy" in text or not is_internal_http_link(
                link["url"], page_domain):
            continue
        return get_abs_link(link["url"], page_url)


def filter_hash_links(links, page_url):
    filtered_links = []
    for link in links:
        if link["url"] and link["url"].rstrip("#").rstrip("/") != page_url.rstrip("/"):
            filtered_links.append(link)
    return filtered_links


def find_privacy_policy_link(links, page_url, cc_links=False):
    if cc_links:
        links = filter_common_crawl_links(links)

    links = filter_hash_links(links, page_url)
    if not links:
        return None
    links_dict = {link["text"].strip().lower(): link for link in links}
    policy_link = get_matching_link(links_dict)
    if policy_link is None:
        policy_link = get_partial_matching_link(links_dict)
        if policy_link is None:
            return None
    return (get_abs_link(policy_link["url"], page_url),
            remove_white_space(policy_link["text"])
            )

    # print potential misses
    # from polyglot.detect import Detector
    # for link in links:
    #     if "privacy" in link["url"].rstrip("/").split("/")[-1]:
    #         detector = Detector(link["text"].lower(), quiet=True)
    #         if detector.languages[0].code != "en":
    #             continue
    #         print("Potential miss: \t%s\t%s\t%s" % (
    #             link["text"], link["url"], page_url))

