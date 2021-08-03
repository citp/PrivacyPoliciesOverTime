import tld
import json
top_trackers = {
    "Yahoo/AOL": (("Yahoo","AOL","America Online"),"Verizon Media.json"),
    "Facebook": (("Facebook",), "Facebook, Inc..json"),
    "Twitter": (("Twitter",), "Twitter, Inc..json"),
    "Google": (("Google","Doubleclick"), "Google LLC.json"),
    "Amazon": (("Amazon",), "Amazon Technologies, Inc..json"),
    "Automattic": (("Automattic",), "Automattic, Inc..json"),
    "Cloudflare": (("Cloudflare",), "Cloudflare, Inc..json"),
    "AppNexus": (("AppNexus",), "AppNexus, Inc..json")
}
labels = []
terms = []
for label, (names, fn) in top_trackers.items():
    with open("../tracker-radar/entities/%s" % fn, "r") as f:
        doms = json.load(f)["properties"]
    doms = map(lambda s: r"\b%s\b" % s.replace(r".", r"\."), doms)
    dom_query = r"(?:%s)" % "|".join(doms)
    names = map(lambda s: r"\b%s\b" % s, names)
    name_query = r"(?:%s)" % "|".join(names)
    terms.append((name_query,dom_query))
    labels.append(label)
    