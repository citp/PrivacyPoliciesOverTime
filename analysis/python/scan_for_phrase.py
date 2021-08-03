#!/bin/python3

import sys
import csv
import historical.util as util
del util.WebClient

def scan(phrase, norm_params, n_override=None):
    phrases = None
    if type(phrase) is list or type(phrase) is tuple:
        phrases = phrase
    else:
        phrases = [phrase]
    if n_override is not None:
        n = n_override
    else:
        n = len(phrases[0].split(' '))
    counts = {phrase:{ys:0 for ys in util.iter_yearseason()} for phrase in phrases}
    for ys in util.iter_yearseason():
        unmet_phrases = set(phrases)
        count_adjust = norm_params[ys]
        for row in util.load_grams(n, ys):
            rphrase=row[1]
            for phrase in unmet_phrases:
                if rphrase == phrase:
                    unmet_phrases.remove(phrase)
                    count=row[0]
                    counts[phrase][ys]=int(count)/count_adjust
                    break #Only one phrase can match
            if len(unmet_phrases) == 0:
                break #No more phrases to be found this interval
    return counts

if __name__ == "__main__":
    #TODO argument parsing
    phrase = sys.argv[1]
    tags = sys.argv[2]
    norm_params = dict(zip([ys for ys in util.iter_yearseason()],[float(i) for i in sys.argv[3].split(' ')]))
    if len(sys.argv) == 5:
        n_override = sys.argv[4]
    else:
        n_override = None
    res = scan(phrase,norm_params,n_override=n_override)
    for phrase in res:
        print("%s,%s" % (phrase, ",".join([str(res[phrase][ys]) for ys in util.iter_yearseason()])))
            
